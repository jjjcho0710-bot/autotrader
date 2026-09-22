"""
stock_trader/kis_trader.py KIS 토큰 Redis 캐시 키 단위 테스트.

배경: 실전 계좌인데 매도 주문이 "모의투자 주문이 불가한 계좌입니다"로 거부되는 사고가
있었다. 원인은 tr_id 계산(VTTC.../TTTC...)이 아니라 — 그건 이미 config.KIS_IS_PAPER를
정확히 따르고 있었다 — KIS 액세스 토큰을 캐싱하는 Redis 키가 모의/실전 구분 없이
"kis:access_token" 하나로 고정되어 있던 것이었다. dashboard/main.py의 get_kis_token()은
"kis:paper_token"/"kis:access_token"을 모드별로 분리해서 쓰는데, stock_trader/kis_trader.py
(그리고 data_collector의 kis_collector.py, daily_collector.py)는 분리 없이 같은 키를
공유해서, 모의투자 모드로 발급된 토큰이 이 키에 캐시되면 실전 매매 프로세스가 그 토큰을
그대로 읽어 실전 tr_id(TTTC...)와 모의투자 토큰을 함께 보내는 불일치가 발생했다.
이 테스트는 _get_token()이 config.KIS_IS_PAPER 값에 따라 서로 다른 Redis 키를
읽고/쓰는지, 그리고 한쪽 모드에 캐시된 토큰이 다른 모드로 새어나가지 않는지 검증한다.
"""
import asyncio
import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp가 테스트 환경에 없어도
    kis_trader.py(순수 로직 포함)를 임포트할 수 있도록 더미로 대체한다.
    실제 패키지가 설치되어 있으면 아무 것도 하지 않는다."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = types.ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub

    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        stub = types.ModuleType("aiohttp")
        stub.ClientSession = object
        stub.TCPConnector = lambda *a, **kw: None
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402
from common.database import cache  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402


class FakeRedisClient:
    """setex/get/delete만 지원하는 최소 in-memory Redis 대역."""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)


class _FakeTokenResponse:
    def __init__(self, token: str):
        self._token = token

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return {"access_token": self._token}


class _FakeTokenSession:
    """kis_trader.py의 _new_session()이 반환할 것으로 기대되는 aiohttp 세션 대역.
    /oauth2/tokenP POST 호출 시 고정된 access_token을 반환한다."""

    def __init__(self, token: str):
        self._token = token

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, json=None, **kwargs):
        return _FakeTokenResponse(self._token)


class TestKISTraderTokenCacheKeyIsModeAware(unittest.TestCase):
    def setUp(self):
        self._orig_is_paper = config.KIS_IS_PAPER
        cache.client = FakeRedisClient()
        self.addCleanup(setattr, config, "KIS_IS_PAPER", self._orig_is_paper)

    def test_real_mode_reads_access_token_key_not_paper_key(self):
        """실전 모드(KIS_IS_PAPER=False)에서는 kis:access_token 키만 읽어야 하고,
        kis:paper_token에 캐시된 (다른 프로세스의) 모의투자 토큰을 절대 사용하면 안 된다."""
        config.KIS_IS_PAPER = False
        cache.client.store["kis:paper_token"] = "WRONG-PAPER-TOKEN"
        cache.client.store["kis:access_token"] = "CORRECT-REAL-TOKEN"

        trader = KISTrader()
        asyncio.run(trader._get_token())

        self.assertEqual(trader.access_token, "CORRECT-REAL-TOKEN")

    def test_paper_mode_reads_paper_token_key_not_access_token_key(self):
        """모의투자 모드(KIS_IS_PAPER=True)에서는 kis:paper_token 키만 읽어야 하고,
        kis:access_token에 캐시된 실전 토큰을 잘못 사용하면 안 된다."""
        config.KIS_IS_PAPER = True
        cache.client.store["kis:access_token"] = "WRONG-REAL-TOKEN"
        cache.client.store["kis:paper_token"] = "CORRECT-PAPER-TOKEN"

        trader = KISTrader()
        asyncio.run(trader._get_token())

        self.assertEqual(trader.access_token, "CORRECT-PAPER-TOKEN")

    def test_real_mode_new_token_is_cached_under_access_token_key(self):
        """캐시 미스로 새 토큰을 발급받으면 실전 모드에서는 kis:access_token 키에
        저장되어야 하고, kis:paper_token 키는 건드리면 안 된다(다른 서비스 오염 방지)."""
        config.KIS_IS_PAPER = False
        trader = KISTrader()
        trader._new_session = lambda: _FakeTokenSession("FRESH-REAL-TOKEN")

        asyncio.run(trader._get_token())

        self.assertEqual(trader.access_token, "FRESH-REAL-TOKEN")
        self.assertEqual(cache.client.store.get("kis:access_token"), "FRESH-REAL-TOKEN")
        self.assertNotIn("kis:paper_token", cache.client.store)

    def test_paper_mode_new_token_is_cached_under_paper_token_key(self):
        """캐시 미스로 새 토큰을 발급받으면 모의투자 모드에서는 kis:paper_token 키에
        저장되어야 하고, kis:access_token 키(실전 주문이 읽는 키)는 건드리면 안 된다."""
        config.KIS_IS_PAPER = True
        trader = KISTrader()
        trader._new_session = lambda: _FakeTokenSession("FRESH-PAPER-TOKEN")

        asyncio.run(trader._get_token())

        self.assertEqual(trader.access_token, "FRESH-PAPER-TOKEN")
        self.assertEqual(cache.client.store.get("kis:paper_token"), "FRESH-PAPER-TOKEN")
        self.assertNotIn("kis:access_token", cache.client.store)

    def test_refresh_token_if_expired_deletes_mode_specific_key_only(self):
        """토큰 만료 재발급 시 삭제 대상도 모드별 키여야 한다 — 실전 모드에서 만료 감지
        시 kis:paper_token을 지우면(버그 시나리오) 다른 프로세스의 모의투자 토큰만
        건드리고 정작 자신의 만료된 실전 토큰은 그대로 남아 재사용되는 문제가 생긴다."""
        config.KIS_IS_PAPER = False
        cache.client.store["kis:access_token"] = "STALE-REAL-TOKEN"
        cache.client.store["kis:paper_token"] = "UNRELATED-PAPER-TOKEN"

        trader = KISTrader()
        trader._new_session = lambda: _FakeTokenSession("REISSUED-REAL-TOKEN")

        asyncio.run(trader._refresh_token_if_expired(
            {"rt_cd": "1", "msg1": "토큰이 만료되었습니다", "msg_cd": ""}
        ))

        self.assertEqual(cache.client.store.get("kis:access_token"), "REISSUED-REAL-TOKEN")
        self.assertEqual(cache.client.store.get("kis:paper_token"), "UNRELATED-PAPER-TOKEN")


if __name__ == "__main__":
    unittest.main()
