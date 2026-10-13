"""
tests/test_dashboard_token_expiry_retry.py - [AT] fix/dashboard-token-expiry 회귀 테스트

배경: 10/13 09:05 이후 /api/positions/stock이 "기간이 만료된 token" 에러로 stale
스냅샷만 반환했다. dashboard/main.py의 get_kis_token()/_get_stock_positions_raw()에
두 가지 문제가 있었다:

1) get_kis_token()이 Redis에서 읽은 토큰의 메모리 만료를 실제 발급 시각과 무관하게
   now+82800(23시간)으로 고정했다 — 이미 곧 만료될 토큰을 23시간 더 유효한 것처럼
   캐싱해 만료 에러가 재발급 없이 계속 재사용됐다.
2) _get_stock_positions_raw()는 응답이 만료 토큰 에러여도 강제 재발급(force_new)
   재시도가 없어 다음 10초 캐시 주기까지 stale 스냅샷만 반환했다.

이 테스트는
- get_kis_token()이 Redis 토큰의 메모리 만료를 Redis TTL 기준으로 계산하고,
  TTL 조회 실패 시 보수적으로 짧게(10분)만 신뢰하는지,
- _get_stock_positions_raw()가 만료 토큰 응답(EGW00xxx, EGW00201 레이트리밋 제외)에서
  강제 재발급 후 1회만 재시도하고, 재시도도 실패하면 stale 폴백을 유지하며,
  EGW00201(레이트리밋)에는 재발급을 시도하지 않는지를 검증한다.
"""
import asyncio
import json
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp/fastapi가 테스트 환경에 없어도
    임포트할 수 있도록 더미로 대체한다."""
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

        class ServerTimeoutError(Exception):
            pass

        stub.ServerTimeoutError = ServerTimeoutError
        sys.modules["aiohttp"] = stub

    for mod_name in [
        "fastapi",
        "fastapi.staticfiles",
        "fastapi.responses",
        "fastapi.middleware.cors",
        "google",
        "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()

import dashboard.main as dm  # noqa: E402


class FakeResponse:
    def __init__(self, data: dict, status: int = 200):
        self._data = data
        self.status = status

    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class FakeSequenceSession:
    """get() 호출마다 responses 리스트의 다음 응답을 순서대로 반환한다
    (마지막 응답은 그 이후 호출에도 반복 반환)."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.get_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        idx = min(self.get_calls, len(self._responses) - 1)
        resp = self._responses[idx]
        self.get_calls += 1
        return FakeResponse(resp)


class _FakeRedisSnapshot:
    """positions:last_ok 스냅샷 폴백 조회용 대역."""

    def __init__(self, snapshot: dict):
        self._snapshot = json.dumps(snapshot)

    async def get(self, key):
        if key == "positions:last_ok":
            return self._snapshot
        return None

    async def setex(self, *a, **kw):
        pass


class _FakeRedisTTL:
    """get_kis_token()의 Redis 캐시 경로(get+ttl)용 대역."""

    def __init__(self, token: str, ttl_value):
        self._token = token
        self._ttl_value = ttl_value
        self.ttl_calls = 0

    async def get(self, key):
        return self._token

    async def ttl(self, key):
        self.ttl_calls += 1
        return self._ttl_value

    async def delete(self, key):
        pass


class TestStockPositionsTokenExpiryRetry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0
        dm._stock_positions_lock = None
        dm.redis_client = None

    async def test_expired_token_response_force_reissues_and_retries_once(self):
        """첫 응답이 만료 토큰 에러(EGW00123)면 force_new 재발급 후 1회 재시도해
        성공 응답을 받아야 한다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])

        async def fake_get_kis_token(force_new=False):
            return "TOK-NEW" if force_new else "TOK-OLD"

        mock_token = AsyncMock(side_effect=fake_get_kis_token)

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        self.assertEqual(session.get_calls, 2)
        self.assertEqual(mock_token.await_count, 2)
        # 두 번째 호출은 강제 재발급이어야 한다
        self.assertEqual(mock_token.await_args_list[1].kwargs.get("force_new"), True)

    async def test_retry_also_fails_falls_back_to_stale_snapshot(self):
        """재발급 후 재시도도 만료 에러면 재시도를 더 하지 않고 last_ok 스냅샷으로
        stale 폴백해야 한다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
        ])
        dm.redis_client = _FakeRedisSnapshot({
            "success": True,
            "data": [],
            "account": {"cash": 500000, "total_eval": 500000},
        })

        async def fake_get_kis_token(force_new=False):
            return "TOK-NEW" if force_new else "TOK-OLD"

        mock_token = AsyncMock(side_effect=fake_get_kis_token)

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session):
            result = await dm._get_stock_positions_raw()

        self.assertEqual(session.get_calls, 2)
        self.assertEqual(mock_token.await_count, 2)
        self.assertTrue(result.get("stale"))
        self.assertEqual(result["account"]["cash"], 500000)

    async def test_rate_limit_egw00201_does_not_trigger_reissue_but_retries_once(self):
        """EGW00201(초당 거래건수 초과)은 만료가 아니므로 재발급을 시도하지 않지만,
        1.5초 후 1회 재시도해 성공 응답을 받아야 한다([AT] fix/token-reissue-condition)."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])

        mock_token = AsyncMock(return_value="TOK-OLD")

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session), \
             patch("asyncio.sleep", AsyncMock()) as mock_sleep:
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        self.assertEqual(session.get_calls, 2)
        self.assertEqual(mock_token.await_count, 1)
        self.assertEqual(mock_token.await_args.kwargs.get("force_new", False), False)
        mock_sleep.assert_awaited_once_with(1.5)

    async def test_rate_limit_egw00201_retry_also_fails_falls_back_to_stale(self):
        """재시도도 EGW00201이면 더 재시도하지 않고 재발급도 시도하지 않은 채
        last_ok 스냅샷으로 stale 폴백해야 한다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"},
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"},
        ])
        dm.redis_client = _FakeRedisSnapshot({
            "success": True,
            "data": [],
            "account": {"cash": 300000, "total_eval": 300000},
        })

        mock_token = AsyncMock(return_value="TOK-OLD")

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session), \
             patch("asyncio.sleep", AsyncMock()) as mock_sleep:
            result = await dm._get_stock_positions_raw()

        self.assertEqual(session.get_calls, 2)
        self.assertEqual(mock_token.await_count, 1)
        self.assertEqual(mock_token.await_args.kwargs.get("force_new", False), False)
        mock_sleep.assert_awaited_once_with(1.5)
        self.assertTrue(result.get("stale"))

    async def test_egw00215_without_expiry_wording_does_not_trigger_reissue(self):
        """EGW00215는 재발급 조건(EGW00121/EGW00123, '만료'/'token' 포함)에 해당하지
        않으므로 재발급을 시도하지 않고 1번만 조회 후 기존 stale 폴백으로 가야 한다
        ([AT] fix/token-reissue-condition — 접두사 매칭 제거 회귀 방지)."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00215", "msg1": "알 수 없는 오류입니다"},
        ])
        dm.redis_client = _FakeRedisSnapshot({
            "success": True,
            "data": [],
            "account": {"cash": 700000, "total_eval": 700000},
        })

        mock_token = AsyncMock(return_value="TOK-OLD")

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session), \
             patch("asyncio.sleep", AsyncMock()) as mock_sleep:
            result = await dm._get_stock_positions_raw()

        self.assertEqual(session.get_calls, 1)
        self.assertEqual(mock_token.await_count, 1)
        self.assertEqual(mock_token.await_args.kwargs.get("force_new", False), False)
        mock_sleep.assert_not_awaited()
        self.assertTrue(result.get("stale"))
        self.assertEqual(result["account"]["cash"], 700000)

    async def test_egw00123_still_triggers_reissue(self):
        """EGW00123은 좁혀진 조건에도 여전히 재발급을 트리거해야 한다(회귀 방지)."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])

        async def fake_get_kis_token(force_new=False):
            return "TOK-NEW" if force_new else "TOK-OLD"

        mock_token = AsyncMock(side_effect=fake_get_kis_token)

        with patch("dashboard.main.get_kis_token", mock_token), \
             patch("aiohttp.ClientSession", return_value=session):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        self.assertEqual(session.get_calls, 2)
        self.assertEqual(mock_token.await_count, 2)
        self.assertEqual(mock_token.await_args_list[1].kwargs.get("force_new"), True)


class TestGetKisTokenRedisTtlExpiry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        dm._kis_token_cache["token"] = ""
        dm._kis_token_cache["expires"] = 0
        dm._kis_token_lock = None
        dm.redis_client = None

    async def test_redis_ttl_drives_memory_expiry(self):
        """Redis에서 읽은 토큰의 메모리 만료는 고정 23시간이 아니라 실제 Redis TTL을
        반영해야 한다."""
        fake_redis = _FakeRedisTTL("TOK-REDIS", 3600)
        dm.redis_client = fake_redis

        before = time.time()
        token = await dm.get_kis_token()
        after = time.time()

        self.assertEqual(token, "TOK-REDIS")
        self.assertGreaterEqual(fake_redis.ttl_calls, 1)
        self.assertTrue(before + 3600 - 5 <= dm._kis_token_cache["expires"] <= after + 3600 + 5)

    async def test_ttl_lookup_failure_uses_conservative_short_expiry(self):
        """TTL이 -1/-2(만료정보 없음) 등으로 돌아오면 23시간이 아니라 보수적으로
        짧게(10분)만 캐시해야 한다."""
        fake_redis = _FakeRedisTTL("TOK-REDIS2", -1)
        dm.redis_client = fake_redis

        before = time.time()
        token = await dm.get_kis_token()
        after = time.time()

        self.assertEqual(token, "TOK-REDIS2")
        self.assertTrue(before + 600 - 5 <= dm._kis_token_cache["expires"] <= after + 600 + 5)
        self.assertLess(dm._kis_token_cache["expires"], before + 82800)

    async def test_ttl_raising_exception_uses_conservative_short_expiry(self):
        """ttl() 조회 자체가 예외를 던져도(Redis 장애 등) 23시간으로 폴백하지 않고
        짧게(10분)만 신뢰해야 한다."""

        class _RaisingTTLRedis(_FakeRedisTTL):
            async def ttl(self, key):
                raise RuntimeError("redis down")

        fake_redis = _RaisingTTLRedis("TOK-REDIS3", 0)
        dm.redis_client = fake_redis

        before = time.time()
        token = await dm.get_kis_token()
        after = time.time()

        self.assertEqual(token, "TOK-REDIS3")
        self.assertTrue(before + 600 - 5 <= dm._kis_token_cache["expires"] <= after + 600 + 5)


if __name__ == "__main__":
    unittest.main()
