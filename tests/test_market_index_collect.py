"""
tests/test_market_index_collect.py - [AT] fix/market-index-collect,
fix/market-index-shared-token (PM 승인)

배경: 10/14 17:55 수동 실행에서 KOSPI는 성공하고 KOSDAQ는 "KIS 토큰 발급 실패"로
실패했다. 원인은 _collect_market_index_daily가 지수마다 KISTrader()를 새로 만들어
trader._get_token()을 호출했는데, 그 메서드가 쓰는 common.database.cache.client가
dashboard 프로세스에서는 connect()된 적이 없어(None) 매번 신규 토큰 발급으로
빠지고, 모의투자 KIS의 토큰 발급 1분 1회 제한에 걸려 두 번째 지수가 실패한 것이었다.

이 테스트가 검증하는 것:
1. KISTrader 인스턴스를 1개만 만들어 두 지수에 재사용하고, 토큰은
   dashboard의 get_kis_token()으로 정확히 1번만 받아 trader.access_token에
   주입한다(trader._get_token()으로 신규 발급하지 않음).
2. get_kis_token()이 빈 값이면 두 지수 모두 KISTrader를 만들지 않고 "토큰 없음"
   (또는 더 구체적인 사유)으로 기록하고 건너뛴다.
3. _collect_market_index_daily()가 지수별 성공/실패 결과({"ok": bool, ...})를
   반환하고, 같은 내용을 Redis health:index_collect:YYYY-MM-DD(7일 TTL)에
   JSON으로 저장한다.
4. 토큰 조회 성공 이후 get_index_price 실패(KIS rt_cd·msg_cd·msg1) / DB 저장 중
   예외가 나도, 한 지수의 실패가 나머지 지수 처리를 막지 않는다(기존 fail-open
   동작 불변).
5. market_index_daily UPSERT는 성공한 지수에 대해서만 실행된다.
"""
import json
import sys
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, raise_on_execute=False):
        self.executed = []
        self.raise_on_execute = raise_on_execute

    async def execute(self, query, *args):
        if self.raise_on_execute:
            raise RuntimeError("DB 저장 실패")
        self.executed.append((query, args))
        return "OK"


class FakePool:
    def __init__(self, conn=None):
        self.conn = conn or FakeConn()

    def acquire(self):
        return _AcquireCtx(self.conn)


class FakeRedis:
    """get/setex만 지원 — health:index_collect:* 저장 검증용."""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value


def _frozen_datetime_at(year, month, day, hour, minute, second=0):
    _real_datetime = datetime

    class _Frozen(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, month, day, hour, minute, second, tzinfo=dm.KST)
    return _Frozen


class FakeKISTrader:
    """_collect_market_index_daily가 이제 KISTrader 인스턴스를 1개만 만들어 두
    지수에 재사용하므로([AT] fix/market-index-shared-token), 더 이상 FIFO configs가
    아니라 index_code 기준 behaviors 딕셔너리로 지수별 동작을 지정한다. 토큰은
    함수가 trader.access_token에 직접 주입하므로 _get_token()은 구현하지 않는다
    (호출되면 AttributeError로 테스트가 실패해, 되돌린 코드를 잡아낸다)."""
    behaviors: dict = {}
    instances: list = []

    def __init__(self):
        self.access_token = ""
        self.last_error = None
        FakeKISTrader.instances.append(self)

    async def get_index_price(self, index_code):
        cfg = FakeKISTrader.behaviors.get(
            index_code, {"ok": True, "close": 2500.0, "change_rate": 1.0})
        if cfg.get("ok"):
            return {"close": cfg["close"], "change_rate": cfg["change_rate"]}
        self.last_error = cfg.get("error", {"reason": "조회 실패"})
        return None


class TestCollectMarketIndexDaily(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        FakeKISTrader.behaviors = {}
        FakeKISTrader.instances = []
        start(patch("stock_trader.kis_trader.KISTrader", FakeKISTrader))
        start(patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 14, 15, 41, 0)))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def test_both_succeed_with_single_token_fetch_and_single_instance(self):
        FakeKISTrader.behaviors = {
            "0001": {"ok": True, "close": 2500.5, "change_rate": -1.23},
            "1001": {"ok": True, "close": 800.1, "change_rate": 0.45},
        }
        pool = FakePool()
        redis = FakeRedis()
        token_mock = AsyncMock(return_value="fake-token")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis), \
                patch.object(dm, "get_kis_token", token_mock):
            results = await dm._collect_market_index_daily()

        self.assertTrue(results["KOSPI"]["ok"])
        self.assertTrue(results["KOSDAQ"]["ok"])
        self.assertEqual(len(pool.conn.executed), 2)
        token_mock.assert_awaited_once()
        self.assertEqual(len(FakeKISTrader.instances), 1)
        self.assertEqual(FakeKISTrader.instances[0].access_token, "fake-token")

        key = f"health:index_collect:{date(2026, 10, 14).isoformat()}"
        self.assertIn(key, redis.store)
        stored = json.loads(redis.store[key])
        self.assertEqual(stored, results)

    async def test_no_token_skips_both_indices_without_creating_trader(self):
        pool = FakePool()
        redis = FakeRedis()
        token_mock = AsyncMock(return_value="")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis), \
                patch.object(dm, "get_kis_token", token_mock), \
                patch.object(dm, "_kis_token_last_error", "유효하지 않은 appkey 입니다."):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertEqual(results["KOSPI"]["error"]["reason"], "유효하지 않은 appkey 입니다.")
        self.assertFalse(results["KOSDAQ"]["ok"])
        self.assertEqual(results["KOSDAQ"]["error"]["reason"], "유효하지 않은 appkey 입니다.")
        self.assertEqual(len(pool.conn.executed), 0)
        self.assertEqual(len(FakeKISTrader.instances), 0)

    async def test_no_token_without_detail_falls_back_to_generic_reason(self):
        pool = FakePool()
        redis = FakeRedis()
        token_mock = AsyncMock(return_value="")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis), \
                patch.object(dm, "get_kis_token", token_mock), \
                patch.object(dm, "_kis_token_last_error", ""):
            results = await dm._collect_market_index_daily()

        self.assertEqual(results["KOSPI"]["error"]["reason"], "토큰 없음")
        self.assertEqual(results["KOSDAQ"]["error"]["reason"], "토큰 없음")

    async def test_kis_api_failure_reason_is_recorded(self):
        FakeKISTrader.behaviors = {
            "0001": {"ok": False, "error": {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "조회 실패"}},
            "1001": {"ok": True, "close": 800.1, "change_rate": 0.45},
        }
        pool = FakePool()
        redis = FakeRedis()
        token_mock = AsyncMock(return_value="fake-token")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis), \
                patch.object(dm, "get_kis_token", token_mock):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertEqual(results["KOSPI"]["error"],
                          {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "조회 실패"})
        self.assertTrue(results["KOSDAQ"]["ok"])
        # KOSDAQ만 DB에 저장됨
        self.assertEqual(len(pool.conn.executed), 1)
        self.assertEqual(pool.conn.executed[0][1][1], "1001")

    async def test_db_exception_on_one_index_does_not_block_the_other(self):
        """DB 저장 중 예외가 나도(기존 fail-open 동작 불변) 나머지 지수는 계속 처리된다."""
        FakeKISTrader.behaviors = {
            "0001": {"ok": True, "close": 2500.5, "change_rate": -1.23},
            "1001": {"ok": True, "close": 800.1, "change_rate": 0.45},
        }
        pool = FakePool(conn=FakeConn(raise_on_execute=True))
        redis = FakeRedis()
        token_mock = AsyncMock(return_value="fake-token")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis), \
                patch.object(dm, "get_kis_token", token_mock):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertIn("RuntimeError", results["KOSPI"]["error"]["reason"])
        self.assertFalse(results["KOSDAQ"]["ok"])

    async def test_redis_setex_failure_does_not_raise(self):
        """Redis 저장 자체가 실패해도(네트워크 등) _collect_market_index_daily는
        예외를 던지지 않고 결과를 반환한다(관찰 전용, 매매 영향 없음)."""
        FakeKISTrader.behaviors = {
            "0001": {"ok": True, "close": 2500.5, "change_rate": -1.23},
            "1001": {"ok": True, "close": 800.1, "change_rate": 0.45},
        }
        pool = FakePool()

        class BrokenRedis:
            async def setex(self, *a, **kw):
                raise RuntimeError("redis down")

        token_mock = AsyncMock(return_value="fake-token")
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", BrokenRedis()), \
                patch.object(dm, "get_kis_token", token_mock):
            results = await dm._collect_market_index_daily()

        self.assertTrue(results["KOSPI"]["ok"])
        self.assertTrue(results["KOSDAQ"]["ok"])


if __name__ == "__main__":
    unittest.main()
