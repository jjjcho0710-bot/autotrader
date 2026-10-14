"""
tests/test_market_index_collect.py - [AT] fix/market-index-collect (PM 승인)

배경: 10/14 15:45 헬스리포트 j)에서 KOSPI/KOSDAQ 모두 "데이터 없음"이었는데
로그 접근이 안 돼 _collect_market_index_daily/KIS 조회 중 무엇이 실패했는지
확인할 수 없었다. 이 테스트가 검증하는 것:

1. _collect_market_index_daily()가 지수별 성공/실패 결과({"ok": bool, ...})를
   반환하고, 같은 내용을 Redis health:index_collect:YYYY-MM-DD(7일 TTL)에
   JSON으로 저장한다.
2. 토큰 발급 실패 / get_index_price 실패(KIS rt_cd·msg_cd·msg1) / DB 저장 중
   예외 각각의 실패 사유가 결과에 반영된다.
3. 한 지수가 실패해도 나머지 지수 저장은 계속된다(기존 fail-open 동작 불변).
4. market_index_daily UPSERT는 성공한 지수에 대해서만 실행된다.
"""
import json
import sys
import unittest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

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
    """MARKET_INDEX_CODES 순서(KOSPI, KOSDAQ)대로 configs를 하나씩 소비한다.
    _collect_market_index_daily가 지수마다 새 KISTrader() 인스턴스를 만들기
    때문에, 클래스 변수 configs를 FIFO 큐로 써서 지수별 동작을 지정한다."""
    configs = []

    def __init__(self):
        self.access_token = ""
        self.last_error = None
        self._cfg = FakeKISTrader.configs.pop(0) if FakeKISTrader.configs else {
            "ok": True, "close": 2500.0, "change_rate": 1.0}

    async def _get_token(self):
        self.access_token = "" if self._cfg.get("token_fail") else "fake-token"

    async def get_index_price(self, index_code):
        if self._cfg.get("ok"):
            return {"close": self._cfg["close"], "change_rate": self._cfg["change_rate"]}
        self.last_error = self._cfg.get("error", {"reason": "조회 실패"})
        return None


class TestCollectMarketIndexDaily(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        FakeKISTrader.configs = []
        start(patch("stock_trader.kis_trader.KISTrader", FakeKISTrader))
        start(patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 14, 15, 41, 0)))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def test_both_succeed_upserts_both_and_records_ok_to_redis(self):
        FakeKISTrader.configs = [
            {"ok": True, "close": 2500.5, "change_rate": -1.23},
            {"ok": True, "close": 800.1, "change_rate": 0.45},
        ]
        pool = FakePool()
        redis = FakeRedis()
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis):
            results = await dm._collect_market_index_daily()

        self.assertTrue(results["KOSPI"]["ok"])
        self.assertTrue(results["KOSDAQ"]["ok"])
        self.assertEqual(len(pool.conn.executed), 2)

        key = f"health:index_collect:{date(2026, 10, 14).isoformat()}"
        self.assertIn(key, redis.store)
        stored = json.loads(redis.store[key])
        self.assertEqual(stored, results)

    async def test_token_failure_skips_that_index_but_other_continues(self):
        FakeKISTrader.configs = [
            {"token_fail": True},
            {"ok": True, "close": 800.1, "change_rate": 0.45},
        ]
        pool = FakePool()
        redis = FakeRedis()
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertEqual(results["KOSPI"]["error"]["reason"], "KIS 토큰 발급 실패")
        self.assertTrue(results["KOSDAQ"]["ok"])
        # KOSDAQ만 DB에 저장됨
        self.assertEqual(len(pool.conn.executed), 1)
        self.assertEqual(pool.conn.executed[0][1][1], "1001")

    async def test_kis_api_failure_reason_is_recorded(self):
        FakeKISTrader.configs = [
            {"ok": False, "error": {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "조회 실패"}},
            {"ok": True, "close": 800.1, "change_rate": 0.45},
        ]
        pool = FakePool()
        redis = FakeRedis()
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertEqual(results["KOSPI"]["error"],
                          {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "조회 실패"})

    async def test_db_exception_on_one_index_does_not_block_the_other(self):
        """DB 저장 중 예외가 나도(기존 fail-open 동작 불변) 나머지 지수는 계속 처리된다."""
        FakeKISTrader.configs = [
            {"ok": True, "close": 2500.5, "change_rate": -1.23},
            {"ok": True, "close": 800.1, "change_rate": 0.45},
        ]
        pool = FakePool(conn=FakeConn(raise_on_execute=True))
        redis = FakeRedis()
        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", redis):
            results = await dm._collect_market_index_daily()

        self.assertFalse(results["KOSPI"]["ok"])
        self.assertIn("RuntimeError", results["KOSPI"]["error"]["reason"])
        self.assertFalse(results["KOSDAQ"]["ok"])

    async def test_redis_setex_failure_does_not_raise(self):
        """Redis 저장 자체가 실패해도(네트워크 등) _collect_market_index_daily는
        예외를 던지지 않고 결과를 반환한다(관찰 전용, 매매 영향 없음)."""
        FakeKISTrader.configs = [
            {"ok": True, "close": 2500.5, "change_rate": -1.23},
            {"ok": True, "close": 800.1, "change_rate": 0.45},
        ]
        pool = FakePool()

        class BrokenRedis:
            async def setex(self, *a, **kw):
                raise RuntimeError("redis down")

        with patch.object(dm, "db_pool", pool), patch.object(dm, "redis_client", BrokenRedis()):
            results = await dm._collect_market_index_daily()

        self.assertTrue(results["KOSPI"]["ok"])
        self.assertTrue(results["KOSDAQ"]["ok"])


if __name__ == "__main__":
    unittest.main()
