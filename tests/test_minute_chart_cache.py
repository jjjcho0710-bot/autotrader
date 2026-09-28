"""
분봉 차트(/api/chart/{symbol}?period=1m|5m|30m) 속도 개선 테스트.

- 1분봉 원본을 종목별로 Redis 에 캐시하고 1m/5m/30m 은 그 원본에서 합성 → 단위 전환은 KIS 호출 0회
- 원본 캐시 TTL: 평일 장중 10초, 평일 15:40 이후·주말 1800초
- 분봉 KIS 호출도 일봉과 같은 동시 호출 제한·'초당' 재시도(0.7초, 최대 2회)
- 실패/빈 결과는 success:false + 사유(계좌·키 마스킹)로 반환하고 캐시하지 않음
- 분봉 조회마다 info 로그(종목·단위·캐시 히트 여부·ms), 계좌·키 미노출
"""
import asyncio
import json
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

RATE_LIMIT_MSG = "초당 거래건수를 초과하였습니다."
MINUTE_PATH = "inquire-time-itemchartprice"


def _minute_rows(n: int = 12, start_min: int = 0) -> list:
    """KIS output2 는 최신→과거 순. 09:00 부터 n분 (가격은 분마다 +1)."""
    rows = []
    for i in range(n):
        m = start_min + i
        rows.append({"stck_bsop_date": "20260928", "stck_cntg_hour": f"09{m:02d}00",
                     "stck_oprc": str(100 + i), "stck_hgpr": str(110 + i), "stck_lwpr": str(90 + i),
                     "stck_prpr": str(105 + i), "cntg_vol": "10"})
    return list(reversed(rows))


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    async def json(self, *a, **k):
        return self._payload


class FakeSession:
    def __init__(self, handler):
        self._handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        return FakeResponse(await self._handler(url, kwargs))


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.setex_calls = []  # (key, ttl)

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl))
        self.store[key] = value


LIVE = datetime(2026, 9, 28, 10, 0, tzinfo=None)      # 월요일 장중
AFTER = datetime(2026, 9, 28, 15, 40, tzinfo=None)    # 월요일 15:40
SATURDAY = datetime(2026, 9, 26, 11, 0, tzinfo=None)


class TestMinuteChartCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.calls = []
        self.redis = FakeRedis()
        self._patches = []
        dm._minute_locks.clear()

        def start(p):
            p.start()
            self._patches.append(p)

        start(patch.object(dm, "_KIS_QUOTE_SEM", asyncio.Semaphore(dm._KIS_QUOTE_CONCURRENCY)))
        start(patch.object(dm, "get_kis_token", new=AsyncMock(return_value="tok")))
        start(patch.object(dm, "redis_client", self.redis))
        start(patch.object(dm.config, "KIS_IS_PAPER", False))
        start(patch.object(dm.config, "KIS_APP_KEY", "APPKEY-SECRET-VALUE"))
        start(patch.object(dm.config, "KIS_APP_SECRET", "APPSECRET-VALUE"))
        start(patch.object(type(dm.config), "kis_account_no",
                           new_callable=PropertyMock, return_value="12345678-01"))
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "정상처리", "output2": _minute_rows()})

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def set_handler(self, handler):
        async def wrapped(url, kw):
            self.calls.append((url, kw["params"]["FID_INPUT_ISCD"]))
            res = handler(url, kw)
            if asyncio.iscoroutine(res):
                res = await res
            return res

        fake = SimpleNamespace(
            ClientSession=lambda **kw: FakeSession(wrapped),
            TCPConnector=lambda **kw: None,
            ClientTimeout=lambda **kw: None,
        )
        p = patch.object(self.dm, "_aiohttp", fake)
        p.start()
        self._patches.append(p)

    def at(self, when):
        """datetime.now(KST) 를 고정 (모듈의 datetime 만 교체)"""
        fake = MagicMock()
        fake.now.return_value = when
        p = patch.object(self.dm, "datetime", fake)
        p.start()
        self._patches.append(p)

    async def chart(self, sym, period):
        return await self.dm._get_chart_data_raw(sym, 30, period)

    # ── (1) 원본 공유 캐시 ────────────────────────────────────────
    async def test_switching_1m_5m_30m_costs_one_kis_call(self):
        r1 = await self.chart("005930", "1m")
        r5 = await self.chart("005930", "5m")
        r30 = await self.chart("005930", "30m")
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(all(r["success"] and r["minute"] for r in (r1, r5, r30)))
        self.assertEqual([len(r["data"]) for r in (r1, r5, r30)], [12, 3, 1])  # 09:00~09:11

    async def test_resampled_candles_are_correct(self):
        r5 = (await self.chart("005930", "5m"))["data"]
        first = r5[0]  # 09:00~09:04 (i=0..4)
        self.assertEqual((first["o"], first["h"], first["l"], first["c"], first["v"]), (100, 114, 90, 109, 50))
        self.assertEqual(first["t"], "090400")  # 마지막 1분봉 시각
        r30 = (await self.chart("005930", "30m"))["data"]
        self.assertEqual(len(r30), 1)
        self.assertEqual((r30[0]["o"], r30[0]["h"], r30[0]["l"], r30[0]["c"], r30[0]["v"]), (100, 121, 90, 116, 120))

    async def test_concurrent_unit_switches_share_one_kis_call(self):
        async def slow(url, kw):
            await asyncio.sleep(0.02)
            return {"rt_cd": "0", "msg1": "정상처리", "output2": _minute_rows()}

        self.set_handler(slow)
        res = await asyncio.gather(*[self.chart("005930", p) for p in ("1m", "5m", "30m", "5m")])
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(all(r["success"] for r in res))

    async def test_other_symbol_is_separate_cache_entry(self):
        await self.chart("005930", "1m")
        await self.chart("000660", "1m")
        self.assertEqual([s for _, s in self.calls], ["005930", "000660"])

    async def test_works_without_redis(self):
        with patch.object(self.dm, "redis_client", None):
            r = await self.chart("005930", "1m")
            await self.chart("005930", "5m")
        self.assertTrue(r["success"])
        self.assertEqual(len(self.calls), 2)  # 캐시 없으면 매번 조회 (오류 없이 동작)

    # ── TTL ────────────────────────────────────────────────────
    async def test_raw_ttl_function(self):
        f = self.dm._minute_raw_ttl
        self.assertEqual(f(LIVE), 10)
        self.assertEqual(f(datetime(2026, 9, 28, 15, 39, 59)), 10)
        self.assertEqual(f(AFTER), 1800)
        self.assertEqual(f(datetime(2026, 9, 28, 20, 0)), 1800)
        self.assertEqual(f(SATURDAY), 1800)
        self.assertEqual(f(datetime(2026, 9, 27, 10, 0)), 1800)  # 일요일

    async def test_after_close_cached_for_1800s(self):
        self.at(AFTER)
        await self.chart("005930", "1m")
        self.assertEqual(self.redis.setex_calls, [("cache:chart:min1:005930", 1800)])
        self.assertEqual(self.dm.ttl_for("5m"), 1800)

    async def test_market_hours_cached_for_10s(self):
        self.at(LIVE)
        await self.chart("005930", "1m")
        self.assertEqual(self.redis.setex_calls, [("cache:chart:min1:005930", 10)])
        self.assertEqual(self.dm.ttl_for("1m"), 10)
        self.assertEqual(self.dm.ttl_for("D"), 300)

    async def test_weekend_cached_for_1800s(self):
        self.at(SATURDAY)
        await self.chart("005930", "30m")
        self.assertEqual(self.redis.setex_calls[0][1], 1800)

    # ── (2) 실패: success False + 사유, 캐시 안 함 ─────────────────
    async def test_failure_returns_success_false_with_reason_and_is_not_cached(self):
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다.",
                                          "output2": []})
        res = await self.chart("005930", "5m")
        self.assertIs(res["success"], False)
        self.assertIn("기간이 만료된 token", res["error"])
        self.assertEqual(res["data"], [])
        self.assertEqual(self.redis.store, {})
        # 실패는 캐시하지 않으므로 다음 요청은 KIS 를 다시 부른다
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "정상처리", "output2": _minute_rows()})
        res = await self.chart("005930", "5m")
        self.assertTrue(res["success"])

    async def test_empty_result_is_not_cached_and_reports_reason(self):
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "조회할 자료가 없습니다.", "output2": []})
        res = await self.chart("005930", "1m")
        self.assertIs(res["success"], False)
        self.assertIn("조회할 자료가 없습니다.", res["error"])
        self.assertEqual(self.redis.store, {})

    async def test_exception_returns_reason_not_silent_empty(self):
        def boom(url, kw):
            raise RuntimeError("connection reset")

        self.set_handler(boom)
        res = await self.chart("005930", "1m")
        self.assertIs(res["success"], False)
        self.assertIn("connection reset", res["error"])
        self.assertEqual(self.redis.store, {})

    async def test_no_token_reports_reason(self):
        with patch.object(self.dm, "get_kis_token", new=AsyncMock(return_value=None)):
            res = await self.chart("005930", "1m")
        self.assertIs(res["success"], False)
        self.assertIn("토큰", res["error"])
        self.assertEqual(self.calls, [])

    async def test_failure_reason_and_logs_do_not_leak_account_or_keys(self):
        leaked = "계좌 12345678 앱키 APPKEY-SECRET-VALUE 거절 (12345678-01)"
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": leaked, "output2": []})
        with self.assertLogs("dashboard", level="INFO") as cm:
            res = await self.chart("005930", "1m")
        blob = res["error"] + "\n" + "\n".join(cm.output)
        for secret in ("12345678", "APPKEY-SECRET-VALUE", "APPSECRET-VALUE"):
            self.assertNotIn(secret, blob)
        self.assertIn("EGW00123", blob)

    # ── (2) 동시 호출 제한·재시도 ─────────────────────────────────
    async def test_rate_limit_response_is_retried(self):
        seq = [
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": RATE_LIMIT_MSG, "output2": []},
            {"rt_cd": "0", "msg1": "정상처리", "output2": _minute_rows()},
        ]
        self.set_handler(lambda url, kw: seq.pop(0))
        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            res = await self.chart("005930", "1m")
        self.assertTrue(res["success"])
        self.assertEqual(len(self.calls), 2)
        mock_sleep.assert_awaited_once_with(0.7)

    async def test_rate_limit_retries_capped_at_two_then_fail_with_reason(self):
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": RATE_LIMIT_MSG, "output2": []})
        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            res = await self.chart("005930", "1m")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(mock_sleep.await_count, 2)
        self.assertIs(res["success"], False)
        self.assertIn("초당", res["error"])
        self.assertEqual(self.redis.store, {})

    async def test_minute_calls_respect_kis_concurrency_limit(self):
        state = {"in_flight": 0, "max": 0}

        async def handler(url, kw):
            state["in_flight"] += 1
            state["max"] = max(state["max"], state["in_flight"])
            await asyncio.sleep(0.02)
            state["in_flight"] -= 1
            return {"rt_cd": "0", "msg1": "정상처리", "output2": _minute_rows()}

        self.set_handler(handler)
        res = await asyncio.gather(*[self.chart(f"00{i:04d}", "1m") for i in range(8)])
        self.assertEqual(len(self.calls), 8)
        self.assertEqual(state["max"], 2)
        self.assertTrue(all(r["success"] for r in res))

    # ── (3) 로그 ──────────────────────────────────────────────
    async def test_info_log_per_request_with_symbol_unit_hit_and_ms(self):
        with self.assertLogs("dashboard", level="INFO") as cm:
            await self.chart("005930", "1m")
            await self.chart("005930", "5m")
        lines = [l for l in cm.output if "분봉 조회 [" in l]
        self.assertEqual(len(lines), 2)
        self.assertIn("005930", lines[0]); self.assertIn("단위=1m", lines[0]); self.assertIn("캐시=미스", lines[0])
        self.assertIn("단위=5m", lines[1]); self.assertIn("캐시=히트", lines[1])
        for l in lines:
            self.assertRegex(l, r"\d+ms")
            for secret in ("12345678", "APPKEY-SECRET-VALUE", "APPSECRET-VALUE"):
                self.assertNotIn(secret, l)

    async def test_daily_path_unchanged_still_works(self):
        """일봉은 기존 경로 그대로 (분봉 캐시와 무관)"""
        def handler(url, kw):
            return {"rt_cd": "0", "msg1": "정상처리", "output2": [
                {"stck_bsop_date": "20260925", "stck_oprc": "100", "stck_hgpr": "110",
                 "stck_lwpr": "90", "stck_clpr": "105", "acml_vol": "1000"}]}
        self.set_handler(handler)
        res = await self.dm._get_chart_data_raw("005930", 90, "D")
        self.assertTrue(res["success"])
        self.assertNotIn("minute", res)
        self.assertEqual(self.redis.store, {})


if __name__ == "__main__":
    unittest.main()
