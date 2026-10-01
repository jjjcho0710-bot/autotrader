"""
tests/test_intraday_scan_alert_cleanup.py - 장중 보충 스캔 메시지/급등주 필터 회귀 테스트

검증 항목:
1. 신규 감시 종목이 8개를 넘으면 "외 N종목 더 있음"을 덧붙여 전부 안내한다 (제목 개수와 본문 불일치 방지)
2. 이미 보유 중인 종목은 "신규 감시" 표시에서 제외하되, watchlist 등록 자체는 유지한다
3. 보유 종목 조회가 실패하면 제외 없이 기존대로 표시한다
4. _kis_scan_candidates가 집계한 당일 급등 제외 종목 수를 텔레그램에 "급등 제외 N종목"으로 표시한다
5. _kis_scan_candidates는 당일 등락률이 SCAN_MAX_CHANGE_PCT(기본 15%)를 초과하는 후보를 감시 편입에서 제외한다
"""
import asyncio
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm


def _candidate(symbol, name, change=1.0, close=10000, score=5):
    return {"symbol": symbol, "name": name, "close": close, "change": change,
            "vol_ratio": 2.0, "score": score, "rsi": 50.0}


class FakeConn:
    def __init__(self, existing_symbols):
        self.existing_symbols = set(existing_symbols)
        self.executed = []

    async def fetch(self, query, *args):
        return [{"symbol": s} for s in self.existing_symbols]

    async def execute(self, query, *args):
        self.executed.append(args)


class FakePool:
    def __init__(self, existing_symbols=()):
        self.conn = FakeConn(existing_symbols)

    def acquire(self):
        return self

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class TestIntradayScanDisplay(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        self.sent = []

        async def fake_send_telegram(text, **kw):
            self.sent.append(text)

        self.fake_send_telegram = fake_send_telegram
        start(patch.object(dm, "_send_telegram", side_effect=fake_send_telegram))
        start(patch.object(dm, "_last_kis_scan_surge_excluded", 0))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def test_more_than_8_new_candidates_shows_overflow_note(self):
        """9개 이상 신규 감시 종목은 제목 전체 개수 + 8개 본문 + '외 N종목 더 있음'"""
        candidates = [_candidate(f"{i:06d}", f"종목{i}") for i in range(10)]
        with patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=candidates)), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": True, "data": []})), \
             patch.object(dm, "db_pool", FakePool(existing_symbols=[])):
            await dm._intraday_scan()

        self.assertEqual(len(self.sent), 1)
        msg = self.sent[0]
        self.assertIn("신규 감시 10종목", msg)
        self.assertEqual(msg.count("· 종목"), 8)
        self.assertIn("외 2종목 더 있음", msg)

    async def test_held_symbols_excluded_from_display_but_registered(self):
        """보유 종목은 '신규 감시' 표시에서 빠지되 watchlist 등록은 그대로 유지"""
        candidates = [_candidate(f"{i:06d}", f"종목{i}") for i in range(5)]
        held = {"000000", "000001"}  # 종목0, 종목1 보유 중
        pool = FakePool(existing_symbols=[])
        with patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=candidates)), \
             patch.object(dm, "get_stock_positions",
                          new=AsyncMock(return_value={"success": True,
                                                       "data": [{"symbol": s} for s in held]})), \
             patch.object(dm, "db_pool", pool):
            await dm._intraday_scan()

        self.assertEqual(len(self.sent), 1)
        msg = self.sent[0]
        self.assertIn("신규 감시 3종목", msg)
        self.assertNotIn("종목0", msg)
        self.assertNotIn("종목1", msg)
        self.assertIn("종목2", msg)
        # 보유 종목도 watchlist에는 그대로 등록(손절 감시 유지)
        self.assertEqual(len(pool.conn.executed), 5)

    async def test_held_lookup_failure_falls_back_to_showing_all(self):
        """보유 종목 조회 실패 시 제외 없이 기존대로 전부 표시"""
        candidates = [_candidate(f"{i:06d}", f"종목{i}") for i in range(3)]
        with patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=candidates)), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(side_effect=Exception("KIS 오류"))), \
             patch.object(dm, "db_pool", FakePool(existing_symbols=[])):
            await dm._intraday_scan()

        self.assertEqual(len(self.sent), 1)
        msg = self.sent[0]
        self.assertIn("신규 감시 3종목", msg)
        for i in range(3):
            self.assertIn(f"종목{i}", msg)

    async def test_surge_excluded_count_appended_to_telegram_message(self):
        """_kis_scan_candidates가 남긴 급등 제외 집계를 텔레그램 한 줄로 표시"""
        candidates = [_candidate("0000010", "종목0")]
        with patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=candidates)), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": True, "data": []})), \
             patch.object(dm, "db_pool", FakePool(existing_symbols=[])), \
             patch.object(dm, "_last_kis_scan_surge_excluded", 3):
            await dm._intraday_scan()

        self.assertEqual(len(self.sent), 1)
        self.assertIn("급등 제외 3종목", self.sent[0])


class FakeHttpResponse:
    def __init__(self, payload):
        self._payload = payload

    async def json(self, *a, **kw):
        return self._payload


class FakeHttpSession:
    def __init__(self, handler):
        self._handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        return FakeHttpResponse(await self._handler(url, kwargs))


def _daily_rows(closes_oldest_to_newest, vols_oldest_to_newest):
    """실제 KIS 응답은 최신→과거 순으로 내려오므로 역순으로 구성"""
    api_order = list(zip(closes_oldest_to_newest, vols_oldest_to_newest))[::-1]
    return [{"stck_clpr": str(c), "acml_vol": str(v)} for c, v in api_order]


class TestKisScanCandidatesSurgeFilter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        start(patch.object(dm, "get_kis_token", new=AsyncMock(return_value="tok")))
        start(patch.object(dm, "_get_price_ceiling", new=AsyncMock(return_value=0)))
        start(patch.object(dm.config, "KIS_IS_PAPER", False))
        start(patch.object(dm.config, "KIS_APP_KEY", "APPKEY-VALUE"))
        start(patch.object(dm.config, "KIS_APP_SECRET", "APPSECRET-VALUE"))
        start(patch("asyncio.sleep", new=AsyncMock()))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def _set_handler(self, handler):
        fake = SimpleNamespace(
            ClientSession=lambda **kw: FakeHttpSession(handler),
            TCPConnector=lambda **kw: None,
            ClientTimeout=lambda **kw: None,
        )
        p = patch.object(dm, "_aiohttp", fake)
        p.start()
        self._patches.append(p)

    async def test_surge_candidate_excluded_normal_candidate_passes(self):
        """당일 등락률 20%(SCAN_MAX_CHANGE_PCT 초과)는 제외, 5%(이하)는 통과"""
        surge_symbol, surge_name = "000120", "윈팩가상"
        normal_symbol, normal_name = "000130", "정상가상"

        volume_rank_payload = {
            "output": [
                {"mksc_shrn_iscd": surge_symbol, "hts_kor_isnm": surge_name},
                {"mksc_shrn_iscd": normal_symbol, "hts_kor_isnm": normal_name},
            ]
        }

        surge_closes = [10000] * 24 + [12000]       # 변동률 +20%
        surge_vols = [10000] * 25
        normal_closes = [10000] * 24 + [10500]      # 변동률 +5%
        normal_vols = [10000] * 24 + [30000]    # 거래량 급증 조건 충족

        async def handler(url, kw):
            tr_id = kw["headers"]["tr_id"]
            if tr_id == "FHPST01710000":
                mkt = kw["params"]["FID_INPUT_ISCD"]
                if mkt == "0001":
                    return volume_rank_payload
                return {"output": []}
            symbol = kw["params"]["FID_INPUT_ISCD"]
            if symbol == surge_symbol:
                return {"output2": _daily_rows(surge_closes, surge_vols)}
            return {"output2": _daily_rows(normal_closes, normal_vols)}

        self._set_handler(handler)

        results = await dm._kis_scan_candidates()

        self.assertIsNotNone(results)
        symbols_in_results = {r["symbol"] for r in results}
        self.assertNotIn(surge_symbol, symbols_in_results)
        self.assertIn(normal_symbol, symbols_in_results)
        self.assertEqual(dm._last_kis_scan_surge_excluded, 1)

    async def test_change_at_threshold_is_not_excluded(self):
        """등락률이 상한선(SCAN_MAX_CHANGE_PCT) 이하면 제외되지 않는다"""
        symbol = "000140"
        volume_rank_payload = {"output": [{"mksc_shrn_iscd": symbol, "hts_kor_isnm": "경계값종목"}]}
        # 정확히 상한값(15%)
        closes = [10000] * 24 + [11500]
        vols = [10000] * 24 + [30000]

        async def handler(url, kw):
            tr_id = kw["headers"]["tr_id"]
            if tr_id == "FHPST01710000":
                mkt = kw["params"]["FID_INPUT_ISCD"]
                if mkt == "0001":
                    return volume_rank_payload
                return {"output": []}
            return {"output2": _daily_rows(closes, vols)}

        self._set_handler(handler)

        results = await dm._kis_scan_candidates()

        self.assertIsNotNone(results)
        self.assertIn(symbol, {r["symbol"] for r in results})
        self.assertEqual(dm._last_kis_scan_surge_excluded, 0)


if __name__ == "__main__":
    unittest.main()
