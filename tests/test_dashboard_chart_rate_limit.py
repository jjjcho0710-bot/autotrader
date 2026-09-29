"""
대시보드 차트(/api/chart/{symbol}) KIS 호출 제한 대응 단위 테스트
- KIS 시세 조회 동시 호출 2건 제한
- "초당 거래건수 초과" 응답 시 재시도
- 데이터가 비면 success False + 사유 반환
"""
import asyncio
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, PropertyMock, patch

from unittest.mock import MagicMock

# dashboard.main 로드 전 필요한 외부 모듈 모킹
for mod_name in [
    "fastapi",
    "fastapi.staticfiles",
    "fastapi.responses",
    "fastapi.middleware.cors",
    "asyncpg",
    "redis",
    "redis.asyncio",
    "aiohttp",
    "google",
    "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

RATE_LIMIT_MSG = "초당 거래건수를 초과하였습니다."
PAPER_BASE = "https://openapivts.koreainvestment.com:29443"
REAL_BASE = "https://openapi.koreainvestment.com:9443"


def _rows(n: int = 3) -> list:
    return [
        {"stck_bsop_date": f"202609{20 + i:02d}", "stck_oprc": "100", "stck_hgpr": "110",
         "stck_lwpr": "90", "stck_clpr": str(100 + i), "acml_vol": "1000"}
        for i in range(n)
    ]


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    async def json(self, *args, **kwargs):
        return self._payload


class FakeSession:
    """handler(url, kwargs) → payload dict (코루틴 가능)"""

    def __init__(self, handler):
        self._handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        return FakeResponse(await self._handler(url, kwargs))


class TestDashboardChartRateLimit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.calls = []  # (url, symbol)
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        # 이벤트 루프가 테스트마다 새로 만들어지므로 세마포어도 새로 생성
        start(patch.object(dm, "_KIS_QUOTE_SEM", asyncio.Semaphore(dm._KIS_QUOTE_CONCURRENCY)))
        start(patch.object(dm, "get_kis_token", new=AsyncMock(return_value="tok")))
        start(patch.object(dm.config, "KIS_IS_PAPER", False))
        start(patch.object(dm.config, "KIS_APP_KEY", "APPKEY-SECRET-VALUE"))
        start(patch.object(dm.config, "KIS_APP_SECRET", "APPSECRET-VALUE"))
        start(patch.object(type(dm.config), "kis_account_no",
                           new_callable=PropertyMock, return_value="12345678-01"))
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "정상처리", "output2": _rows()})

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

    async def test_concurrent_chart_requests_never_exceed_two_kis_calls(self):
        """차트 요청 10개를 동시에 보내도 KIS 일봉 호출 동시성은 2를 넘지 않아야 함"""
        state = {"in_flight": 0, "max": 0}

        async def handler(url, kw):
            state["in_flight"] += 1
            state["max"] = max(state["max"], state["in_flight"])
            await asyncio.sleep(0.02)
            state["in_flight"] -= 1
            return {"rt_cd": "0", "msg1": "정상처리", "output2": _rows()}

        self.set_handler(handler)

        results = await asyncio.gather(*[
            self.dm._get_chart_data_raw(f"00{i:04d}", 90, "D") for i in range(10)
        ])

        self.assertEqual(len(self.calls), 10)
        self.assertEqual(self.dm._KIS_QUOTE_CONCURRENCY, 2)
        self.assertLessEqual(state["max"], 2)
        self.assertEqual(state["max"], 2)  # 제한이 실제로 병렬 2건까지는 허용하는지
        for r in results:
            self.assertTrue(r["success"])
            self.assertEqual(len(r["data"]), 3)

    async def test_rate_limit_response_is_retried_and_returns_data(self):
        """첫 시도에서 '초당 거래건수 초과'가 나도 0.7초 뒤 재시도로 데이터가 와야 함"""
        seq = [
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": RATE_LIMIT_MSG, "output2": []},
            {"rt_cd": "0", "msg1": "정상처리", "output2": _rows(5)},
        ]
        self.set_handler(lambda url, kw: seq.pop(0))

        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            res = await self.dm._get_chart_data_raw("003010", 90, "D")

        self.assertTrue(res["success"])
        self.assertEqual(len(res["data"]), 5)
        self.assertEqual(len(self.calls), 2)
        mock_sleep.assert_awaited_once_with(0.7)

    async def test_rate_limit_retries_are_capped_at_two(self):
        """계속 '초당' 거절이면 최초 1회 + 재시도 2회 = 3회까지만 호출하고 사유와 함께 실패해야 함"""
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00201",
                                          "msg1": RATE_LIMIT_MSG, "output2": []})

        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            res = await self.dm._get_chart_data_raw("003010", 90, "D")

        self.assertEqual(len(self.calls), 3)
        self.assertEqual(mock_sleep.await_count, 2)
        self.assertFalse(res["success"])
        self.assertIn("초당", res["error"])
        self.assertEqual(res["data"], [])

    async def test_empty_response_returns_success_false_with_reason(self):
        """빈 응답이면 조용히 빈 목록을 주지 않고 success False + KIS 응답 사유를 반환해야 함"""
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "조회할 자료가 없습니다.", "output2": []})

        res = await self.dm._get_chart_data_raw("073240", 90, "D")

        self.assertIs(res["success"], False)
        self.assertIn("조회할 자료가 없습니다.", res["error"])
        self.assertEqual(res["data"], [])

    async def test_failure_reason_and_log_do_not_leak_account_or_keys(self):
        """사유(응답)와 로그에 계좌번호·앱키가 평문으로 남지 않아야 함"""
        leaked = "계좌 12345678 앱키 APPKEY-SECRET-VALUE 거절 (12345678-01)"
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": leaked, "output2": []})

        with self.assertLogs("dashboard", level="WARNING") as cm:
            res = await self.dm._get_chart_data_raw("003010", 90, "D")

        blob = res["error"] + "\n" + "\n".join(cm.output)
        self.assertFalse(res["success"])
        for secret in ("12345678", "APPKEY-SECRET-VALUE", "APPSECRET-VALUE"):
            self.assertNotIn(secret, blob)
        self.assertIn("EGW00123", blob)

    async def test_paper_mode_tries_real_server_fallback_when_primary_empty(self):
        """모의투자에서 기본 서버가 비면 실전 서버로 1회 더 시도하고, 거기서 데이터가 오면 성공해야 함"""
        def handler(url, kw):
            if url.startswith(PAPER_BASE):
                return {"rt_cd": "0", "msg1": "정상처리", "output2": []}
            return {"rt_cd": "0", "msg1": "정상처리", "output2": _rows(4)}

        self.set_handler(handler)

        with patch.object(self.dm.config, "KIS_IS_PAPER", True):
            res = await self.dm._get_chart_data_raw("003010", 90, "D")

        self.assertTrue(res["success"])
        self.assertEqual(len(res["data"]), 4)
        self.assertEqual([u.split("/uapi")[0] for u, _ in self.calls], [PAPER_BASE, REAL_BASE])

    async def test_paper_mode_both_servers_empty_reports_primary_reason(self):
        """모의/실전 둘 다 빈 응답이면 기본(모의) 서버의 사유를 success False로 반환해야 함"""
        def handler(url, kw):
            if url.startswith(PAPER_BASE):
                return {"rt_cd": "0", "msg1": "모의 빈응답", "output2": []}
            return {"rt_cd": "1", "msg_cd": "EGW00121", "msg1": "유효하지 않은 token 입니다.", "output2": []}

        self.set_handler(handler)

        with patch.object(self.dm.config, "KIS_IS_PAPER", True):
            res = await self.dm._get_chart_data_raw("003010", 90, "D")

        self.assertFalse(res["success"])
        self.assertIn("모의 빈응답", res["error"])
        self.assertEqual(len(self.calls), 2)

    async def test_daily_empty_error_response_retries_once_and_succeeds(self):
        """오류 응답(rt_cd != "0")으로 일봉이 비어도 1.5초 뒤 재시도에서 데이터가 오면 성공해야 함"""
        seq = [
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "일시적 오류", "output2": []},
            {"rt_cd": "0", "msg1": "정상처리", "output2": _rows(4)},
        ]
        self.set_handler(lambda url, kw: seq.pop(0))

        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            rows, err = await self.dm._fetch_daily_ohlcv_ex("024060", 40)

        self.assertEqual(len(rows), 4)
        self.assertEqual(err, "")
        self.assertEqual(len(self.calls), 2)
        mock_sleep.assert_awaited_once_with(self.dm._KIS_DAILY_RETRY_DELAY)

    async def test_daily_empty_error_response_retry_also_fails(self):
        """재시도까지 계속 오류 응답이면 그때 사유와 함께 실패 처리해야 함"""
        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg_cd": "EGW00123",
                                          "msg1": "지속 오류", "output2": []})

        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            rows, err = await self.dm._fetch_daily_ohlcv_ex("024060", 40)

        self.assertEqual(rows, [])
        self.assertIn("지속 오류", err)
        self.assertEqual(len(self.calls), 2)
        mock_sleep.assert_awaited_once_with(self.dm._KIS_DAILY_RETRY_DELAY)

    async def test_daily_legitimate_no_data_response_does_not_retry(self):
        """rt_cd="0"인 정상 빈 응답(진짜 데이터 없음)은 재시도하지 않아야 함"""
        self.set_handler(lambda url, kw: {"rt_cd": "0", "msg1": "조회할 자료가 없습니다.", "output2": []})

        with patch("dashboard.main.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            rows, err = await self.dm._fetch_daily_ohlcv_ex("024060", 40)

        self.assertEqual(rows, [])
        self.assertIn("조회할 자료가 없습니다.", err)
        self.assertEqual(len(self.calls), 1)
        mock_sleep.assert_not_awaited()

    async def test_fetch_daily_ohlcv_keeps_list_contract_for_other_callers(self):
        """_fetch_daily_ohlcv는 기존 호출자(차트 분석·챗 컨텍스트)를 위해 성공 시 목록, 실패 시 빈 목록을 반환해야 함"""
        rows = await self.dm._fetch_daily_ohlcv("003010", 40)
        self.assertEqual([r["close"] for r in rows], [100, 101, 102])

        self.set_handler(lambda url, kw: {"rt_cd": "1", "msg1": "실패", "output2": []})
        self.assertEqual(await self.dm._fetch_daily_ohlcv("003010", 40), [])


if __name__ == "__main__":
    unittest.main()
