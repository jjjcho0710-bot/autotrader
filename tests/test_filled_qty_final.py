"""
[AT] fix/filled-qty-final 회귀 테스트.

배경: 10/14 023790 손절 SELL이 KIS=135주 체결됐는데 DB=1.0으로 기록됐다. 체결수량
재조회 루프(stock_trader/kis_trader.py buy()/sell(), dashboard/main.py
_kis_stock_order)가 filled_qty>0이면 즉시 break하고 요청수량과 비교하지 않아서,
재확인 중 관찰된 "첫 번째 0 초과 값"(부분체결 중간값)을 최종값으로 써버린 것이 원인이다.

이 파일은 두 구현(stock_trader/kis_trader.py, dashboard/main.py) 모두에서:
- 0<filled_qty<요청수량이면 기존 재시도 대기(_FILL_RETRY_DELAYS_SEC) 범위 안에서
  계속 재조회해 관찰된 최댓값을 최종값으로 쓴다(총 대기시간 불변).
- 요청수량 이상이 확인되면 그 즉시 재조회를 멈춘다.
- 재시도 소진 후에도 미달이면 최댓값 + partial 표시(WARNING)를 쓴다.
- _get_filled_qty/_get_kis_filled_qty는 같은 odno 행이 여러 개면 누적체결수량
  (tot_ccld_qty) 최댓값을 쓴다(첫 행만 쓰지 않음).
"""
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


def _stub_missing_runtime_deps():
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

    for mod_name in [
        "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
        "google", "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402
from common.database import cache  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)


class DummyResponse:
    def __init__(self, data):
        self._data = data

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class DummySession:
    def __init__(self, resp_data):
        self.resp_data = resp_data

    def post(self, url, headers=None, json=None):
        return DummyResponse(self.resp_data)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


# ── stock_trader/kis_trader.py: buy()/sell() 최댓값 추적 ────────────────────


class TestKisTraderBuySellMaxTracking(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"
        self.order_resp = {
            "rt_cd": "0", "output": {"ODNO": "0009135000"}, "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(self.order_resp)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_sequence_1_1_135_uses_max_not_first_nonzero(self, mock_sleep):
        """[1,1,135] — 중간값(1)에서 멈추지 않고 최종 135로 확정해야 한다(10/14 사고 재현)."""
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(1, 70000.0), (1, 70000.0), (135, 70000.0)])

        res = await self.trader.buy("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_sequence_1_1_135_uses_max_not_first_nonzero(self, mock_sleep):
        """매도도 동일하게 [1,1,135] → 135로 확정해야 한다(023790 손절 SELL 사고 재현)."""
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(1, 70000.0), (1, 70000.0), (135, 70000.0)])

        res = await self.trader.sell("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_sequence_1_1_1_exhausts_retries_as_partial(self, mock_sleep):
        """[1,1,1] — 끝까지 1주에서 더 늘지 않으면 pending이 아니라 partial=True로 확정한다."""
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(1, 70000.0), (1, 70000.0), (1, 70000.0)])

        res = await self.trader.buy("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 1)
        self.assertTrue(res["partial"])
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_sequence_1_1_1_exhausts_retries_as_partial(self, mock_sleep):
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(1, 70000.0), (1, 70000.0), (1, 70000.0)])

        res = await self.trader.sell("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 1)
        self.assertTrue(res["partial"])
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_sequence_0_0_135_confirms_full_fill(self, mock_sleep):
        """[0,0,135] — 0으로 머물다 마지막에 전량 체결되면 그대로 135로 확정한다."""
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(0, 0.0), (0, 0.0), (135, 70000.0)])

        res = await self.trader.buy("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_sequence_0_0_135_confirms_full_fill(self, mock_sleep):
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(0, 0.0), (0, 0.0), (135, 70000.0)])

        res = await self.trader.sell("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_sequence_135_stops_immediately_no_retry(self, mock_sleep):
        """[135] — 첫 확인에서 바로 전량 체결이면 재조회를 추가로 하지 않는다."""
        self.trader._get_filled_qty = AsyncMock(return_value=(135, 70000.0))

        res = await self.trader.buy("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 1)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_sequence_135_stops_immediately_no_retry(self, mock_sleep):
        self.trader._get_filled_qty = AsyncMock(return_value=(135, 70000.0))

        res = await self.trader.sell("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 135)
        self.assertNotIn("partial", res)
        self.assertEqual(self.trader._get_filled_qty.await_count, 1)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_never_regresses_to_smaller_observed_value(self, mock_sleep):
        """최댓값 추적 — 중간에 더 작은 값이 조회돼도(네트워크 지연 등) 이전 최댓값을
        덮어쓰지 않는다."""
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(100, 70000.0), (0, 0.0), (0, 0.0)])

        res = await self.trader.buy("023790", 70000, 135)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 100)
        self.assertTrue(res["partial"])

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_pending_when_every_retry_still_zero(self, mock_sleep):
        """끝까지 0주면 여전히 pending(기존 동작 유지) — Redis 락 저장도 그대로."""
        self.trader._get_filled_qty = AsyncMock(return_value=(0, 0.0))
        fake_redis = FakeRedis()

        with patch.object(cache, "client", fake_redis):
            res = await self.trader.buy("023790", 70000, 135, strategy="MA크로스_매수")

        self.assertFalse(res["success"])
        self.assertTrue(res.get("pending"))
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)


# ── stock_trader/kis_trader.py: _get_filled_qty 다중 odno 행 최댓값 ─────────


class TestGetFilledQtyMultiRowMax(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    async def test_multiple_rows_same_odno_uses_max_tot_ccld_qty(self):
        """같은 odno가 여러 행으로 쪼개져 내려오면 첫 행이 아니라 누적체결수량
        최댓값을 가진 행을 쓴다."""

        class GetResponse:
            def __init__(self, data):
                self._data = data

            async def json(self):
                return self._data

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                pass

        class GetSession:
            def get(self, url, headers=None, params=None):
                return GetResponse({
                    "output1": [
                        {"odno": "O1", "tot_ccld_qty": "50", "avg_prvs": "70000"},
                        {"odno": "O1", "tot_ccld_qty": "135", "avg_prvs": "70050"},
                        {"odno": "OTHER", "tot_ccld_qty": "999", "avg_prvs": "1"},
                    ]
                })

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                pass

        self.trader._new_session = lambda: GetSession()

        fq, avg = await self.trader._get_filled_qty("O1", "023790", with_price=True)

        self.assertEqual(fq, 135)
        self.assertEqual(avg, 70050.0)


# ── dashboard/main.py: _kis_stock_order 최댓값 추적(두 번째 구현) ──────────


def _load_dashboard_main():
    import dashboard.main as dm
    return dm


class FakeKisConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True


class FakePostResponse:
    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class FakeOrderAndFillSession:
    def __init__(self, post_result, get_results):
        self._post_result = post_result
        self._get_results = list(get_results)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        return self._post_result

    def get(self, *args, **kwargs):
        return self._get_results.pop(0)


def _fill_resp(qty, avg_price=70000):
    return FakePostResponse({
        "output1": [{"odno": "O135", "tot_ccld_qty": str(qty), "avg_prvs": str(avg_price)}]})


class TestDashboardKisStockOrderMaxTracking(unittest.IsolatedAsyncioTestCase):
    def _patches(self, get_responses):
        import dashboard.main as dm
        return (
            patch.object(dm, "config", FakeKisConfig()),
            patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")),
            patch("dashboard.main.asyncio.sleep", new=AsyncMock()),
            patch("aiohttp.ClientSession", return_value=FakeOrderAndFillSession(
                FakePostResponse({"rt_cd": "0", "output": {"ODNO": "O135"}}), get_responses)),
        )

    async def test_buy_sequence_1_1_135_uses_max_not_first_nonzero(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(1), _fill_resp(1), _fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, True)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)

    async def test_sell_sequence_1_1_135_uses_max_not_first_nonzero(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(1), _fill_resp(1), _fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, False)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)

    async def test_buy_sequence_1_1_1_exhausts_retries_as_partial(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(1), _fill_resp(1), _fill_resp(1)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, True)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 1)
        self.assertTrue(result["partial"])

    async def test_sell_sequence_1_1_1_exhausts_retries_as_partial(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(1), _fill_resp(1), _fill_resp(1)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, False)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 1)
        self.assertTrue(result["partial"])

    async def test_buy_sequence_0_0_135_confirms_full_fill(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(0), _fill_resp(0), _fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, True)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)

    async def test_sell_sequence_0_0_135_confirms_full_fill(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(0), _fill_resp(0), _fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, False)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)

    async def test_buy_sequence_135_stops_immediately_no_retry(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, True)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)

    async def test_sell_sequence_135_stops_immediately_no_retry(self):
        dm = _load_dashboard_main()
        patches = self._patches([_fill_resp(135)])
        with patches[0], patches[1], patches[2], patches[3]:
            result = await dm._kis_stock_order("023790", 70000, 135, False)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 135)
        self.assertNotIn("partial", result)


# ── dashboard/main.py: _get_kis_filled_qty 다중 odno 행 최댓값 ─────────────


class TestDashboardGetKisFilledQtyMultiRowMax(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_rows_same_odno_uses_max_tot_ccld_qty(self):
        dm = _load_dashboard_main()
        get_resp = FakePostResponse({
            "output1": [
                {"odno": "O1", "tot_ccld_qty": "50", "avg_prvs": "70000"},
                {"odno": "O1", "tot_ccld_qty": "135", "avg_prvs": "70050"},
                {"odno": "OTHER", "tot_ccld_qty": "999", "avg_prvs": "1"},
            ]
        })

        class GetOnlySession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            def get(self, *args, **kwargs):
                return get_resp

        with patch.object(dm, "config", FakeKisConfig()), \
             patch("aiohttp.ClientSession", return_value=GetOnlySession()):
            fq, avg = await dm._get_kis_filled_qty(
                "TOKEN", "12345678", "01", "O1", "023790", True, with_price=True)

        self.assertEqual(fq, 135)
        self.assertEqual(avg, 70050.0)


if __name__ == "__main__":
    unittest.main()
