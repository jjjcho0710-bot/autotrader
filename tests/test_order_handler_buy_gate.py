"""
router/handlers/order_handler.handle_trade_command() 매수 안전장치 관문(buy_gate) 통합
테스트 ([AT] buy-gate-unification).

배경: 채팅 직접 매수("삼성전자 3주 매수")가 stark/execution_guard를 전혀 거치지 않아
투자경고 종목 추가매수가 가드 없이 체결된 사고(10/1 11:02)가 있었다. 이 테스트는 현재가
확인 직후·사이징 한도 적용 전에 호출되는 buy_gate가 투자경고·악재공시·당일 손절 2회·
보유종목수 한도·물타기 위반 각각에서 실제로 주문을 막는지, 그리고 "한도무시" 키워드가
붙어도(사이징 금액 한도만 무시할 뿐) 이 안전 차단은 넘지 못하는지 handle_trade_command
전체를 통해 검증한다. 매도 경로는 건드리지 않았으므로 여기서 다루지 않는다
(tests/test_order_handler.py 참고).
"""
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from tests.test_order_handler import FakeConfig, FakePool, FakeRedis, make_quote_fn  # noqa: E402


def _quote_fn(price: int = 70000, name: str = "삼성전자"):
    return make_quote_fn({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})


async def _ok_positions():
    return {"success": True, "data": []}


async def _ok_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


class TestOrderHandlerBuyGateBlocks(unittest.IsolatedAsyncioTestCase):
    def _kwargs(self, *, pool=None, get_stock_positions_fn=None, get_market_warning_fn=None):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        self.orders = []

        async def kis_order(symbol, price, qty, is_buy):
            self.orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        return dict(
            pool=pool or FakePool(), redis=FakeRedis(), universe=universe,
            get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
            get_stock_positions_fn=get_stock_positions_fn or _ok_positions,
            get_market_warning_fn=get_market_warning_fn or _ok_warning,
            get_quote_fn=_quote_fn(),
        )

    async def _run(self, msg, **kwargs):
        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        full_kwargs = self._kwargs(**kwargs)
        full_kwargs["send_telegram_fn"] = send_telegram
        full_kwargs["log_journal_fn"] = log_journal
        return await order_handler.handle_trade_command(msg, **full_kwargs)

    async def test_blocked_by_investment_warning(self):
        async def warning(symbol):
            return {"mrkt_warn_cls_code": "02", "vi_cls_code": "N"}

        reply = await self._run("삼성전자 2주 매수", get_market_warning_fn=warning)
        self.assertIn("⛔", reply)
        self.assertIn("투자경고", reply)
        self.assertEqual(self.orders, [])

    async def test_blocked_by_vi_triggered(self):
        async def warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "Y"}

        reply = await self._run("삼성전자 2주 매수", get_market_warning_fn=warning)
        self.assertIn("⛔", reply)
        self.assertIn("VI", reply)
        self.assertEqual(self.orders, [])

    async def test_blocked_by_bad_disclosure(self):
        pool = FakePool(disclosures=[{"report_name": "상장폐지 사유 발생"}])
        reply = await self._run("삼성전자 2주 매수", pool=pool)
        self.assertIn("⛔", reply)
        self.assertIn("공시", reply)
        self.assertEqual(self.orders, [])

    async def test_blocked_by_daily_stop_loss_limit(self):
        pool = FakePool(stop_loss_count=2)
        reply = await self._run("삼성전자 2주 매수", pool=pool)
        self.assertIn("⛔", reply)
        self.assertIn("손절", reply)
        self.assertEqual(self.orders, [])

    async def test_blocked_by_max_positions_limit(self):
        current = [{"symbol": f"0000{i}"} for i in range(1, 6)]

        async def get_positions():
            return {"success": True, "data": current}

        reply = await self._run("삼성전자 2주 매수", get_stock_positions_fn=get_positions)
        self.assertIn("⛔", reply)
        self.assertIn("한도 초과", reply)
        self.assertEqual(self.orders, [])

    async def test_blocked_by_averaging_down(self):
        async def get_positions():
            return {"success": True, "data": [{"symbol": "005930"}]}

        # 이미 보유 중(005930)이고 평생 1회 물타기 한도를 이미 썼음(BUY 이력 2건)
        # → 가격 조건(-3% 이내)과 무관하게 즉시 차단돼야 한다.
        pool = FakePool(buy_history={"005930": [{"price": 70000}, {"price": 69000}]})
        reply = await self._run(
            "삼성전자 2주 매수", pool=pool, get_stock_positions_fn=get_positions)
        self.assertIn("⛔", reply)
        self.assertIn("물타기", reply)
        self.assertEqual(self.orders, [])

    async def test_hando_muzi_does_not_bypass_safety_block(self):
        """'한도무시'는 사이징 금액 한도만 무시할 뿐, buy_gate의 안전 차단은 넘지 못한다."""
        async def warning(symbol):
            return {"mrkt_warn_cls_code": "02", "vi_cls_code": "N"}

        reply = await self._run("삼성전자 2주 매수 한도무시", get_market_warning_fn=warning)
        self.assertIn("⛔", reply)
        self.assertIn("투자경고", reply)
        self.assertEqual(self.orders, [])
        self.assertNotIn("매수 완료", reply)

    async def test_passes_through_when_nothing_blocks(self):
        reply = await self._run("삼성전자 2주 매수")
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)


if __name__ == "__main__":
    unittest.main()
