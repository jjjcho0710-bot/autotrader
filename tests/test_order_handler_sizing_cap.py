"""
router/handlers/order_handler.py 리스크 기반 매수 사이징 한도([AT] fix/chat-order-sizing-cap) 테스트.

배경: 채팅 "N주 매수" 지시가 자동매매의 리스크 기반 사이징(종목당 최대 손실 0.75%)을 거치지
않고 사용자가 말한 수량 그대로 주문되는 문제(9/30 삼성바이오로직스 2주×140만원=280만원 매수,
기준의 2.6배)가 있었다. 사이징 수학(기본금액·ATR 변동성배율)은 common/position_sizing.py
공용 모듈을 쓴다(tests/test_position_sizing.py 가 stock_trader 쪽 회귀를 검증).
"""
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from tests.test_order_handler import FakeConfig, FakePool, FakeRedis, make_quote_fn  # noqa: E402


class FakeSizingConfig(FakeConfig):
    RISK_PER_TRADE_PCT = 0.75
    INITIAL_SEED_KRW = 10_000_000


def _quote_fn(price: int, name: str = "삼성바이오로직스"):
    return make_quote_fn({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})


async def _default_get_positions():
    return {"success": True, "data": []}


async def _default_get_market_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


class TestBuySizingCap(unittest.IsolatedAsyncioTestCase):
    def _kwargs(self, *, price=70000, get_balance_fn=None, get_stock_positions_fn=None,
                get_market_warning_fn=None):
        universe = Universe(None)
        universe.replace_cache({"삼성바이오로직스": "207940"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def kis_order(symbol, price_, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        return dict(
            pool=FakePool(), redis=FakeRedis(), universe=universe, get_kis_token_fn=get_kis_token,
            config=FakeSizingConfig(), kis_order_fn=kis_order,
            # 사이징 한도 계산 자체는 이 파일의 관심사이므로, 매수 안전장치 관문(buy_gate,
            # [AT] buy-gate-unification)은 기본값(보유 없음·정상 종목)으로 통과시켜
            # 사이징 로직만 분리해서 검증한다.
            get_stock_positions_fn=get_stock_positions_fn or _default_get_positions,
            send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, get_balance_fn=get_balance_fn, get_recent_ohlcv_fn=None,
            get_market_warning_fn=get_market_warning_fn or _default_get_market_warning,
            get_quote_fn=_quote_fn(price),
        )

    async def test_qty_reduced_when_exceeding_cap(self):
        # equity 1000만 × 위험 0.75% ÷ 손절 7%(기본값) ≈ 107.1만원 기본금액.
        # ATR 조회 없음(get_recent_ohlcv_fn=None) → 보수적 폴백 배율 0.75배 → 한도 ≈80.36만원.
        # 40만원 × 3주 = 120만원이 한도를 넘으므로 floor(80.36만/40만) = 2주로 줄어야 한다.
        async def get_balance():
            return {"total": 10_000_000}

        kwargs = self._kwargs(price=400_000, get_balance_fn=get_balance)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 3주 매수", **kwargs)
        self.assertIn("요청 3주 → 사이징 한도로 2주로 조정", reply)
        self.assertIn("2주 매수 완료", reply)

    async def test_zero_qty_after_cap_rejects_order(self):
        # 한도(≈80.36만원)보다 비싼 100만원짜리 1주 → floor(80.36만/100만) = 0주 → 주문 거부.
        async def get_balance():
            return {"total": 10_000_000}

        kwargs = self._kwargs(price=1_000_000, get_balance_fn=get_balance)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 1주 매수", **kwargs)
        self.assertIn("사이징 한도", reply)
        self.assertIn("1주도 매수 불가", reply)
        self.assertIn("고가 종목", reply)

    async def test_within_cap_qty_unchanged(self):
        # 5만원 × 5주 = 25만원 ≪ 한도(≈80.36만원) → 조정 없이 그대로 체결.
        async def get_balance():
            return {"total": 10_000_000}

        kwargs = self._kwargs(price=50_000, get_balance_fn=get_balance)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매수", **kwargs)
        self.assertNotIn("사이징 한도로", reply)
        self.assertIn("5주 매수 완료", reply)

    async def test_bypass_keyword_skips_cap_and_marks_warning(self):
        # "한도무시"가 있으면 잔고 조회조차 하지 않고 원래 수량 그대로 주문해야 한다.
        async def get_balance():
            raise AssertionError("한도무시 키워드가 있으면 잔고 조회를 하면 안 된다")

        kwargs = self._kwargs(price=1_000_000, get_balance_fn=get_balance)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매수 한도무시", **kwargs)
        self.assertIn("5주 매수 완료", reply)
        self.assertIn("⚠️ 한도무시 적용", reply)

    async def test_missing_sizing_dependencies_does_not_block_buy(self):
        # get_balance_fn 미주입(예: 테스트/구버전 호출부) → 사이징을 건너뛰고 요청 수량 그대로 체결.
        kwargs = self._kwargs(price=1_000_000, get_balance_fn=None)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매수", **kwargs)
        self.assertIn("5주 매수 완료", reply)
        self.assertNotIn("사이징", reply)

    async def test_sizing_cap_exception_notifies_and_does_not_block_buy(self):
        # 한도 계산이 예외로 실패해도(get_balance_fn 미주입과 달리 운영 경로의 실제 실패)
        # 주문은 그대로 진행하되, 조용히 한도 없이 나가면 안 되므로 텔레그램 알림과
        # 응답 표시를 남겨야 한다([AT] fix/chat-sizing-cap-wiring).
        telegram_msgs = []

        async def send_telegram(text, **kw):
            telegram_msgs.append(text)

        async def get_balance():
            raise RuntimeError("DB 연결 끊김")

        kwargs = self._kwargs(price=50_000, get_balance_fn=get_balance)
        kwargs["send_telegram_fn"] = send_telegram
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매수", **kwargs)

        self.assertIn("5주 매수 완료", reply)
        self.assertIn("사이징 한도 계산 실패 — 한도 미적용", reply)
        self.assertTrue(any("사이징 한도 계산 실패" in m for m in telegram_msgs))

    async def test_sell_path_never_calls_sizing(self):
        # 매도는 보유수량만큼 파는 것이라 사이징과 무관해야 한다(요구사항: 매도 경로는 건드리지 않음).
        async def get_balance():
            raise AssertionError("매도 경로는 사이징을 적용하면 안 된다")

        async def get_stock_positions():
            return {"data": [{"symbol": "207940", "qty": 10, "avg_price": 90000}]}

        kwargs = self._kwargs(
            price=100_000, get_balance_fn=get_balance, get_stock_positions_fn=get_stock_positions)
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매도", **kwargs)
        self.assertIn("5주 매도 완료", reply)
        self.assertNotIn("사이징", reply)


if __name__ == "__main__":
    unittest.main()
