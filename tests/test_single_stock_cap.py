"""
종목당 비중 상한 테스트 ([AT] feat/single-stock-cap, PM 지시).

배경: 207940(삼성바이오로직스)을 채팅 수동 매수로 4주(총자산의 약 59%) 보유하다
-7.1%에 손절해 -404,000원 손실. 수동 매수 경로는 사이징 가드를 모두 우회하고
종목당 비중 상한이 없었다.

매수 후 예상 비중 = (해당 종목 현재 평가금액 + 이번 매수금액) ÷ 총자산(KIS 계좌 총평가).
상한(기본 20%, SINGLE_STOCK_MAX_PCT) 초과 시:
  - AI 자동매수(stark.execution_guard.execute()): 차단(SKIP, stark_decisions/trade_journal
    기록). 총자산 조회 실패 시에는 차단하지 않고 기존 동작 유지(경고 로그만).
  - 수동 채팅 매수(order_handler.handle_trade_command)·제안 승인 매수
    (handle_proposal_response): 차단 + "'강제매수' 포함해서 다시 지시" 안내. 명령에
    '강제매수'가 있으면 경고 문구와 함께 통과. 총자산 조회 실패 시에는 경고만 붙이고 통과.
  - 매도·코인 경로는 영향받지 않는다.

revert 확인(수동): stark/execution_guard.py execute()의 "(1a) 종목당 비중 상한" 블록과
router/handlers/order_handler.py의 두 '강제매수' 검사 블록을 제거하면 아래 차단 테스트들
(test_blocks_automatic_buy_over_cap, test_chat_buy_blocked_over_cap,
test_proposal_approval_blocked_over_cap)이 모두 실패(차단되지 않고 통과)하는 것을 확인했다
— 이 기능이 실제로 차단을 담당하고 있음을 보증.
"""
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.config import config  # noqa: E402
from common.position_sizing import compute_projected_weight_pct  # noqa: E402
from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import (  # noqa: E402
    FakePool as EgFakePool,
    FakeRedis as EgFakeRedis,
    make_decision,
    make_signal,
)
from tests.test_order_handler import (  # noqa: E402
    FakeConfig,
    FakePool as OhFakePool,
    FakeRedis as OhFakeRedis,
    make_quote_fn,
)


def _positions_fn(total_eval, holdings=None):
    """get_positions_fn 더미: account.total_eval + 종목별 보유(symbol -> (qty, cur_price))."""
    data = []
    for symbol, (qty, cur_price) in (holdings or {}).items():
        data.append({"symbol": symbol, "qty": qty, "cur_price": cur_price})

    async def _fn():
        return {"success": True, "data": data, "account": {"total_eval": total_eval}}
    return _fn


def _failed_positions_fn(stale=False, raise_exc=False):
    async def _fn():
        if raise_exc:
            raise RuntimeError("KIS 조회 실패")
        if stale:
            return {"success": True, "data": [], "account": {"total_eval": 10_000_000}, "stale": True}
        return {"success": False, "error": "KIS 조회 실패", "data": []}
    return _fn


def _zero_equity_positions_fn():
    """보유 종목수 조회(check_buy_position_limits)는 정상 통과하지만 총자산(total_eval)을
    확인할 수 없는 상황 — 비중 상한 쪽만 "조회 실패"로 isolate하기 위한 더미(같은
    get_positions_fn을 공유하는 다른 안전장치가 success=False/stale에 먼저 걸려서
    fail-closed로 차단해버리면 비중 상한 경고 통과 동작을 isolate해서 검증할 수 없다)."""
    async def _fn():
        return {"success": True, "data": [], "account": {"total_eval": 0}}
    return _fn


# ── 1) 공용 계산 로직 — common.position_sizing.compute_projected_weight_pct ──
class TestComputeProjectedWeightPct(unittest.TestCase):
    def test_basic_projection(self):
        # 보유 0원 + 매수 210만원, 총자산 1천만원 -> 21%
        pct = compute_projected_weight_pct(0, 2_100_000, 10_000_000)
        self.assertAlmostEqual(pct, 21.0)

    def test_existing_holding_is_included(self):
        # 기존 보유 190만원 + 매수 14만원, 총자산 1천만원 -> 20.4%
        pct = compute_projected_weight_pct(1_900_000, 140_000, 10_000_000)
        self.assertAlmostEqual(pct, 20.4)

    def test_equity_zero_returns_none(self):
        self.assertIsNone(compute_projected_weight_pct(0, 100, 0))

    def test_equity_negative_returns_none(self):
        self.assertIsNone(compute_projected_weight_pct(0, 100, -1))


# ── 2) stark.execution_guard.check_single_stock_cap() 단위 테스트 ──
class TestCheckSingleStockCap(unittest.IsolatedAsyncioTestCase):
    async def test_blocks_when_over_cap(self):
        get_positions = _positions_fn(10_000_000)
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 30, get_positions_fn=get_positions,  # 210만원 = 21% > 20%
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["blocked"], "single_stock_cap")
        self.assertIn("single_stock_cap", result["reason"])
        self.assertIn("21.0%", result["reason"])
        self.assertIn("20.0%", result["reason"])

    async def test_passes_when_within_cap(self):
        get_positions = _positions_fn(10_000_000)
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 1, get_positions_fn=get_positions,  # 7만원 = 0.7%
        )
        self.assertIsNone(result)

    async def test_includes_existing_holding_value(self):
        get_positions = _positions_fn(10_000_000, holdings={"005930": (20, 100000)})  # 보유 200만원
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 1, get_positions_fn=get_positions,  # +7만원 = 20.7%
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["blocked"], "single_stock_cap")

    async def test_fetch_failure_does_not_block(self):
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 100, get_positions_fn=_failed_positions_fn(),
        )
        self.assertIsNone(result)

    async def test_stale_data_does_not_block(self):
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 100, get_positions_fn=_failed_positions_fn(stale=True),
        )
        self.assertIsNone(result)

    async def test_exception_does_not_block(self):
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 100, get_positions_fn=_failed_positions_fn(raise_exc=True),
        )
        self.assertIsNone(result)

    async def test_no_positions_fn_does_not_block(self):
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 100, get_positions_fn=None,
        )
        self.assertIsNone(result)

    async def test_custom_max_pct_override(self):
        get_positions = _positions_fn(10_000_000)
        result = await execution_guard.check_single_stock_cap(
            "005930", 70000, 1, get_positions_fn=get_positions, max_pct=0.5,  # 0.7% > 0.5%
        )
        self.assertIsNotNone(result)

    async def test_config_default_is_honored(self):
        original = config.SINGLE_STOCK_MAX_PCT
        try:
            config.SINGLE_STOCK_MAX_PCT = 50.0
            get_positions = _positions_fn(10_000_000)
            result = await execution_guard.check_single_stock_cap(
                "005930", 70000, 30, get_positions_fn=get_positions,  # 21% < 50%(설정값)
            )
            self.assertIsNone(result)
        finally:
            config.SINGLE_STOCK_MAX_PCT = original


# ── 3) execute() 통합 테스트 (AI 자동매수 경로) ──
class TestExecuteSingleStockCap(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    async def test_reduces_automatic_buy_over_cap(self):
        """[fix/sizing-exceeds-single-stock-cap] 상한 초과 시 차단하지 않고 수량을 줄여 주문한다.
        7만원 x 30주 = 21% -> 총자산 1천만원의 20% = 200만원 -> 28주."""
        pool = EgFakePool()
        redis = EgFakeRedis()
        ordered = []

        async def kis_order(symbol, price, qty, is_buy):
            ordered.append(qty)
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(price=70000, qty=30), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
            get_positions_fn=_positions_fn(10_000_000),
        )
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [28])
        # 줄였다는 사실이 stark_decisions에 SIZE_REDUCED로 남는다(원래/줄인 수량 포함)
        self.assertEqual(len(pool._conn.decisions), 1)
        logged = pool._conn.decisions[0]
        self.assertIn("SIZE_REDUCED", logged)
        self.assertTrue(any(isinstance(a, str) and "30주 -> 28주" in a for a in logged))

    async def test_passes_automatic_buy_within_cap(self):
        pool = EgFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(price=70000, qty=1), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
            get_positions_fn=_positions_fn(10_000_000),  # 7만원 = 0.7%
        )
        self.assertTrue(result["executed"])

    async def test_total_asset_fetch_failure_keeps_existing_behavior(self):
        """총자산을 확인할 수 없으면 AI 자동매수는 차단하지 않는다(기존 동작 유지, PM 지시)."""
        pool = EgFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(price=70000, qty=30), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
            get_positions_fn=_zero_equity_positions_fn(),
        )
        self.assertTrue(result["executed"])

    async def test_no_positions_fn_keeps_existing_behavior(self):
        """get_positions_fn 자체가 없으면(구버전 호출 등) 상한 검사를 건너뛴다."""
        pool = EgFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(price=70000, qty=30), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertTrue(result["executed"])

    async def test_sell_action_is_not_affected(self):
        """매도 경로는 종목당 비중 상한과 무관하다."""
        pool = EgFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(action="sell", price=70000, qty=30), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
            get_positions_fn=_positions_fn(10_000_000),
        )
        self.assertTrue(result["executed"])


# ── 4) handle_trade_command() 통합 테스트 (수동 채팅 매수) ──
class FakeCapConfig(FakeConfig):
    SINGLE_STOCK_MAX_PCT = 20.0


def _quote_fn(price: int = 70000, name: str = "삼성전자"):
    return make_quote_fn({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})


async def _ok_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


class TestHandleTradeCommandSingleStockCap(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    def _kwargs(self, *, pool, get_stock_positions_fn, trade_config=None):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        self.orders = []

        async def kis_order(symbol, price, qty, is_buy):
            self.orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        return dict(
            pool=pool, redis=OhFakeRedis(), universe=universe,
            get_kis_token_fn=get_kis_token, config=trade_config or FakeCapConfig(),
            kis_order_fn=kis_order,
            get_stock_positions_fn=get_stock_positions_fn, get_market_warning_fn=_ok_warning,
            get_quote_fn=_quote_fn(),
        )

    async def _run(self, msg, *, pool, get_stock_positions_fn, trade_config=None):
        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        kwargs = self._kwargs(pool=pool, get_stock_positions_fn=get_stock_positions_fn,
                               trade_config=trade_config)
        kwargs["send_telegram_fn"] = send_telegram
        kwargs["log_journal_fn"] = log_journal
        return await order_handler.handle_trade_command(msg, **kwargs)

    async def test_blocked_over_cap_with_guidance_message(self):
        pool = OhFakePool()
        # 30주 @ 7만원 = 210만원, 총자산 1천만원 -> 21% > 20%(FakeConfig 기본값)
        reply = await self._run(
            "삼성전자 30주 매수", pool=pool, get_stock_positions_fn=_positions_fn(10_000_000))
        self.assertIn("예상 비중", reply)
        self.assertIn("강제매수", reply)
        self.assertEqual(len(self.orders), 0)

    async def test_force_keyword_overrides_block_and_warns(self):
        pool = OhFakePool()
        reply = await self._run(
            "삼성전자 30주 매수 강제매수", pool=pool, get_stock_positions_fn=_positions_fn(10_000_000))
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)
        self.assertIn("강제매수", reply)

    async def test_within_cap_passes_without_warning(self):
        pool = OhFakePool()
        reply = await self._run(
            "삼성전자 1주 매수", pool=pool, get_stock_positions_fn=_positions_fn(10_000_000))
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)
        self.assertNotIn("비중", reply)

    async def test_total_asset_fetch_failure_warns_but_passes(self):
        pool = OhFakePool()
        reply = await self._run(
            "삼성전자 30주 매수", pool=pool, get_stock_positions_fn=_zero_equity_positions_fn())
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)
        self.assertIn("확인 실패", reply)

    async def test_sell_is_unaffected(self):
        pool = OhFakePool()

        async def positions():
            return {"success": True, "data": [{"symbol": "005930", "qty": 5, "avg_price": 60000}],
                    "account": {"total_eval": 10_000_000}}

        reply = await self._run("삼성전자 2주 매도", pool=pool, get_stock_positions_fn=positions)
        self.assertNotIn("비중", reply)
        self.assertEqual(len(self.orders), 1)

    async def test_disabled_when_config_has_no_attribute(self):
        """FakeConfig에 SINGLE_STOCK_MAX_PCT가 없으면(getattr 기본값 None) 검사를
        건너뛴다 — 기존(이 기능 추가 전) 테스트들과의 하위호환."""
        self.assertFalse(hasattr(FakeConfig(), "SINGLE_STOCK_MAX_PCT"))
        pool = OhFakePool()
        reply = await self._run(
            "삼성전자 30주 매수", pool=pool, get_stock_positions_fn=_positions_fn(10_000_000),
            trade_config=FakeConfig())
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)


# ── 5) handle_proposal_response() 통합 테스트 (제안 승인 매수) ──
class TestHandleProposalResponseSingleStockCap(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    def _redis(self, qty=30, price=70000):
        return OhFakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps(
                {"symbol": "005930", "name": "삼성전자", "price": price, "qty": qty}),
        })

    async def test_blocked_over_cap_keeps_proposal_for_retry(self):
        pool = OhFakePool()
        redis = self._redis()

        async def kis_order(symbol, price, qty, is_buy):
            raise AssertionError("상한 차단 중 주문이 나가면 안 된다")

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=_positions_fn(10_000_000), pool=pool,
        )
        self.assertIn("예상 비중", reply)
        self.assertIn("강제매수", reply)
        # 위치한도 차단과 달리 proposal을 지우지 않아 "강제매수"로 재승인할 수 있어야 한다
        self.assertIn("proposal:005930", redis.store)

    async def test_force_keyword_overrides_block_and_executes(self):
        pool = OhFakePool()
        redis = self._redis()
        orders = []

        async def kis_order(symbol, price, qty, is_buy):
            orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        reply = await order_handler.handle_proposal_response(
            "사자 강제매수", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=_positions_fn(10_000_000), pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertIn("강제매수", reply)
        self.assertEqual(len(orders), 1)

    async def test_within_cap_passes_without_warning(self):
        pool = OhFakePool()
        redis = self._redis(qty=1)

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=_positions_fn(10_000_000), pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertNotIn("비중", reply)

    async def test_total_asset_fetch_failure_warns_but_passes(self):
        pool = OhFakePool()
        redis = self._redis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=_zero_equity_positions_fn(), pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertIn("확인 실패", reply)


if __name__ == "__main__":
    unittest.main()


# ── 4) 10/29 실제 차단 7건 재현 + 수량 축소 ([AT] fix/sizing-exceeds-single-stock-cap) ──
# 총자산 9,182,742원(balance_snapshot 10/29), 보유 0 기준. 한도 20% = 1,836,548원.
class TestSizingReducedToCapReal1029(unittest.IsolatedAsyncioTestCase):
    EQUITY = 9_182_742

    async def _run(self, symbol, name, price, qty, holdings=None):
        execution_guard.reset_buy_lock()
        # 보유 종목은 물타기 정책 통과를 위해 최초 매수 이력 1건(현재가와 동일)을 둔다.
        pool = EgFakePool(buy_history={s: [{"price": price}] for s in (holdings or {})})
        redis = EgFakeRedis()
        ordered, journaled = [], []

        async def kis_order(sym, p, q, is_buy):
            ordered.append(q)
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(sym):
            return name

        result = await execution_guard.execute(
            make_signal(symbol=symbol, name=name, price=price, qty=qty), make_decision(),
            pool=pool, redis=redis, kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None, code_to_name_fn=code_to_name,
            get_positions_fn=_positions_fn(self.EQUITY, holdings=holdings),
        )
        return result, ordered, pool, journaled

    async def test_namuga_187_shares_reduced_to_99(self):
        # 18,400 x 187 = 3,440,800 (37.5%) -> 1,836,548 / 18,400 = 99주
        result, ordered, pool, journaled = await self._run("190510", "나무가", 18400, 187)
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [99])
        self.assertLessEqual(99 * 18400 / self.EQUITY * 100, config.SINGLE_STOCK_MAX_PCT)
        self.assertIn("187주 -> 99주", " ".join(str(a) for a in pool._conn.decisions[0]))

    async def test_sk_hynix_2_shares_reduced_to_1(self):
        # 1,543,000 x 2 = 3,086,000 (33.6%) -> 1,836,548 / 1,543,000 = 1주
        result, ordered, pool, journaled = await self._run("000660", "SK하이닉스", 1543000, 2)
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [1])

    async def test_hyundai_12_shares_reduced_to_6(self):
        # 300,500 x 12 = 3,606,000 (39.3%) -> 1,836,548 / 300,500 = 6주
        result, ordered, pool, journaled = await self._run("005380", "현대차", 300500, 12)
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [6])

    async def test_below_one_share_after_reduction_is_blocked(self):
        # 1주(1,549,000)는 들어가지만 이미 1,500,000원 보유 중이면 여유 336,548원 -> 0주 -> 차단
        result, ordered, pool, journaled = await self._run(
            "000660", "SK하이닉스", 1549000, 2, holdings={"000660": (1, 1_500_000)})
        self.assertFalse(result["executed"])
        self.assertEqual(result["blocked"], "size_below_one_share")
        self.assertEqual(ordered, [])
        self.assertEqual(len(journaled), 1)
        self.assertEqual(journaled[0][6], "SKIP")

    async def test_existing_holding_is_counted_when_reducing(self):
        # 나무가 보유 50주(18,400 = 920,000원) -> 여유 916,548원 -> 49주
        result, ordered, pool, journaled = await self._run(
            "190510", "나무가", 18400, 187, holdings={"190510": (50, 18400)})
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [49])

    async def test_within_cap_keeps_original_qty_and_no_reduction_log(self):
        result, ordered, pool, journaled = await self._run("190510", "나무가", 18400, 90)
        self.assertTrue(result["executed"])
        self.assertEqual(ordered, [90])
        self.assertEqual(len(pool._conn.decisions), 0)

    async def test_limit_value_unchanged(self):
        self.assertEqual(config.SINGLE_STOCK_MAX_PCT, 20.0)


class TestMaxQtyWithinCap(unittest.TestCase):
    def test_basic(self):
        from common.position_sizing import max_qty_within_cap
        self.assertEqual(max_qty_within_cap(0, 18400, 9_182_742, 20), 99)

    def test_no_room_returns_zero(self):
        from common.position_sizing import max_qty_within_cap
        self.assertEqual(max_qty_within_cap(2_000_000, 1000, 9_182_742, 20), 0)

    def test_invalid_inputs_return_none(self):
        from common.position_sizing import max_qty_within_cap
        self.assertIsNone(max_qty_within_cap(0, 0, 100, 20))
        self.assertIsNone(max_qty_within_cap(0, 100, 0, 20))
