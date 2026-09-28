"""
stark/execution_guard.py 단위 테스트: 룰 기반 안전장치(precheck)와 실행 결과 후처리(execute).

핵심 검증: 당일 잔고없음 매도 억제, 당일 2회 손절 시 신규매수 차단, 안전장치 확인
자체가 실패하면 보수적으로 매수를 막는지, 그리고 execute()가 성공/실패 각각에서
매매기록·텔레그램·캐시무효화·매매일지를 빠짐없이 수행하는지.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import execution_guard  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeTradeHistoryConnection:
    def __init__(self, stop_loss_count=0, raise_on_fetchval=False):
        self.stop_loss_count = stop_loss_count
        self.raise_on_fetchval = raise_on_fetchval
        self.inserted = []
        self.decisions = []

    async def fetchval(self, query, *args):
        if self.raise_on_fetchval:
            raise RuntimeError("DB down")
        if "trade_history" in query:
            return self.stop_loss_count
        if "stark_decisions" in query:
            self.decisions.append(args)
            return len(self.decisions)
        return None

    async def fetchrow(self, query, *args):
        if "strategy_config" in query:
            return {"params": {"max_positions": 5}}
        return None

    async def execute(self, query, *args):
        if "trade_history" in query:
            self.inserted.append(args)
            return "INSERT 1"
        return "OK"


class FakePool:
    def __init__(self, stop_loss_count=0, raise_on_fetchval=False):
        self._conn = FakeTradeHistoryConnection(stop_loss_count, raise_on_fetchval)

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}
        self.deleted = []

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    async def keys(self, pattern="*"):
        import fnmatch
        return [k for k in self.store.keys() if fnmatch.fnmatch(k, pattern)]


class TestPrecheck(unittest.IsolatedAsyncioTestCase):
    async def test_passes_when_nothing_blocks(self):
        pool = FakePool(stop_loss_count=0)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_sell_fail_suppress_blocks_sell(self):
        redis = FakeRedis({"sell_fail_suppress:005930": "1"})
        result = await execution_guard.precheck("005930", "sell", "stock_trader", pool=None, redis=redis)
        self.assertEqual(result, {"blocked": "sell_fail_suppress"})

    async def test_suppress_key_does_not_affect_buy(self):
        redis = FakeRedis({"sell_fail_suppress:005930": "1"})
        pool = FakePool(stop_loss_count=0)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=redis)
        self.assertIsNone(result)

    async def test_two_stop_losses_blocks_new_buy(self):
        pool = FakePool(stop_loss_count=2)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertEqual(result, {"blocked": "daily_stop_loss_limit"})

    async def test_one_stop_loss_does_not_block(self):
        pool = FakePool(stop_loss_count=1)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_stop_loss_check_only_applies_to_stock_trader(self):
        pool = FakePool(stop_loss_count=5)
        result = await execution_guard.precheck("BTC", "buy", "crypto_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_guard_check_failure_blocks_conservatively(self):
        pool = FakePool(raise_on_fetchval=True)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIn("error", result)


def make_signal(**overrides):
    base = {"symbol": "005930", "name": "삼성전자", "bot": "stock_trader", "action": "buy",
            "price": 70000, "qty": 1, "strategy": "MA크로스", "reason": "골든크로스"}
    base.update(overrides)
    return base


def make_decision(**overrides):
    base = {"is_small": False, "reply": "EXECUTE: 강한 확신"}
    base.update(overrides)
    return base


class TestExecute(unittest.IsolatedAsyncioTestCase):
    async def test_success_records_trade_and_notifies(self):
        pool = FakePool()
        redis = FakeRedis({"cache:positions:stock": "1", "cache:account:stock": "1"})
        sent, journaled, memory_saved = [], [], []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def save_trade_memory(**kwargs):
            memory_saved.append(kwargs)

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=save_trade_memory,
            code_to_name_fn=code_to_name,
        )
        self.assertEqual(result, {"success": True, "executed": True, "jarvis_reply": "EXECUTE: 강한 확신"})
        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(journaled), 1)
        self.assertEqual(len(memory_saved), 1)
        self.assertEqual(redis.deleted, ["cache:positions:stock", "cache:account:stock"])

    async def test_no_balance_failure_sets_suppress_key_once(self):
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "매도가능 수량 부족"}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["success"])
        self.assertIn("sell_fail_suppress:005930", redis.store)
        self.assertEqual(len(sent), 1)

        # 두 번째 실패는 이미 억제 키가 있으므로 재알림하지 않는다
        result2 = await execution_guard.execute(
            make_signal(action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result2["success"])
        self.assertEqual(len(sent), 1)  # 여전히 1건 — 재알림 안 됨

    async def test_buy_failure_sets_buy_suppress_key_once(self):
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "지정가 주문 거부"}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["success"])
        self.assertIn("buy_fail_suppress:005930", redis.store)
        self.assertEqual(len(sent), 1)

        # 재시도 시 이미 억제 키가 있으므로 중복 알림 방지
        result2 = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result2["success"])
        self.assertEqual(len(sent), 1)

    async def test_buy_fail_suppress_blocks_precheck(self):
        redis = FakeRedis({"buy_fail_suppress:005930": "1"})
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=None, redis=redis)
        self.assertEqual(result, {"blocked": "buy_fail_suppress"})

    async def test_sell_other_failure_sets_sell_suppress_key(self):
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "모의투자 주문이 불가한 계좌입니다."}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "일동제약"

        result = await execution_guard.execute(
            make_signal(symbol="249420", action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["success"])
        self.assertIn("sell_fail_suppress:249420", redis.store)
        self.assertEqual(len(sent), 1)

    async def test_max_positions_limit_skips_and_logs_without_telegram(self):
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, journaled, orders = [], [], []

        # 현재 5종목 보유 중
        current_positions = [{"symbol": f"0000{i}", "name": f"종목{i}", "qty": 10} for i in range(1, 6)]

        async def get_positions():
            return {"success": True, "data": current_positions}

        async def kis_order(symbol, price, qty, is_buy):
            orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return "신규종목"

        # 6번째 신규 종목 매수 신호
        signal = make_signal(symbol="000099", name="신규종목", action="buy")
        decision = make_decision(reply="EXECUTE: 매수 추천")

        result = await execution_guard.execute(
            signal, decision, pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
            get_positions_fn=get_positions,
            max_positions=5,
        )

        self.assertTrue(result["success"])
        self.assertFalse(result["executed"])
        self.assertTrue(result.get("skipped"))
        self.assertEqual(result.get("blocked"), "max_positions_limit")
        self.assertIn("한도 초과", result.get("reason", ""))

        # 주문 및 텔레그램 발송 0건 확인 (알림 폭탄 방지)
        self.assertEqual(len(orders), 0)
        self.assertEqual(len(sent), 0)

        # stark_decisions에 SKIP과 사유 기록 확인
        self.assertEqual(len(pool._conn.decisions), 1)
        decision_record = pool._conn.decisions[0]
        # (symbol, name, decision, confidence, reason, rationale, ...)
        self.assertEqual(decision_record[0], "000099")  # symbol
        self.assertEqual(decision_record[2], "SKIP")    # decision
        self.assertIn("한도 초과", decision_record[4])  # reason

        # 매매일지에 SKIP 기록 확인
        self.assertEqual(len(journaled), 1)
        self.assertEqual(journaled[0][6], "SKIP")

    async def test_already_held_symbol_allowed_even_at_limit(self):
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, orders = [], []

        # 현재 5종목 보유 중 (그 중 005930 포함)
        current_positions = [{"symbol": f"0000{i}", "name": f"종목{i}", "qty": 10} for i in range(1, 5)]
        current_positions.append({"symbol": "005930", "name": "삼성전자", "qty": 5})

        async def get_positions():
            return {"success": True, "data": current_positions}

        async def kis_order(symbol, price, qty, is_buy):
            orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def save_trade_memory(**kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        # 이미 보유 중인 005930 추가 매수
        signal = make_signal(symbol="005930", name="삼성전자", action="buy")
        decision = make_decision()

        result = await execution_guard.execute(
            signal, decision, pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=save_trade_memory,
            code_to_name_fn=code_to_name,
            get_positions_fn=get_positions,
            max_positions=5,
        )

        # 기존 보유 종목 추가 매수는 허용되어 주문 체결 및 알림 발송
        self.assertTrue(result["success"])
        self.assertTrue(result["executed"])
        self.assertEqual(len(orders), 1)
        self.assertEqual(len(sent), 1)

    async def test_concurrent_10_buy_signals_limited_to_max_positions(self):
        """동시 신호 10건이 들어와도 한도(5)를 넘어 주문되지 않아야 한다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, orders = [], []

        # 공유 잔고 상태 (초기 0개 종목 보유)
        positions = []

        async def get_positions():
            # 실제 KIS 잔고 호출처럼 약간의 비동기 지연 모사
            await asyncio.sleep(0.005)
            return {"success": True, "data": list(positions)}

        async def kis_order(symbol, price, qty, is_buy):
            await asyncio.sleep(0.005)
            orders.append((symbol, price, qty, is_buy))
            # 주문 성공 시 체결되어 보유 목록에 추가됨
            positions.append({"symbol": symbol, "name": f"종목_{symbol}", "qty": qty})
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def save_trade_memory(**kwargs):
            pass

        async def code_to_name(symbol):
            return f"종목_{symbol}"

        # 10개의 서로 다른 종목 매수 신호 동시 생성
        async def run_one(i):
            sym = f"{100000 + i:06d}"
            sig = make_signal(symbol=sym, name=f"종목_{sym}", action="buy")
            dec = make_decision()
            return await execution_guard.execute(
                sig, dec, pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=save_trade_memory,
                code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
                max_positions=5,
            )

        # 10건 동시 실행
        results = await asyncio.gather(*(run_one(i) for i in range(10)))

        # 10건 중 성공 체결은 정확히 5건, SKIP은 5건이어야 함
        executed_results = [r for r in results if r.get("executed")]
        skipped_results = [r for r in results if r.get("skipped")]

        self.assertEqual(len(executed_results), 5)
        self.assertEqual(len(skipped_results), 5)
        self.assertEqual(len(orders), 5)
        self.assertEqual(len(sent), 5)
        self.assertEqual(len(positions), 5)

        # SKIP된 5건 모두 stark_decisions에 기록되었는지 확인
        self.assertEqual(len(pool._conn.decisions), 5)
        for d in pool._conn.decisions:
            self.assertEqual(d[2], "SKIP")
            self.assertIn("한도 초과", d[4])

    async def test_concurrent_10_buy_signals_delayed_balance_reflection_under_limit(self):
        """상황 1: 체결 반영이 조회 3번 뒤에 지연되어도 in-flight 추적으로 주문 5건 이하(최대 5건) 유지."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, orders = [], []
        confirmed_positions = []
        pending_queue = []

        async def get_positions():
            await asyncio.sleep(0.005)
            # 주문 후 3회 호출 뒤에야 confirmed_positions로 이전됨 (지연 반영)
            if pending_queue:
                for item in pending_queue:
                    item["delay"] -= 1
                    if item["delay"] <= 0 and item["pos"] not in confirmed_positions:
                        confirmed_positions.append(item["pos"])
            return {"success": True, "data": list(confirmed_positions)}

        async def kis_order(symbol, price, qty, is_buy):
            await asyncio.sleep(0.005)
            orders.append((symbol, price, qty, is_buy))
            # 3회 조회 지연 후 반영되도록 큐에 삽입
            pending_queue.append({"delay": 3, "pos": {"symbol": symbol, "name": f"종목_{symbol}", "qty": qty}})
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return f"종목_{symbol}"

        async def run_one(i):
            sym = f"{200000 + i:06d}"
            sig = make_signal(symbol=sym, name=f"종목_{sym}", action="buy")
            dec = make_decision()
            return await execution_guard.execute(
                sig, dec, pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
                max_positions=5,
            )

        results = await asyncio.gather(*(run_one(i) for i in range(10)))

        # 기대값: 주문 5건 이하 (정확히 5건)
        self.assertLessEqual(len(orders), 5)
        self.assertEqual(len(orders), 5)
        self.assertEqual(len(sent), 5)
        executed_count = len([r for r in results if r.get("executed")])
        self.assertEqual(executed_count, 5)

    async def test_concurrent_10_buy_signals_fail_closed_on_check_failure_response(self):
        """상황 2: 보유 조회가 실패(success False 또는 stale)하면 주문 0건 (fail-closed)."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, orders, journaled = [], [], []

        async def get_positions():
            await asyncio.sleep(0.005)
            return {"success": False, "error": "KIS API 500", "data": []}

        async def kis_order(symbol, price, qty, is_buy):
            orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return f"종목_{symbol}"

        async def run_one(i):
            sym = f"{300000 + i:06d}"
            sig = make_signal(symbol=sym, name=f"종목_{sym}", action="buy")
            dec = make_decision()
            return await execution_guard.execute(
                sig, dec, pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
                max_positions=5,
            )

        results = await asyncio.gather(*(run_one(i) for i in range(10)))

        # 기대값: 주문 0건, 텔레그램 알림 0건
        self.assertEqual(len(orders), 0)
        self.assertEqual(len(sent), 0)

        # 10건 모두 SKIP 처리 및 stark_decisions에 "보유 종목수 확인 불가" 기록
        self.assertEqual(len(results), 10)
        for r in results:
            self.assertTrue(r.get("skipped"))
            self.assertFalse(r.get("executed"))
            self.assertEqual(r.get("reason"), "보유 종목수 확인 불가")

        self.assertEqual(len(pool._conn.decisions), 10)
        for d in pool._conn.decisions:
            self.assertEqual(d[2], "SKIP")
            self.assertEqual(d[4], "보유 종목수 확인 불가")

    async def test_concurrent_10_buy_signals_fail_closed_on_check_exception(self):
        """상황 3: 보유 조회가 예외를 던지면 주문 0건 (fail-closed)."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, orders, journaled = [], [], []

        async def get_positions():
            await asyncio.sleep(0.005)
            raise RuntimeError("KIS network timeout")

        async def kis_order(symbol, price, qty, is_buy):
            orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return f"종목_{symbol}"

        async def run_one(i):
            sym = f"{400000 + i:06d}"
            sig = make_signal(symbol=sym, name=f"종목_{sym}", action="buy")
            dec = make_decision()
            return await execution_guard.execute(
                sig, dec, pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
                max_positions=5,
            )

        results = await asyncio.gather(*(run_one(i) for i in range(10)))

        # 기대값: 주문 0건, 텔레그램 알림 0건
        self.assertEqual(len(orders), 0)
        self.assertEqual(len(sent), 0)

        # 10건 모두 SKIP 처리 및 stark_decisions에 "보유 종목수 확인 불가" 기록
        self.assertEqual(len(results), 10)
        for r in results:
            self.assertTrue(r.get("skipped"))
            self.assertFalse(r.get("executed"))
            self.assertEqual(r.get("reason"), "보유 종목수 확인 불가")

        self.assertEqual(len(pool._conn.decisions), 10)
        for d in pool._conn.decisions:
            self.assertEqual(d[2], "SKIP")
            self.assertEqual(d[4], "보유 종목수 확인 불가")


if __name__ == "__main__":
    unittest.main()
