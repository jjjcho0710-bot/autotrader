"""
tests/test_buy_lock_chat_path.py — [AT] fix/buy-lock-chat-path 전용 테스트.

신호 매수(stark.execution_guard.execute)와 채팅 직접매수·제안 승인 매수
(router.handlers.order_handler.handle_trade_command / handle_proposal_response)가
동시에 들어와도 get_buy_lock()으로 직렬화되어 보유 종목수 한도(max_positions)를
넘지 않는지, 매도는 이 락과 무관하게 즉시 진행되는지, 락 획득이 오래 걸리면
무한정 기다리지 않고 타임아웃으로 빠져나오는지를 검증한다.
"""
import asyncio
import json
import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# order_handler.py는 handle_trade_command 안에서 필요할 때만 aiohttp를 지연 임포트한다
# (test_order_handler.py와 동일한 이유로 더미 모듈을 등록해둔다).
try:
    import aiohttp  # noqa: F401
except ImportError:
    _stub = types.ModuleType("aiohttp")
    _stub.ClientSession = object
    _stub.TCPConnector = lambda *a, **kw: None
    _stub.ClientTimeout = lambda *a, **kw: None
    sys.modules["aiohttp"] = _stub

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from stark import execution_guard  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeTradeConnection:
    """공시 없음·당일 손절 0회·max_positions 조회는 기본값(5)으로 응답하는 최소
    커넥션 — buy_gate()의 precheck 경로가 거치는 쿼리를 모두 무해하게 통과시킨다."""

    def __init__(self):
        self.inserted = []

    async def fetch(self, query, *args):
        return []

    async def fetchval(self, query, *args):
        return 0

    async def fetchrow(self, query, *args):
        return None

    async def execute(self, query, *args):
        self.inserted.append(args)
        return "INSERT 1"


class FakePool:
    def __init__(self):
        self._conn = FakeTradeConnection()

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)

    async def keys(self, pattern="*"):
        import fnmatch
        return [k for k in self.store if fnmatch.fnmatch(k, pattern)]


def make_signal(symbol, price=70000, qty=1):
    return {"symbol": symbol, "name": f"종목_{symbol}", "bot": "stock_trader", "action": "buy",
            "price": price, "qty": qty, "strategy": "테스트", "reason": "테스트"}


def make_decision():
    return {"is_small": False, "reply": "EXECUTE: 테스트"}


async def ok_market_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


async def noop(*args, **kwargs):
    pass


class TestConcurrentSignalAndChatBuy(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # get_buy_lock()은 모듈 전역 싱글톤이라 이벤트루프에 바인딩된다 — 테스트마다 새
        # 이벤트루프를 쓰는 IsolatedAsyncioTestCase에서는 매 테스트 시작 시 리셋해야 한다.
        execution_guard.reset_buy_lock()

    async def test_concurrent_signal_and_chat_buy_respects_max_positions(self):
        """신호 매수 3건 + 채팅 매수 6건이 동시에 들어와도(보유 0종목, 한도 5) 정확히
        5건만 체결되고 4건은 한도 초과로 막혀야 한다. get_buy_lock()을 공유하지 않으면
        두 경로 모두 "현재 5개 미만"으로 보고 둘 다 통과해 한도를 넘길 수 있었다
        (이 테스트는 router/handlers/order_handler.handle_trade_command의 락 래핑을
        되돌리면 6건이 모두 체결되어 실패한다)."""
        pool = FakePool()
        redis = FakeRedis()
        positions = []
        orders = []

        async def get_positions():
            await asyncio.sleep(0.005)
            return {"success": True, "data": list(positions)}

        async def kis_order(symbol, price, qty, is_buy):
            await asyncio.sleep(0.005)
            orders.append(symbol)
            if is_buy:
                positions.append({"symbol": symbol, "qty": qty, "avg_price": price})
            return {"success": True}

        async def get_quote(symbol):
            return {"stck_prpr": 70000}

        universe = Universe(None)

        async def run_signal(i):
            sym = f"{300000 + i:06d}"
            return await execution_guard.execute(
                make_signal(sym), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=noop,
                log_journal_fn=noop, save_trade_memory_fn=noop,
                code_to_name_fn=lambda s: s, get_positions_fn=get_positions,
                max_positions=5,
            )

        async def run_chat(i):
            sym = f"{400000 + i:06d}"
            return await order_handler.handle_trade_command(
                f"{sym} 1주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=None, config=object(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_positions, send_telegram_fn=noop,
                log_journal_fn=noop, get_market_warning_fn=ok_market_warning,
                get_quote_fn=get_quote,
            )

        # 신호 3건 + 채팅 6건 = 9건. 채팅 쪽만 6건이라 — 채팅 경로끼리도 서로 락을
        # 공유하지 않으면 그 6건만으로 이미 한도(5)를 넘긴다. 신호 경로는 원래부터
        # execute() 자체 락으로 신호끼리는 직렬화돼 있었으므로, 신호 쪽 숫자를 작게 둬도
        # "신호 1건 + 채팅 6건이 동시에 0개 보유로 읽는" 1차 물량만으로 7건이 몰려
        # 우연히 정확히 5건에서 끝나는 타이밍 우연을 피할 수 있다.
        results = await asyncio.gather(
            run_signal(0), run_signal(1), run_signal(2),
            run_chat(0), run_chat(1), run_chat(2), run_chat(3), run_chat(4), run_chat(5),
        )

        self.assertEqual(len(positions), 5, f"한도(5)를 넘겨 체결됨: {positions}")
        self.assertEqual(len(orders), 5)
        blocked_count = sum(
            1 for r in results
            if (isinstance(r, dict) and r.get("skipped"))
            or (isinstance(r, str) and "⛔" in r)
        )
        self.assertEqual(blocked_count, 4)

    async def test_concurrent_signal_and_proposal_approval_respects_max_positions(self):
        """신호 매수 3건 + 제안 승인 매수 6건이 동시에 들어와도 한도(5)를 넘기면 안 된다
        — handle_proposal_response는 원래 한도체크 자체가 없었고(별도 수정으로 추가),
        신호 경로와 락도 공유하지 않았다. 이 테스트는 둘 중 하나라도 되돌리면 실패한다."""
        pool = FakePool()
        redis = FakeRedis()
        positions = []
        orders = []

        async def get_positions():
            await asyncio.sleep(0.005)
            return {"success": True, "data": list(positions)}

        async def kis_order(symbol, price, qty, is_buy):
            await asyncio.sleep(0.005)
            orders.append(symbol)
            if is_buy:
                positions.append({"symbol": symbol, "qty": qty, "avg_price": price})
            return {"success": True}

        async def run_signal(i):
            sym = f"{500000 + i:06d}"
            return await execution_guard.execute(
                make_signal(sym), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=noop,
                log_journal_fn=noop, save_trade_memory_fn=noop,
                code_to_name_fn=lambda s: s, get_positions_fn=get_positions,
                max_positions=5,
            )

        async def run_proposal(i):
            sym = f"{600000 + i:06d}"
            name = f"제안종목_{i}"
            redis.store[f"proposal:{sym}"] = json.dumps(
                {"symbol": sym, "name": name, "price": 70000, "qty": 1})
            return await order_handler.handle_proposal_response(
                f"{name} 사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=noop,
                send_telegram_fn=noop, get_positions_fn=get_positions, pool=pool,
            )

        results = await asyncio.gather(
            run_signal(0), run_signal(1), run_signal(2),
            run_proposal(0), run_proposal(1), run_proposal(2),
            run_proposal(3), run_proposal(4), run_proposal(5),
        )

        self.assertEqual(len(positions), 5, f"한도(5)를 넘겨 체결됨: {positions}")
        self.assertEqual(len(orders), 5)
        blocked_count = sum(
            1 for r in results
            if (isinstance(r, dict) and r.get("skipped"))
            or (isinstance(r, str) and "⛔" in r)
        )
        self.assertEqual(blocked_count, 4)

    async def test_sell_signal_proceeds_without_waiting_for_buy_lock(self):
        """매수가 매수 락을 길게 쥐고 있어도(KIS 주문 지연 모사), 동시에 들어온 매도
        신호는 그 락과 무관하게 즉시 체결돼야 한다(손절·익절이 매수 처리 대기 때문에
        지연되면 안 됨). execute() 내부 락을 다시 action과 무관하게 걸도록 되돌리면
        매도가 매수 지연(0.3초) 때문에 늦게 끝나 이 테스트가 실패한다."""
        pool = FakePool()
        redis = FakeRedis()
        # 매도 대상 종목을 미리 보유 중인 상태로 세팅
        positions = [{"symbol": "900000", "qty": 1, "avg_price": 70000}]
        buy_started = asyncio.Event()
        sell_done_at = {}
        buy_done_at = {}

        async def get_positions():
            return {"success": True, "data": list(positions)}

        async def kis_order(symbol, price, qty, is_buy):
            if is_buy:
                buy_started.set()
                await asyncio.sleep(0.3)  # KIS 주문이 느려지는 상황을 모사
                return {"success": True}
            return {"success": True}

        async def run_buy():
            res = await execution_guard.execute(
                make_signal("910000"), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=noop,
                log_journal_fn=noop, save_trade_memory_fn=noop,
                code_to_name_fn=lambda s: s, get_positions_fn=get_positions,
                max_positions=5,
            )
            buy_done_at["t"] = asyncio.get_event_loop().time()
            return res

        async def run_sell():
            await buy_started.wait()  # 매수가 락을 잡은 뒤에 매도를 시작
            sell_signal = {**make_signal("900000"), "action": "sell"}
            res = await execution_guard.execute(
                sell_signal, make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=noop,
                log_journal_fn=noop, save_trade_memory_fn=noop,
                code_to_name_fn=lambda s: s, get_positions_fn=get_positions,
                max_positions=5,
            )
            sell_done_at["t"] = asyncio.get_event_loop().time()
            return res

        results = await asyncio.gather(run_buy(), run_sell())
        self.assertTrue(all(r.get("executed") for r in results))
        # 매도가 매수(0.3초 지연) 완료보다 훨씬 먼저 끝나야 한다 — 락을 공유했다면 매도도
        # 매수가 끝날 때까지(>=0.3초) 대기했을 것이다.
        self.assertLess(sell_done_at["t"], buy_done_at["t"])
        self.assertGreaterEqual(buy_done_at["t"] - sell_done_at["t"], 0.2)

    async def test_chat_buy_returns_busy_message_on_lock_timeout(self):
        """다른 매수가 락을 오래 쥐고 있으면 채팅 매수는 무한정 기다리지 않고 타임아웃
        시 "다른 매수 처리 중" 안내를 반환해야 한다(주문을 진행하지 않음).

        락을 쥔 쪽을 반드시 별도 asyncio Task로 만들어야 한다 — AsyncRLock은 같은
        태스크 안에서는 재진입(reentrant)을 허용하므로, 테스트 코루틴이 직접
        lock.acquire()를 부르면 handle_trade_command가 "같은 태스크"로 보여 타임아웃
        없이 그냥 통과해버려 이 테스트의 의미가 사라진다."""
        original_timeout = order_handler.CHAT_BUY_LOCK_WAIT_SEC
        order_handler.CHAT_BUY_LOCK_WAIT_SEC = 0.05  # 테스트를 빠르게 하기 위한 짧은 타임아웃
        holder_released = asyncio.Event()

        async def _hold_lock_in_other_task():
            lock = execution_guard.get_buy_lock()
            await lock.acquire()
            try:
                await holder_released.wait()
            finally:
                await lock.release()

        holder = asyncio.create_task(_hold_lock_in_other_task())
        await asyncio.sleep(0.01)  # holder가 먼저 락을 잡을 시간을 준다
        try:
            orders = []

            async def kis_order(symbol, price, qty, is_buy):
                orders.append(symbol)
                return {"success": True}

            async def get_positions():
                return {"success": True, "data": []}

            async def get_quote(symbol):
                return {"stck_prpr": 70000}

            reply = await order_handler.handle_trade_command(
                "123456 1주 매수", pool=FakePool(), redis=FakeRedis(), universe=Universe(None),
                get_kis_token_fn=None, config=object(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_positions, send_telegram_fn=noop,
                log_journal_fn=noop, get_market_warning_fn=ok_market_warning,
                get_quote_fn=get_quote,
            )
            self.assertIn("다른 매수 처리 중", reply)
            self.assertEqual(len(orders), 0)
        finally:
            holder_released.set()
            await holder
            order_handler.CHAT_BUY_LOCK_WAIT_SEC = original_timeout


if __name__ == "__main__":
    unittest.main()
