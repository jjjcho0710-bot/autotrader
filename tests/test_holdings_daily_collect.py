"""
보유종목 일봉 수집 중단 방지 테스트 ([AT] fix/holdings-daily-collect, PM 지시).

배경: 000270(기아), 249420(일진머티리얼즈) 등의 일봉 수집이 중단됐다. 원인: 08:30 아침
스캐너(dashboard._jarvis_stock_scanner)의 보유종목 보호 로직이 get_stock_positions()
결과로 held_symbols를 구하는데, KIS 응답 실패(success:False)나 stale일 때 held_symbols가
빈 집합이 되어 실제 보유 중인 scanner 종목이 is_active=FALSE로 꺼지고 그날부터 일봉 수집
대상(watchlist WHERE is_active=TRUE)에서 빠졌다. 또한 채팅 수동매수 등으로 담은 종목은
watchlist에 upsert되는 경로가 전혀 없어 애초에 수집 대상에 들지 못할 수 있었다.

검증 항목:
  1) 보유 조회 실패(success:False)/stale/예외 시 아침 스캐너의 비활성화 루프 전체를
     건너뛰는지(오늘 뽑히지 않은 종목도 끄지 않음) + 경고 로그.
  2) 정상 조회 시 기존 동작 유지(보유중 아닌 옛 scanner 종목은 그대로 비활성화).
  3) KIS 잔고 조회 output2 빈 배열 응답 시 다른 실패 분기와 동일하게 last_ok 스냅샷으로
     폴백(stale=True 유지)하는지.
  4) 매수 체결 확정 시 watchlist upsert(AI 자동매수/수동 채팅매수/제안승인 매수/
     stock_trader 지연체결확인) — 신규 추가와 기존 비활성 종목 재활성화 모두.
  5) watchlist upsert가 예외로 실패해도 매수 기록·주문 흐름은 정상 진행.
  6) 매도 경로는 watchlist upsert와 무관.

revert 확인(수동): dashboard/main.py _jarvis_stock_scanner()의 "if positions_ok:" 보호
조건을 제거하면 test_positions_failure_skips_deactivation_loop와 그 stale/예외 변형이
모두 실패(비활성화가 실행돼버림)하는 것을 확인했다.
"""
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


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

        class ServerTimeoutError(Exception):
            pass

        stub.ServerTimeoutError = ServerTimeoutError
        sys.modules["aiohttp"] = stub

    for mod_name in [
        "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
        "google", "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()

import dashboard.main as dm  # noqa: E402
from stark import execution_guard  # noqa: E402
from router.handlers import order_handler, watchlist_handler  # noqa: E402
from tests.test_morning_scan_fallback import _winning_df  # noqa: E402
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
from market.universe import Universe  # noqa: E402


# ── 1)+2) 아침 스캐너 보유종목 보호 ──────────────────────────────
class _ScanAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class ScanFakeConn:
    """watchlist 비활성화(1단계)/활성화(2단계) 쿼리를 모두 받는 대역.
    old_scanner_symbols: 현재 is_active=TRUE, added_by='jarvis_scanner'인 종목들."""

    def __init__(self, old_scanner_symbols):
        self.old_scanner_symbols = list(old_scanner_symbols)
        self.executed = []

    async def fetch(self, query, *args):
        if "added_by='jarvis_scanner'" in query:
            return [{"symbol": s} for s in self.old_scanner_symbols]
        if "FROM watchlist" in query:
            return []
        return []

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "OK"


class ScanFakePool:
    def __init__(self, old_scanner_symbols):
        self.conn = ScanFakeConn(old_scanner_symbols)

    def acquire(self):
        return _ScanAcquireCtx(self.conn)


class TestScannerHoldingsProtection(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        self.sent = []

        async def fake_send_telegram(text, **kw):
            self.sent.append((text, kw))

        start(patch.object(dm, "_send_telegram", side_effect=fake_send_telegram))
        start(patch("time.sleep", new=MagicMock()))

        self._pykrx = MagicMock()
        start(patch.dict(sys.modules, {"pykrx": MagicMock(stock=self._pykrx), "pykrx.stock": self._pykrx}))

        def get_market_ticker_list(date, market=None):
            return {"KOSPI": ["000020"], "KOSDAQ": []}[market]

        def get_market_ohlcv(d30, today, ticker):
            return {"005930": None, "000020": _winning_df()}.get(ticker)

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=get_market_ticker_list)
        self._pykrx.get_market_ohlcv = MagicMock(side_effect=get_market_ohlcv)
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=lambda t: f"종목{t}")

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def _run(self, *, get_positions_result=None, get_positions_exc=None,
                   old_scanner_symbols=("011200",)):
        pool = ScanFakePool(old_scanner_symbols)

        async def fake_positions():
            if get_positions_exc is not None:
                raise get_positions_exc
            return get_positions_result

        with patch.object(dm, "db_pool", pool), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(side_effect=fake_positions)):
            with self.assertLogs("dashboard", level="WARNING") if (
                get_positions_exc is not None or not (get_positions_result or {}).get("success")
                or (get_positions_result or {}).get("stale")
            ) else _nullcontext():
                await dm._jarvis_stock_scanner()
        return pool

    def _deactivated_symbols(self, pool):
        return {
            args[0] for query, args in pool.conn.executed
            if "UPDATE watchlist" in query and "is_active=FALSE" in query
        }

    async def test_positions_failure_skips_deactivation_loop(self):
        """get_stock_positions() success:False → 비활성화 루프 전체를 건너뛴다
        (보유 중일 수 있는 011200도 꺼지지 않아야 한다)."""
        pool = await self._run(
            get_positions_result={"success": False, "error": "KIS 조회 실패", "data": []},
            old_scanner_symbols=("011200",))
        self.assertEqual(self._deactivated_symbols(pool), set())

    async def test_positions_stale_skips_deactivation_loop(self):
        """stale=True(last_ok 폴백 데이터)도 신뢰할 수 없으므로 동일하게 건너뛴다."""
        pool = await self._run(
            get_positions_result={"success": True, "stale": True, "data": []},
            old_scanner_symbols=("011200",))
        self.assertEqual(self._deactivated_symbols(pool), set())

    async def test_positions_exception_skips_deactivation_loop(self):
        """get_stock_positions()가 예외를 던져도 동일하게 건너뛴다."""
        pool = await self._run(
            get_positions_exc=RuntimeError("연결 끊김"),
            old_scanner_symbols=("011200",))
        self.assertEqual(self._deactivated_symbols(pool), set())

    async def test_successful_lookup_still_deactivates_non_held_old_symbols(self):
        """정상 조회 시에는 기존 동작 그대로 — 보유 중 아닌 옛 scanner 종목은 비활성화되고,
        보유 중인 종목은 보호된다(회귀 아님)."""
        pool = await self._run(
            get_positions_result={"success": True, "data": [{"symbol": "000270"}]},
            old_scanner_symbols=("011200", "000270"))
        self.assertEqual(self._deactivated_symbols(pool), {"011200"})

    async def test_today_candidates_still_activated_when_positions_fail(self):
        """보유 조회가 실패해도 오늘 스캔에서 뽑힌 종목의 활성화(2단계)는 그대로 진행된다
        — 비활성화 루프만 건너뛰는 것이지 전체 갱신을 막는 게 아니다."""
        pool = await self._run(
            get_positions_result={"success": False, "error": "x", "data": []},
            old_scanner_symbols=())
        inserted = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertIn("000020", inserted)


class _nullcontext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


# ── 3) KIS 잔고 조회 output2 빈 배열 → last_ok 폴백 ──────────────
class _FakeKisResponse:
    def __init__(self, data: dict, status: int = 200):
        self._data = data
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class _FakeKisSession:
    def __init__(self, output2):
        self._output2 = output2

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return _FakeKisResponse({"rt_cd": "0", "output1": [], "output2": self._output2})


class FakeRedisLastOk:
    def __init__(self, snapshot=None):
        self._snapshot = snapshot
        self.setex_calls = []

    async def get(self, key):
        if key == "positions:last_ok" and self._snapshot is not None:
            return json.dumps(self._snapshot)
        return None

    async def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))


class TestOutputTwoEmptyFallback(unittest.IsolatedAsyncioTestCase):
    async def _run(self, *, output2, redis_snapshot=None):
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0
        fake_redis = FakeRedisLastOk(redis_snapshot)
        with patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch.object(dm, "redis_client", fake_redis), \
             patch("aiohttp.ClientSession", return_value=_FakeKisSession(output2)):
            return await dm._get_stock_positions_raw()

    async def test_empty_output2_falls_back_to_last_ok_snapshot(self):
        """output2가 빈 배열이어도 다른 실패 분기와 동일하게 last_ok 스냅샷으로
        폴백해야 한다(fix 이전엔 success:False만 반환)."""
        snapshot = {
            "success": True,
            "data": [{"symbol": "000270", "qty": 10, "cur_price": 50000}],
            "account": {"total_eval": 10_000_000, "cash": 1_000_000},
        }
        result = await self._run(output2=[], redis_snapshot=snapshot)
        self.assertTrue(result.get("success"))
        self.assertTrue(result.get("stale"))
        self.assertEqual(result["account"]["total_eval"], 10_000_000)

    async def test_empty_output2_without_snapshot_returns_failure(self):
        """last_ok 스냅샷조차 없으면(최초 실행 등) 기존처럼 success:False를 반환한다."""
        result = await self._run(output2=[], redis_snapshot=None)
        self.assertFalse(result.get("success"))
        self.assertEqual(result.get("data"), [])


# ── 4)+5) 매수 체결 확정 시 watchlist upsert ──────────────────
class WatchlistAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class WatchlistFakeConn:
    def __init__(self, rows=None):
        self.rows = {r["symbol"]: dict(r) for r in (rows or [])}
        self.executed = []

    async def execute(self, query, *args):
        self.executed.append((query, args))
        if "INSERT INTO watchlist" in query:
            symbol, name = args[0], args[1]
            row = self.rows.setdefault(symbol, {"symbol": symbol, "name": name, "is_active": False})
            row["is_active"] = True
            if name:
                row["name"] = name
        return "OK"

    async def fetch(self, query, *args):
        return []

    async def fetchrow(self, query, *args):
        return None

    async def fetchval(self, query, *args):
        return None


class WatchlistFakePool:
    def __init__(self, rows=None):
        self.conn = WatchlistFakeConn(rows)

    def acquire(self):
        return WatchlistAcquireCtx(self.conn)


class TestExecutionGuardWatchlistUpsert(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    async def test_ai_buy_fill_upserts_watchlist_new_symbol(self):
        pool = WatchlistFakePool()
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
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertTrue(result["executed"])
        self.assertIn("005930", pool.conn.rows)
        self.assertTrue(pool.conn.rows["005930"]["is_active"])

    async def test_ai_buy_fill_reactivates_inactive_symbol(self):
        pool = WatchlistFakePool(rows=[{"symbol": "005930", "name": "삼성전자", "is_active": False}])
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertTrue(pool.conn.rows["005930"]["is_active"])

    async def test_upsert_failure_does_not_block_buy_fill(self):
        """watchlist upsert가 예외로 실패해도 매수 체결 자체는 성공해야 한다."""
        pool = WatchlistFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        with patch.object(watchlist_handler, "add_symbol", new=AsyncMock(side_effect=RuntimeError("DB 오류"))):
            result = await execution_guard.execute(
                make_signal(), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name,
            )
        self.assertTrue(result["executed"])

    async def test_sell_does_not_touch_watchlist(self):
        pool = WatchlistFakePool()
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        await execution_guard.execute(
            make_signal(action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertEqual(pool.conn.rows, {})


class TestOrderHandlerWatchlistUpsert(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    def _quote_fn(self, price: int = 70000, name: str = "삼성전자"):
        return make_quote_fn({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})

    async def _ok_warning(self, symbol):
        return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

    async def test_manual_chat_buy_upserts_watchlist(self):
        pool = WatchlistFakePool()
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def positions():
            return {"success": True, "data": []}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        reply = await order_handler.handle_trade_command(
            "삼성전자 1주 매수", pool=pool, redis=OhFakeRedis(), universe=universe,
            get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
            get_stock_positions_fn=positions, get_market_warning_fn=self._ok_warning,
            get_quote_fn=self._quote_fn(), send_telegram_fn=send_telegram, log_journal_fn=log_journal,
        )
        self.assertIn("매수 완료", reply)
        self.assertIn("005930", pool.conn.rows)
        self.assertTrue(pool.conn.rows["005930"]["is_active"])

    async def test_manual_chat_sell_does_not_touch_watchlist(self):
        pool = WatchlistFakePool()
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def positions():
            return {"success": True, "data": [{"symbol": "005930", "qty": 5, "avg_price": 60000}]}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        reply = await order_handler.handle_trade_command(
            "삼성전자 2주 매도", pool=pool, redis=OhFakeRedis(), universe=universe,
            get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
            get_stock_positions_fn=positions, get_market_warning_fn=self._ok_warning,
            get_quote_fn=self._quote_fn(), send_telegram_fn=send_telegram, log_journal_fn=log_journal,
        )
        self.assertIn("매도", reply)
        self.assertEqual(pool.conn.rows, {})

    async def test_manual_chat_buy_upsert_failure_does_not_block_order(self):
        pool = WatchlistFakePool()
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def positions():
            return {"success": True, "data": []}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        with patch.object(watchlist_handler, "add_symbol", new=AsyncMock(side_effect=RuntimeError("DB 오류"))):
            reply = await order_handler.handle_trade_command(
                "삼성전자 1주 매수", pool=pool, redis=OhFakeRedis(), universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=positions, get_market_warning_fn=self._ok_warning,
                get_quote_fn=self._quote_fn(), send_telegram_fn=send_telegram, log_journal_fn=log_journal,
            )
        self.assertIn("매수 완료", reply)

    async def test_proposal_approval_buy_upserts_watchlist(self):
        pool = WatchlistFakePool()
        redis = OhFakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps(
                {"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        async def get_positions():
            return {"success": True, "data": []}

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertIn("005930", pool.conn.rows)
        self.assertTrue(pool.conn.rows["005930"]["is_active"])


class TestStockTraderPendingBuyWatchlistUpsert(unittest.IsolatedAsyncioTestCase):
    async def test_pending_buy_confirmed_upserts_watchlist(self):
        import stock_trader.main as st_main

        trader = MagicMock()
        trader._get_filled_qty = AsyncMock(return_value=(5, 70000))
        trader.invalidate_balance_cache = MagicMock()

        bot = st_main.StockTrader.__new__(st_main.StockTrader)
        bot.trader = trader
        bot._invalidate_position_cache = AsyncMock()
        bot._clear_pending_order = AsyncMock()

        ctx = {"symbol": "005930", "side": "buy", "qty": 5, "price": 70000,
               "strategy": "MA크로스", "avg_price": None, "accepted_at": 0}

        with patch.object(st_main.db, "insert_trade", new=AsyncMock()), \
             patch.object(st_main.db, "add_watchlist", new=AsyncMock(return_value=True)) as mock_add, \
             patch.object(st_main.cache, "client", MagicMock(get=AsyncMock(return_value=json.dumps(ctx)))):
            await bot._reconcile_one_pending_order("order_pending:ORDER1")

        mock_add.assert_awaited_once()
        self.assertEqual(mock_add.call_args.args[0], "005930")

    async def test_pending_sell_confirmed_does_not_touch_watchlist(self):
        import stock_trader.main as st_main

        trader = MagicMock()
        trader._get_filled_qty = AsyncMock(return_value=(5, 70000))
        trader.invalidate_balance_cache = MagicMock()

        bot = st_main.StockTrader.__new__(st_main.StockTrader)
        bot.trader = trader
        bot._invalidate_position_cache = AsyncMock()
        bot._clear_pending_order = AsyncMock()

        ctx = {"symbol": "005930", "side": "sell", "qty": 5, "price": 70000,
               "strategy": "손절", "avg_price": 65000, "accepted_at": 0}

        with patch.object(st_main.db, "insert_trade", new=AsyncMock()), \
             patch.object(st_main.db, "add_watchlist", new=AsyncMock(return_value=True)) as mock_add, \
             patch.object(st_main.cache, "client", MagicMock(get=AsyncMock(return_value=json.dumps(ctx)))):
            await bot._reconcile_one_pending_order("order_pending:ORDER2")

        mock_add.assert_not_awaited()

    async def test_pending_buy_watchlist_failure_does_not_raise(self):
        import stock_trader.main as st_main

        trader = MagicMock()
        trader._get_filled_qty = AsyncMock(return_value=(5, 70000))
        trader.invalidate_balance_cache = MagicMock()

        bot = st_main.StockTrader.__new__(st_main.StockTrader)
        bot.trader = trader
        bot._invalidate_position_cache = AsyncMock()
        bot._clear_pending_order = AsyncMock()

        ctx = {"symbol": "005930", "side": "buy", "qty": 5, "price": 70000,
               "strategy": "", "avg_price": None, "accepted_at": 0}

        with patch.object(st_main.db, "insert_trade", new=AsyncMock()), \
             patch.object(st_main.db, "add_watchlist", new=AsyncMock(side_effect=RuntimeError("DB 오류"))), \
             patch.object(st_main.cache, "client", MagicMock(get=AsyncMock(return_value=json.dumps(ctx)))):
            await bot._reconcile_one_pending_order("order_pending:ORDER3")  # 예외가 전파되지 않아야 함

        bot._clear_pending_order.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
