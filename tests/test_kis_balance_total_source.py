"""
tests/test_kis_balance_total_source.py - [AT] fix/balance-total-source 회귀 테스트

확정된 버그: stock_trader/kis_trader.py get_balance()가 매수가능 조회 API
(inquire-psbl-order, VTTC8908R)의 output.tot_evlu_amt를 total로 읽었는데, 이 API 응답에는
tot_evlu_amt 키가 아예 없다(10/1 실측 — output 키 목록: cma_evlu_amt, fund_rpch_chgs,
max_buy_amt, max_buy_qty, nrcvb_buy_amt, nrcvb_buy_qty, ord_psbl_cash,
ord_psbl_frcr_amt_wcrc, ord_psbl_sbst, ovrs_re_use_amt_wcrc, psbl_qty_calc_unpr,
ruse_psbl_amt). 그래서 정상 응답에서도 total이 항상 0이었다.

수정: 총평가금액(total)은 잔고조회 API(inquire-balance, get_positions()가 이미 호출)의
output2[0].tot_evlu_amt에서 읽는다. get_positions()가 호출될 때마다 KISTrader._last_total/
_last_total_ts에 캐시해 두고, get_balance()는 180초 이내 신선하면 재사용하며 없으면
get_positions()를 1회 직접 호출한다. 둘 다 실패하면 total=0, stale=True(옛 값 재사용 금지).

이 파일은 (a)~(e)를 검증한다. 가짜 응답은 실제 KIS 응답 모양(inquire-psbl-order에
tot_evlu_amt가 없는 진짜 키 목록, inquire-balance의 output2에 tot_evlu_amt가 있는 모양)을
그대로 쓴다 — 기존 테스트처럼 inquire-psbl-order 응답에 tot_evlu_amt를 넣어 total을 읽는
잘못된 가정은 쓰지 않는다(그 가정이 이번 버그를 숨겼다).
"""
import asyncio
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402
from common.database import Database  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402

import main as stock_main  # noqa: E402  (stock_trader/main.py)
from main import StockTrader  # noqa: E402


# ── 실제 KIS 응답 모양(10/1 실측) ────────────────────────────────────────────

# inquire-psbl-order(VTTC8908R/TTTC8908R) 응답 — tot_evlu_amt 키가 아예 없다
REAL_PSBL_ORDER_OUTPUT = {
    "cma_evlu_amt": "0",
    "fund_rpch_chgs": "0",
    "max_buy_amt": "1000000",
    "max_buy_qty": "14",
    "nrcvb_buy_amt": "1000000",
    "nrcvb_buy_qty": "14",
    "ord_psbl_cash": "1000000",
    "ord_psbl_frcr_amt_wcrc": "0",
    "ord_psbl_sbst": "0",
    "ovrs_re_use_amt_wcrc": "0",
    "psbl_qty_calc_unpr": "70000",
    "ruse_psbl_amt": "0",
}


def _psbl_order_response(rt_cd="0"):
    return {"rt_cd": rt_cd, "msg_cd": "MCA00000", "msg1": "정상처리",
            "output": dict(REAL_PSBL_ORDER_OUTPUT)}


def _inquire_balance_response(total_eval="12000000", cash="3000000"):
    return {
        "rt_cd": "0",
        "output1": [],
        "output2": [{"tot_evlu_amt": total_eval, "dnca_tot_amt": cash}],
    }


class _Resp:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _UrlRoutedSession:
    """URL(엔드포인트 경로)에 따라 서로 다른 응답을 반환하는 세션 더블 —
    get_balance()가 내부적으로 inquire-psbl-order와 inquire-balance를 모두 호출할 수
    있게 된 것을 반영한다([AT] fix/balance-total-source)."""

    def __init__(self, psbl_order_resp=None, inquire_balance_resp=None):
        self.psbl_order_resp = psbl_order_resp
        self.inquire_balance_resp = inquire_balance_resp
        self.psbl_order_calls = 0
        self.inquire_balance_calls = 0

    def get(self, url, *args, **kwargs):
        if "inquire-psbl-order" in url:
            self.psbl_order_calls += 1
            return _Resp(self.psbl_order_resp)
        if "inquire-balance" in url:
            self.inquire_balance_calls += 1
            return _Resp(self.inquire_balance_resp)
        raise AssertionError(f"예상치 못한 URL 호출: {url}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _make_trader():
    config.KIS_ACCOUNT_NO = "50193041"
    return KISTrader()


# ── (a) 둘 다 정상 — total이 inquire-balance에서 읽혀 양수로 나온다 ─────────


class TestGetBalanceReadsTotalFromInquireBalance(unittest.IsolatedAsyncioTestCase):
    async def test_total_is_positive_when_psbl_order_lacks_tot_evlu_amt(self):
        """inquire-psbl-order 응답(실제 키 목록, tot_evlu_amt 없음)만으로는 total이 0이
        됐던 버그 — inquire-balance(output2.tot_evlu_amt)에서 읽어 양수가 나와야 한다."""
        trader = _make_trader()
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp=_inquire_balance_response(total_eval="12000000", cash="3000000"),
        )
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["cash"], 1000000)  # ord_psbl_cash(psbl-order)
        self.assertEqual(res["total"], 12000000)  # tot_evlu_amt(inquire-balance)
        self.assertNotIn("stale", res)
        self.assertEqual(session.psbl_order_calls, 1)
        self.assertEqual(session.inquire_balance_calls, 1)  # 신선한 캐시 없으므로 1회 직접 조회

    async def test_psbl_order_response_has_no_tot_evlu_amt_key(self):
        """회귀 방지 문서화: 실제 inquire-psbl-order 응답에는 tot_evlu_amt 키가 없다."""
        self.assertNotIn("tot_evlu_amt", REAL_PSBL_ORDER_OUTPUT)


# ── (c) 신선한 캐시 재사용 / 오래되면 직접 조회 ──────────────────────────────


class TestTotalCacheFreshness(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_last_total_is_reused_without_extra_kis_call(self):
        """get_positions()가 최근에 채워 둔 _last_total이 180초 이내면 inquire-balance를
        추가로 호출하지 않고 그대로 재사용한다."""
        trader = _make_trader()
        trader._last_total = 9_000_000
        trader._last_total_ts = time.time() - 10  # 10초 전 — 신선함
        session = _UrlRoutedSession(psbl_order_resp=_psbl_order_response())
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["total"], 9_000_000)
        self.assertNotIn("stale", res)
        self.assertEqual(session.inquire_balance_calls, 0)  # 추가 호출 없음

    async def test_stale_last_total_triggers_direct_refetch(self):
        """_last_total이 180초를 넘기면 신선하지 않다고 보고 inquire-balance를 1회 직접 조회한다."""
        trader = _make_trader()
        trader._last_total = 1_000  # 오래된 값(새 값과 달라야 재조회됐는지 구분 가능)
        trader._last_total_ts = time.time() - 181  # 181초 전 — 신선하지 않음
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp=_inquire_balance_response(total_eval="20000000", cash="5000000"),
        )
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["total"], 20_000_000)  # 오래된 1,000이 아니라 새로 조회된 값
        self.assertNotIn("stale", res)
        self.assertEqual(session.inquire_balance_calls, 1)

    async def test_exactly_at_ttl_boundary_is_treated_as_stale(self):
        """180초 "이내"만 신선한 것으로 본다 — 정확히 180초 지났으면 직접 재조회해야 한다."""
        trader = _make_trader()
        trader._last_total = 1_000
        trader._last_total_ts = time.time() - 180
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp=_inquire_balance_response(total_eval="7000000", cash="1000000"),
        )
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["total"], 7_000_000)
        self.assertEqual(session.inquire_balance_calls, 1)


# ── (d) 둘 다 실패하면 total=0, stale=True(옛 값 재사용 금지) ───────────────


class TestTotalBothSourcesFail(unittest.IsolatedAsyncioTestCase):
    async def test_both_fail_returns_zero_and_stale_not_old_value(self):
        """_last_total이 없고(=0) inquire-balance 직접 조회도 실패하면, 옛 값을 총자산으로
        돌려주지 않고 total=0, stale=True를 반환해야 한다."""
        trader = _make_trader()
        # _last_total은 초기값(0)이라 신선한 캐시가 없다.
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp={"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "토큰 오류"},
        )
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["total"], 0)
        self.assertTrue(res.get("stale"))
        self.assertEqual(res["cash"], 1000000)  # 예수금 조회 자체는 성공했으므로 그대로 반환

    async def test_stale_old_last_total_is_not_reused_when_refetch_fails(self):
        """_last_total이 오래됐고(180초 초과) 재조회마저 실패하면, 오래된 값을 쓰지 않고
        0+stale로 돌아가야 한다(옛 값을 총자산으로 돌려주지 말라는 안전 규칙)."""
        trader = _make_trader()
        trader._last_total = 99_999_999  # 눈에 띄는 오래된 값 — 이게 반환되면 버그
        trader._last_total_ts = time.time() - 300
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp={"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "속도제한"},
        )
        trader._new_session = lambda: session

        with patch("stock_trader.kis_trader.asyncio.sleep", new=AsyncMock()):
            res = await trader.get_balance()

        self.assertEqual(res["total"], 0)
        self.assertTrue(res.get("stale"))
        self.assertNotEqual(res["total"], 99_999_999)

    async def test_get_positions_exception_returns_zero_and_stale(self):
        """get_positions() 자체가 예외를 던져도(네트워크 오류 등) total=0, stale=True로
        안전하게 떨어져야 하고 예외가 get_balance() 밖으로 전파되면 안 된다."""
        trader = _make_trader()

        async def _raise(*a, **kw):
            raise ConnectionError("연결 거부")

        trader.get_positions = _raise
        session = _UrlRoutedSession(psbl_order_resp=_psbl_order_response())
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["total"], 0)
        self.assertTrue(res.get("stale"))


# ── get_positions()가 _last_total을 채우는지 직접 확인 ──────────────────────


class TestGetPositionsCachesLastTotal(unittest.IsolatedAsyncioTestCase):
    async def test_get_positions_stores_tot_evlu_amt_as_last_total(self):
        trader = _make_trader()
        session = _UrlRoutedSession(
            inquire_balance_resp=_inquire_balance_response(total_eval="15000000", cash="2000000"))
        trader._new_session = lambda: session

        await trader.get_positions()

        self.assertEqual(trader._last_total, 15_000_000)
        self.assertGreater(trader._last_total_ts, 0)


# ── (e) 일별 기록이 이 실제 모양의 응답으로 기록됨 ──────────────────────────


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeSnapshotConn:
    def __init__(self):
        self.executed = []

    async def fetchval(self, query, *args):
        return False  # 오늘 기록 아직 없음

    async def execute(self, query, *args):
        self.executed.append(args)


class _FakeSnapshotPool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestDailySnapshotRecordsWithRealResponseShape(unittest.IsolatedAsyncioTestCase):
    async def test_try_record_balance_snapshot_inserts_with_real_kis_shape(self):
        """실제 KIS 응답 모양(psbl-order에 tot_evlu_amt 없음)으로 get_balance()가 정상적으로
        total을 채우면, _try_record_balance_snapshot이 그 값을 그대로 기록해야 한다."""
        kis_trader = _make_trader()
        session = _UrlRoutedSession(
            psbl_order_resp=_psbl_order_response(),
            inquire_balance_resp=_inquire_balance_response(total_eval="11000000", cash="2500000"),
        )
        kis_trader._new_session = lambda: session

        bot = StockTrader.__new__(StockTrader)
        bot.trader = kis_trader

        conn = _FakeSnapshotConn()
        db = Database.__new__(Database)
        db.pool = _FakeSnapshotPool(conn)

        with patch.object(stock_main, "db", db):
            recorded, reason = await StockTrader._try_record_balance_snapshot(bot)

        self.assertTrue(recorded)
        self.assertEqual(reason, "")
        self.assertEqual(len(conn.executed), 1)
        bot_name, total, cash, eval_krw = conn.executed[0]
        self.assertEqual(bot_name, "stock_trader")
        self.assertEqual(total, 11_000_000)
        # cash는 항상 inquire-psbl-order(ord_psbl_cash)에서 온다 — 그대로 유지(item 1)
        self.assertEqual(cash, 1_000_000)
        self.assertEqual(eval_krw, 11_000_000 - 1_000_000)


if __name__ == "__main__":
    unittest.main()
