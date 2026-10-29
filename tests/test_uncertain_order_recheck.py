"""
[AT] fix/uncertain-order-recheck 회귀 테스트.

배경: 10/14 005880 BUY 497주(@2135, 09:32:56)가 "결과 불명" 알림 후 실제로는
체결됐는데 DB 기록이 누락됐다. stark/execution_guard.py reconcile_uncertain_order가
4초 1회 확인 후 status=unknown으로 끝나고 이후 자동 재확인이 없던 것이 원인.

이 파일은 다음을 검증한다:
1) common/uncertain_order_tracker.py: 등록 → 첫 체크포인트(unknown)에서는 확정하지
   않고 다음 체크포인트로 넘어간다 → 다음 체크포인트에서 체결 확인되면 trade_history/
   매매일지 기록 + "체결 확인(지연)" 알림 + order_uncertain 차단 키 해제.
2) 마지막 체크포인트까지 체결 확인이 안 되면 "미확인 — 수동 확인 필요" 알림을 1회만
   보내고 추적을 종료한다.
3) 이미 같은 종목·방향·날짜·수량의 trade_history 행이 있으면 중복 기록하지 않는다
   (차단 키 해제는 그대로 수행).
4) 보유 수량 조회 실패(fail-safe) 시 체크포인트를 넘기지 않고 다음 주기에 재시도한다.
5) stark/execution_guard.reconcile_uncertain_order가 bot/name을 받으면 unknown으로
   끝날 때 백그라운드 재확인을 실제로 등록한다(연동 확인).
"""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common import uncertain_order_tracker as uot  # noqa: E402


class FakeConn:
    def __init__(self, fetchval_result=None):
        self.inserted = []
        self.fetchval_result = fetchval_result

    async def execute(self, query, *args):
        assert "trade_history" in query
        self.inserted.append(args)
        return "INSERT 1"

    async def fetchval(self, query, *args):
        return self.fetchval_result


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self, fetchval_result=None):
        self._conn = FakeConn(fetchval_result=fetchval_result)

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    """uncertain_order_tracker가 쓰는 get/setex/delete/scan_iter만 지원하는 인메모리 대역."""

    def __init__(self, store=None):
        self.store = store or {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, val):
        self.store[key] = val

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)

    async def scan_iter(self, match="*"):
        import fnmatch
        for k in list(self.store.keys()):
            if fnmatch.fnmatch(k, match):
                yield k


def _ctx(checkpoint_idx=0, elapsed=31, pre_qty=0.0, qty=497, is_buy=True, extra=None):
    import time as _time
    base = {
        "symbol": "005880", "name": "현대차", "side": "buy" if is_buy else "sell",
        "bot": "stock_trader", "pre_qty": pre_qty, "qty": qty, "price": 2135,
        "strategy": "AI매수", "reason": "모멘텀", "reply": "EXECUTE", "order_no": None,
        "pre_avg_price": None, "registered_at": _time.time() - elapsed,
        "checkpoint_idx": checkpoint_idx,
    }
    if extra:
        base.update(extra)
    return base


class TestRegisterUncertainOrder(unittest.IsolatedAsyncioTestCase):
    async def test_skips_registration_when_pre_qty_none(self):
        redis = FakeRedis()
        await uot.register_uncertain_order(
            redis, symbol="005880", name="현대차", is_buy=True, bot="stock_trader",
            pre_qty=None, qty=497, price=2135, strategy="AI매수",
        )
        self.assertEqual(redis.store, {})

    async def test_registers_context_with_order_no_key(self):
        redis = FakeRedis()
        await uot.register_uncertain_order(
            redis, symbol="005880", name="현대차", is_buy=True, bot="stock_trader",
            pre_qty=0.0, qty=497, price=2135, strategy="AI매수", order_no="O-123",
        )
        raw = redis.store[f"{uot.UNCERTAIN_ORDER_KEY_PREFIX}O-123"]
        ctx = json.loads(raw)
        self.assertEqual(ctx["symbol"], "005880")
        self.assertEqual(ctx["side"], "buy")
        self.assertEqual(ctx["checkpoint_idx"], 0)


class TestPollUncertainOrders(unittest.IsolatedAsyncioTestCase):
    async def test_first_checkpoint_unknown_advances_without_finalizing(self):
        """첫 체크포인트(+30초)에서 보유 수량 변화가 없으면 확정하지 않고 다음
        체크포인트로 넘어가야 한다(기록·알림 없음)."""
        redis = FakeRedis({"uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=0, elapsed=31))})
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": []}  # pre_qty=0, 여전히 0 — 변화 없음

        async def send_telegram(text):
            sent.append(text)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 0)
        self.assertEqual(sent, [])
        ctx = json.loads(redis.store["uncertain_order_pending:O1"])
        self.assertEqual(ctx["checkpoint_idx"], 1)

    async def test_second_checkpoint_confirms_fill_records_and_clears_block(self):
        """두 번째 체크포인트(+90초)에서 보유 수량이 늘었으면 체결로 확정하고
        trade_history/매매일지 기록 + "체결 확인(지연)" 알림 + 차단 키 해제를 한다."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=1, elapsed=91)),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool()
        sent = []
        journaled = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 497, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, log_journal)

        self.assertEqual(len(pool._conn.inserted), 1)
        inserted = pool._conn.inserted[0]
        self.assertEqual(inserted[2], "005880")  # symbol
        self.assertEqual(inserted[3], "BUY")  # side
        self.assertEqual(inserted[5], 497.0)  # quantity
        self.assertEqual(len(journaled), 1)
        self.assertEqual(len(sent), 1)
        self.assertIn("체결 확인(지연)", sent[0])
        self.assertNotIn("order_uncertain:005880:buy", redis.store)
        self.assertNotIn("uncertain_order_pending:O1", redis.store)

    async def test_sell_fill_confirmed_computes_pnl_from_pre_avg_price(self):
        redis = FakeRedis({
            "uncertain_order_pending:O2": json.dumps(_ctx(
                checkpoint_idx=0, elapsed=31, pre_qty=10.0, qty=10, is_buy=False,
                extra={"price": 60000, "pre_avg_price": 70000.0})),
        })
        pool = FakePool()

        async def get_positions():
            return {"success": True, "data": []}  # 전량 매도 → 보유 0

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, AsyncMock(), AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 1)
        inserted = pool._conn.inserted[0]
        self.assertEqual(inserted[3], "SELL")
        self.assertEqual(inserted[5], 10.0)
        self.assertEqual(inserted[8], (60000 - 70000.0) * 10)  # pnl

    async def test_partial_increase_at_first_checkpoint_keeps_tracking_without_finalizing(self):
        """[AT] fix/uncertain-order-partial-finalize 회귀: 보유 증가분(1주)이 주문
        수량(3주)보다 적으면 첫 체크포인트에서 바로 확정하지 말고 다음 체크포인트로
        넘어가야 한다(10/16 002030 BUY가 1주 증가 시점에 즉시 확정·키 삭제돼 나머지
        2주가 영영 기록되지 않은 사고 재현 방지)."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(
                _ctx(checkpoint_idx=0, elapsed=31, pre_qty=0.0, qty=3)),
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 1, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 0)
        self.assertEqual(sent, [])
        ctx = json.loads(redis.store["uncertain_order_pending:O1"])
        self.assertEqual(ctx["checkpoint_idx"], 1)  # 확정하지 않고 다음 체크포인트로 이동

    async def test_additional_fill_at_next_checkpoint_finalizes_with_full_quantity(self):
        """부분 반영(1주) 후 다음 체크포인트에서 주문 수량(3주)을 채우면 그제서야
        전체 증가분(3주)으로 확정해 기록한다(추가 체결분이 정상 반영됨)."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(
                _ctx(checkpoint_idx=1, elapsed=91, pre_qty=0.0, qty=3)),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 3, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 1)
        inserted = pool._conn.inserted[0]
        self.assertEqual(inserted[5], 3.0)  # quantity — 1주가 아니라 누적 전체(3주)로 기록
        self.assertIn("체결 확인(지연)", sent[0])
        self.assertNotIn("uncertain_order_pending:O1", redis.store)

    async def test_last_early_checkpoint_partial_keeps_tracking_before_close(self):
        """[fix/uncertain-order-extended-tracking] 180초 체크포인트에서 부분 반영(1/3주)이어도
        마감 전이면 확정·키 삭제 없이 계속 추적한다(차단 키 유지, 안내 알림 1회)."""
        last_idx = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC) - 1
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(
                _ctx(checkpoint_idx=last_idx, elapsed=181, pre_qty=0.0, qty=3)),
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 1, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 0)
        self.assertIn("uncertain_order_pending:O1", redis.store)
        self.assertIn("order_uncertain:005880:buy", redis.store)  # 차단 키 유지
        self.assertEqual(len(sent), 1)
        self.assertIn("계속 추적", sent[0])
        ctx = json.loads(redis.store["uncertain_order_pending:O1"])
        self.assertEqual(ctx["checkpoint_idx"], len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC))

    async def test_final_check_after_close_partial_records_best_effort_and_warns_shortfall(self):
        """마감 후 최종 확인에서도 주문 수량(3주)에 못 미치면(1주) 확인된 증가분만 기록하고
        부족분(2주)을 알린 뒤 추적·차단 키를 종료한다."""
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(
                _ctx(checkpoint_idx=n, elapsed=20000, pre_qty=0.0, qty=3)),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 1, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=True):
            await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(pool._conn.inserted[0][5], 1.0)
        self.assertIn("체결 확인(지연)", sent[0])
        self.assertIn("2주는 수동 확인 필요", sent[0])
        self.assertNotIn("uncertain_order_pending:O1", redis.store)
        self.assertNotIn("order_uncertain:005880:buy", redis.store)

    async def test_no_fill_before_close_keeps_block_key_and_does_not_end(self):
        """180초가 지나도 체결 없음 + 마감 전이면 종료하지 않는다(과거: 키 삭제·수동확인 알림)."""
        last_idx = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC) - 1
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=last_idx, elapsed=181)),
        })
        sent = []

        async def get_positions():
            return {"success": True, "data": []}

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, FakePool(), get_positions, None, send_telegram, AsyncMock())

        self.assertIn("uncertain_order_pending:O1", redis.store)
        self.assertIn("order_uncertain:005880:buy", redis.store)
        self.assertFalse(any("수동 확인 필요" in m for m in sent))

    async def test_exhausts_all_checkpoints_sends_manual_check_alert_once(self):
        """마감 후 최종 확인에서도 변화 없으면 "미확인 — 수동 확인 필요"를 1회만 보내고
        추적 키와 차단 키를 함께 종료(삭제)한다."""
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=n, elapsed=20000)),
            "order_uncertain:005880:buy": "1",
        })
        sent = []

        async def get_positions():
            return {"success": True, "data": []}

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=True):
            await uot.poll_uncertain_orders_once(redis, FakePool(), get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(sent), 1)
        self.assertIn("미확인 — 수동 확인 필요", sent[0])
        self.assertNotIn("uncertain_order_pending:O1", redis.store)
        self.assertNotIn("order_uncertain:005880:buy", redis.store)

    async def test_fill_after_180s_before_close_is_now_captured(self):
        """핵심 회귀: 180초 이후(마감 전)에 늦게 체결돼도 놓치지 않고 기록한다."""
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(
                checkpoint_idx=n, elapsed=600, extra={"last_check_at": __import__("time").time() - 200})),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 497, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(pool._conn.inserted[0][5], 497.0)
        self.assertNotIn("order_uncertain:005880:buy", redis.store)
        self.assertNotIn("uncertain_order_pending:O1", redis.store)

    async def test_extended_interval_150s_skips_early_poll(self):
        """연장 추적은 150초 간격 — 마지막 확인 후 150초가 안 지났으면 조회하지 않는다."""
        import time as _t
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(
                checkpoint_idx=n, elapsed=600, extra={"last_check_at": _t.time() - 100})),
        })
        calls = []

        async def get_positions():
            calls.append(1)
            return {"success": True, "data": []}

        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, FakePool(), get_positions, None, AsyncMock(), AsyncMock())
        self.assertEqual(calls, [])
        self.assertIn("order_uncertain:005880:buy", redis.store)  # 조회 안 해도 차단 키는 갱신 유지

        ctx = json.loads(redis.store["uncertain_order_pending:O1"])
        ctx["last_check_at"] = _t.time() - 151
        redis.store["uncertain_order_pending:O1"] = json.dumps(ctx)
        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, FakePool(), get_positions, None, AsyncMock(), AsyncMock())
        self.assertEqual(len(calls), 1)

    async def test_order_no_fill_qty_used_with_fill_price(self):
        """order_no가 있으면 inquire-daily-ccld 체결수량/평균가를 우선 사용한다(보유수량 조회 불필요)."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(
                checkpoint_idx=0, elapsed=31, qty=10, extra={"order_no": "O1"})),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool()
        positions_calls = []

        async def get_positions():
            positions_calls.append(1)
            return {"success": True, "data": []}

        async def get_filled(order_no, symbol, is_buy):
            self.assertEqual(order_no, "O1")
            return 10, 2140.0

        await uot.poll_uncertain_orders_once(
            redis, pool, get_positions, None, AsyncMock(), AsyncMock(), get_filled)

        self.assertEqual(positions_calls, [])
        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(pool._conn.inserted[0][4], 2140.0)  # price = 체결평균가
        self.assertEqual(pool._conn.inserted[0][5], 10.0)

    async def test_order_no_zero_falls_back_to_position_change(self):
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(
                checkpoint_idx=0, elapsed=31, qty=10, extra={"order_no": "O1"})),
        })
        pool = FakePool()

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 10, "avg_price": 2135}]}

        async def get_filled(order_no, symbol, is_buy):
            return 0, None

        await uot.poll_uncertain_orders_once(
            redis, pool, get_positions, None, AsyncMock(), AsyncMock(), get_filled)
        self.assertEqual(len(pool._conn.inserted), 1)

    async def test_final_check_query_failure_retries_then_alerts(self):
        """최종 확인 조회가 계속 실패하면 N회 재시도 후 수동 확인 알림으로 종료한다."""
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=n, elapsed=20000)),
            "order_uncertain:005880:buy": "1",
        })
        sent = []

        async def get_positions():
            raise ConnectionError("down")

        async def send_telegram(text):
            sent.append(text)

        with patch.object(uot, "_is_after_close", return_value=True):
            for _ in range(uot.UNCERTAIN_ORDER_FINAL_MAX_ATTEMPTS):
                await uot.poll_uncertain_orders_once(redis, FakePool(), get_positions, None, send_telegram, AsyncMock())
                if len(sent) == 0:
                    self.assertIn("uncertain_order_pending:O1", redis.store)
        self.assertEqual(len(sent), 1)
        self.assertIn("수동 확인 필요", sent[0])
        self.assertNotIn("uncertain_order_pending:O1", redis.store)
        self.assertNotIn("order_uncertain:005880:buy", redis.store)

    async def test_poll_refreshes_block_key_with_short_ttl_until_end(self):
        """추적 중에는 폴링마다 차단 키를 갱신한다(키가 180초 TTL로 만료돼도 추적이 살아있으면 복구)."""
        n = len(uot.UNCERTAIN_ORDER_CHECKPOINTS_SEC)
        import time as _t
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(
                checkpoint_idx=n, elapsed=900, extra={"last_check_at": _t.time()})),
        })
        with patch.object(uot, "_is_after_close", return_value=False):
            await uot.poll_uncertain_orders_once(redis, FakePool(), AsyncMock(), None, AsyncMock(), AsyncMock())
        self.assertIn("order_uncertain:005880:buy", redis.store)

    async def test_already_recorded_skips_duplicate_insert_but_clears_block(self):
        """같은 종목·방향·날짜·수량의 trade_history 행이 이미 있으면(정산 보정 등이 먼저
        기록) 중복 INSERT·알림 없이 차단 키만 해제하고 추적을 종료한다."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=1, elapsed=91)),
            "order_uncertain:005880:buy": "1",
        })
        pool = FakePool(fetchval_result=1)  # 이미 기록된 행이 있다고 응답
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 497, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 0)
        self.assertEqual(sent, [])
        self.assertNotIn("order_uncertain:005880:buy", redis.store)
        self.assertNotIn("uncertain_order_pending:O1", redis.store)

    async def test_position_fetch_failure_is_fail_safe_and_retries_same_checkpoint(self):
        """보유 수량 조회 자체가 실패하면(네트워크 오류 등) 체크포인트를 넘기지 않고
        다음 주기에 같은 체크포인트로 재시도한다."""
        redis = FakeRedis({
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=0, elapsed=31)),
        })
        pool = FakePool()

        async def get_positions():
            raise ConnectionError("조회 실패")

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, AsyncMock(), AsyncMock())

        ctx = json.loads(redis.store["uncertain_order_pending:O1"])
        self.assertEqual(ctx["checkpoint_idx"], 0)  # 그대로 유지(넘어가지 않음)
        self.assertEqual(len(pool._conn.inserted), 0)

    async def test_redis_scan_failure_does_not_crash(self):
        class RaisingScanRedis(FakeRedis):
            async def scan_iter(self, match="*"):
                raise ConnectionError("redis 조회 실패")
                yield  # pragma: no cover - 제너레이터로 만들기 위한 unreachable yield

        redis = RaisingScanRedis()
        await uot.poll_uncertain_orders_once(redis, FakePool(), AsyncMock(), None, AsyncMock(), AsyncMock())  # 예외 없이 반환돼야 함

    async def test_per_key_processing_error_does_not_abort_other_keys(self):
        """한 키 처리 중 오류가 나도 다른 키 처리는 계속돼야 한다."""
        redis = FakeRedis({
            "uncertain_order_pending:BAD": "not-json{{",
            "uncertain_order_pending:O1": json.dumps(_ctx(checkpoint_idx=1, elapsed=91)),
        })
        pool = FakePool()
        sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005880", "qty": 497, "avg_price": 2135}]}

        async def send_telegram(text):
            sent.append(text)

        await uot.poll_uncertain_orders_once(redis, pool, get_positions, None, send_telegram, AsyncMock())

        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertIn("uncertain_order_pending:BAD", redis.store)  # 손상된 항목은 그대로 남아있되 크래시 없음


class TestExecutionGuardRegistersBackgroundRecheck(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_status_with_bot_and_name_registers_tracker_entry(self):
        """bot/name을 넘기면 unknown으로 끝날 때 백그라운드 재확인이 실제로 등록된다."""
        from stark import execution_guard

        redis = FakeRedis()

        async def get_positions():
            return {"success": True, "data": []}  # pre_qty와 동일 — 변화 없음

        with patch("stark.execution_guard.asyncio.sleep", new=AsyncMock()):
            recon = await execution_guard.reconcile_uncertain_order(
                "005880", True, pre_qty=0.0, get_positions_fn=get_positions, redis=redis,
                bot="stock_trader", name="현대차", qty=497, price=2135, strategy="AI매수",
            )

        self.assertEqual(recon["status"], "unknown")
        keys = [k for k in redis.store if k.startswith(uot.UNCERTAIN_ORDER_KEY_PREFIX)]
        self.assertEqual(len(keys), 1)
        ctx = json.loads(redis.store[keys[0]])
        self.assertEqual(ctx["symbol"], "005880")
        self.assertEqual(ctx["qty"], 497)

    async def test_unknown_status_without_bot_or_name_does_not_register(self):
        """bot/name을 안 넘기는 기존 호출부는 등록되지 않는다(기존 동작 그대로)."""
        from stark import execution_guard

        redis = FakeRedis()

        async def get_positions():
            return {"success": True, "data": []}

        with patch("stark.execution_guard.asyncio.sleep", new=AsyncMock()):
            await execution_guard.reconcile_uncertain_order(
                "005880", True, pre_qty=0.0, get_positions_fn=get_positions, redis=redis,
            )

        keys = [k for k in redis.store if k.startswith(uot.UNCERTAIN_ORDER_KEY_PREFIX)]
        self.assertEqual(keys, [])


if __name__ == "__main__":
    unittest.main()
