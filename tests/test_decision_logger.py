"""
stark/decision_logger.py 왕복(round-trip) 무결성 테스트: log_decision → get_recent_decisions /
get_decisions_by_symbol.

한계: 이 작업 환경에는 asyncpg/postgres/docker/pip이 전혀 없어 실제 DB에 연결할 수
없었다. 대신 asyncpg.Pool/Connection의 인터페이스(async with pool.acquire() as conn,
conn.fetchval/conn.fetch)를 그대로 흉내 내는 인메모리 FakePool/FakeConnection을
사용해 decision_logger의 SQL 조립·인자 바인딩·반환값 처리 로직을 검증한다.
실제 PostgreSQL의 타입 강제, 제약조건(NOT NULL 등), 트랜잭션 동작까지는
검증하지 못한다.

FakeConnection은 asyncpg의 실제 jsonb 왕복 동작(인코딩 시 json.dumps된 str을 그대로
받고, 디코딩 시에도 str로 돌려줌 — 이 저장소의 다른 jsonb 컬럼들과 동일한 관례)을
흉내 내기 위해 features_json을 파싱하지 않고 str 그대로 저장한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark.decision_logger import (  # noqa: E402
    get_decisions_by_symbol,
    get_recent_decisions,
    log_decision,
)


class FakeConnection:
    """stark_decisions 테이블 하나만 흉내 내는 최소 asyncpg.Connection 대역."""

    def __init__(self, store: list, fail_on_insert: bool = False):
        self._store = store  # 공유 리스트(FakePool과 공유) — 각 원소는 dict
        self._fail_on_insert = fail_on_insert

    async def fetchval(self, query: str, *args):
        assert "INSERT INTO stark_decisions" in query
        assert "RETURNING id" in query
        if self._fail_on_insert:
            raise RuntimeError("시뮬레이션된 DB 오류")
        (symbol, name, decision, confidence, reason, rationale,
         strategy, source, model_name, executed, order_success,
         price, quantity, features_json) = args
        new_id = len(self._store) + 1
        row = {
            "id": new_id, "symbol": symbol, "name": name, "decision": decision,
            "confidence": confidence, "reason": reason, "rationale": rationale,
            "strategy": strategy, "source": source, "model_name": model_name,
            "executed": executed, "order_success": order_success,
            "price": price, "quantity": quantity,
            "features_json": features_json,  # asyncpg와 동일하게 str 그대로 저장
            "decided_at": new_id,  # 정렬만 확인하면 되므로 단순 증가값으로 대체
        }
        self._store.append(row)
        return new_id

    async def fetch(self, query: str, *args):
        rows = list(self._store)
        if "WHERE symbol=$1" in query:
            symbol = args[0]
            limit = args[1]
            rows = [r for r in rows if r["symbol"] == symbol]
        else:
            limit = args[0]
        rows.sort(key=lambda r: r["decided_at"], reverse=True)
        return rows[:limit]


class _AcquireCtx:
    """asyncpg pool.acquire()의 async context manager 대역.

    실제 asyncpg처럼 __aexit__에서 커넥션을 풀에 반환(release)한다 — INSERT 중
    예외가 발생해도 이 반환은 반드시 일어나야 커넥션 누수가 없다."""

    def __init__(self, pool: "FakePool", conn: FakeConnection):
        self._pool = pool
        self._conn = conn

    async def __aenter__(self):
        self._pool.acquired_count += 1
        return self._conn

    async def __aexit__(self, *exc):
        self._pool.released_count += 1
        return False


class FakePool:
    """asyncpg.Pool 대역: acquire()가 async context manager를 반환."""

    def __init__(self, fail_on_insert: bool = False):
        self.store = []
        self._conn = FakeConnection(self.store, fail_on_insert=fail_on_insert)
        self.acquired_count = 0
        self.released_count = 0

    def acquire(self):
        return _AcquireCtx(self, self._conn)


class TestDecisionLoggerRoundTrip(unittest.TestCase):
    def setUp(self):
        self.pool = FakePool()

    def test_log_and_get_recent_round_trip(self):
        async def scenario():
            did = await log_decision(
                self.pool, "005930", "BUY",
                name="삼성전자", confidence=0.82, reason="골든크로스+거래량급증",
                strategy="MA크로스", source="ai", model_name="gemini-2.5-flash",
                executed=True, order_success=True, price=74200, quantity=1,
            )
            self.assertIsNotNone(did)

            recent = await get_recent_decisions(self.pool, limit=10)
            self.assertEqual(len(recent), 1)
            row = recent[0]
            self.assertEqual(row["symbol"], "005930")
            self.assertEqual(row["decision"], "BUY")
            self.assertEqual(row["reason"], "골든크로스+거래량급증")
            self.assertTrue(row["executed"])
            return did

        asyncio.run(scenario())

    def test_skip_decision_is_logged_not_silently_dropped(self):
        """STARK_PLAN 원칙: SKIP/HOLD도 반드시 사유와 함께 기록되어야 함."""
        async def scenario():
            await log_decision(
                self.pool, "000660", "SKIP",
                reason="당일 손절 2회 — 신규 매수 전면 중단", source="rule",
            )
            recent = await get_recent_decisions(self.pool, limit=10)
            self.assertEqual(len(recent), 1)
            self.assertEqual(recent[0]["decision"], "SKIP")
            self.assertIn("손절", recent[0]["reason"])

        asyncio.run(scenario())

    def test_get_decisions_by_symbol_filters_and_orders_desc(self):
        async def scenario():
            await log_decision(self.pool, "005930", "BUY", reason="1차")
            await log_decision(self.pool, "000660", "HOLD", reason="다른 종목")
            await log_decision(self.pool, "005930", "SELL", reason="2차")

            rows = await get_decisions_by_symbol(self.pool, "005930", limit=10)
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(r["symbol"] == "005930" for r in rows))
            # 최신순(decided_at DESC) 정렬 확인 — 나중에 기록한 SELL이 먼저 나와야 함
            self.assertEqual(rows[0]["decision"], "SELL")
            self.assertEqual(rows[1]["decision"], "BUY")

        asyncio.run(scenario())

    def test_log_decision_returns_none_without_pool(self):
        async def scenario():
            result = await log_decision(None, "005930", "BUY")
            self.assertIsNone(result)

        asyncio.run(scenario())

    def test_features_json_round_trip(self):
        """features_json(JSONB) 직렬화 → 저장 → 조회 시 원래 dict로 역직렬화되는지 확인."""
        async def scenario():
            features = {"rsi": 32.5, "ma_cross": True, "volume_ratio": 2.1}
            did = await log_decision(
                self.pool, "005930", "BUY", reason="RSI 과매도 반등",
                features=features,
            )
            self.assertIsNotNone(did)

            # FakeConnection은 asyncpg처럼 문자열 그대로 저장하므로, 여기서
            # str로 들어갔는지도 함께 확인해 실제 인코딩 경로를 검증한다.
            self.assertIsInstance(self.pool.store[0]["features_json"], str)

            recent = await get_recent_decisions(self.pool, limit=10)
            self.assertEqual(recent[0]["features_json"], features)

            by_symbol = await get_decisions_by_symbol(self.pool, "005930", limit=10)
            self.assertEqual(by_symbol[0]["features_json"], features)

        asyncio.run(scenario())

    def test_features_json_none_when_not_provided(self):
        async def scenario():
            await log_decision(self.pool, "005930", "HOLD", reason="관망")
            recent = await get_recent_decisions(self.pool, limit=10)
            self.assertIsNone(recent[0]["features_json"])

        asyncio.run(scenario())

    def test_log_decision_rejects_missing_required_columns(self):
        """symbol/decision은 stark_decisions에서 NOT NULL — 비어 있으면 DB 왕복 없이 None."""
        async def scenario():
            result_empty_symbol = await log_decision(self.pool, "", "BUY")
            result_empty_decision = await log_decision(self.pool, "005930", "")
            self.assertIsNone(result_empty_symbol)
            self.assertIsNone(result_empty_decision)
            # 유효하지 않은 호출은 INSERT 자체를 시도하지 않아야 함
            self.assertEqual(len(self.pool.store), 0)

        asyncio.run(scenario())

    def test_connection_is_released_even_when_insert_raises(self):
        """INSERT 중 예외가 나도 pool.acquire()의 async with가 커넥션을 반드시 반환해야 함
        (판단 로그 기록 실패가 커넥션 누수로 이어지면 안 됨)."""
        async def scenario():
            failing_pool = FakePool(fail_on_insert=True)
            result = await log_decision(failing_pool, "005930", "BUY")
            self.assertIsNone(result)
            self.assertEqual(failing_pool.acquired_count, 1)
            self.assertEqual(failing_pool.released_count, 1)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
