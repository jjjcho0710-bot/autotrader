"""
stark/decision_logger.py 왕복(round-trip) 무결성 테스트: log_decision → get_recent_decisions /
get_decisions_by_symbol.

한계: 이 작업 환경에는 asyncpg/postgres/docker/pip이 전혀 없어 실제 DB에 연결할 수
없었다. 대신 asyncpg.Pool/Connection의 인터페이스(async with pool.acquire() as conn,
conn.fetchval/conn.fetch)를 그대로 흉내 내는 인메모리 FakePool/FakeConnection을
사용해 decision_logger의 SQL 조립·인자 바인딩·반환값 처리 로직을 검증한다.
실제 PostgreSQL의 타입 강제, 제약조건(NOT NULL 등), 트랜잭션 동작까지는
검증하지 못한다.
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

    def __init__(self, store: list):
        self._store = store  # 공유 리스트(FakePool과 공유) — 각 원소는 dict

    async def fetchval(self, query: str, *args):
        assert "INSERT INTO stark_decisions" in query
        assert "RETURNING id" in query
        (symbol, name, decision, confidence, reason, rationale,
         strategy, source, model_name, executed, order_success,
         price, quantity) = args
        new_id = len(self._store) + 1
        row = {
            "id": new_id, "symbol": symbol, "name": name, "decision": decision,
            "confidence": confidence, "reason": reason, "rationale": rationale,
            "strategy": strategy, "source": source, "model_name": model_name,
            "executed": executed, "order_success": order_success,
            "price": price, "quantity": quantity,
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
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    """asyncpg.Pool 대역: acquire()가 async context manager를 반환."""

    def __init__(self):
        self.store = []
        self._conn = FakeConnection(self.store)

    def acquire(self):
        return _AcquireCtx(self._conn)


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


if __name__ == "__main__":
    unittest.main()
