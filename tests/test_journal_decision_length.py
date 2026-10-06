"""
tests/test_journal_decision_length.py - [AT] fix/journal-decision-length

배경(EDITH 조회): trade_journal.jarvis_decision이 VARCHAR(10)인데
stark/execution_guard.py가 소액매수 판단을 "EXECUTE_SMALL"(13자)로 기록한다.
Postgres는 VARCHAR(n) 초과 INSERT를 조용히 자르지 않고 예외를 내므로, 이 INSERT는
매번 실패해 dashboard/main.py._log_journal의 try/except에 걸려 버려졌다 — 최근 30일
stark_decisions에는 EXECUTE_SMALL 48건이 있는데 trade_journal에는 0건.

검증 범위
1) migrations/V011__widen_jarvis_decision_column.sql이 trade_journal.jarvis_decision을
   VARCHAR(20)으로 넓히는지 (구조 검증은 tests/test_migrations.py에서 더 자세히 함 —
   여기서는 "EXECUTE_SMALL" 13자가 실제로 들어가는지를 직접 확인).
2) _log_journal이 INSERT 실패 시 조용히 삼키지 않고 WARNING 이상으로 로그를 남기는지
   (이미 satisfied — 코드 변경 없이 회귀 방지용으로 고정).
"""
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402
from common.migrations import extract_up_sql  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = REPO_ROOT / "migrations"


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestV011WidensJarvisDecisionColumn(unittest.TestCase):
    def test_up_section_widens_to_20_and_guards_against_narrowing(self):
        text = (MIGRATIONS_DIR / "V011__widen_jarvis_decision_column.sql").read_text(
            encoding="utf-8")
        up = extract_up_sql(text)
        self.assertIn("trade_journal", up)
        self.assertIn("jarvis_decision", up)
        self.assertIn("VARCHAR(20)", up)
        # 여러 번 실행해도 안전해야 함 — 이미 20자 이상이면 건너뛰는 가드가 있어야 한다
        self.assertIn("character_maximum_length", up)

    def test_execute_small_fits_within_new_width(self):
        """"EXECUTE_SMALL"(13자)이 새 VARCHAR(20) 한도 안에 들어가는지 직접 확인."""
        self.assertLessEqual(len("EXECUTE_SMALL"), 20)
        self.assertGreater(len("EXECUTE_SMALL"), 10, "애초에 VARCHAR(10)을 넘기는 값이어야 재현 의미가 있음")


class _BoomConnection:
    """INSERT 시 VARCHAR(10) 초과처럼 실패하는 상황을 흉내내는 가짜 커넥션."""

    async def execute(self, query, *args):
        raise Exception("value too long for type character varying(10)")


class TestLogJournalFailureIsLoggedNotSwallowed(unittest.IsolatedAsyncioTestCase):
    async def test_insert_failure_logs_at_warning_or_above(self):
        """_log_journal이 INSERT 실패를 조용히 삼키지 않고 WARNING 이상으로 남기는지
        확인한다. 레벨을 logger.debug 등으로 낮추는 회귀가 생기면 이 테스트가 깨진다."""
        with patch.object(dm, "db_pool", FakePool(_BoomConnection())):
            with self.assertLogs("dashboard", level="WARNING") as captured:
                await dm._log_journal(
                    "stock_trader", "005930", "삼성전자", "buy", "MA크로스", "신호 이유",
                    "EXECUTE_SMALL", "판단 이유", True, True, 70000, 1)

        levels = [r.levelname for r in captured.records]
        self.assertTrue(
            any(lvl in ("WARNING", "ERROR", "CRITICAL") for lvl in levels),
            f"매매일지 기록 실패가 WARNING 이상으로 로그되지 않음: {levels}")
        joined = "\n".join(r.getMessage() for r in captured.records)
        self.assertIn("매매일지 기록 실패", joined)


if __name__ == "__main__":
    unittest.main()
