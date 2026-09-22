"""
migrations/*.sql 구조 검증 + Up/Down 왕복(round-trip) 시뮬레이션 테스트.

한계(중요): 이 테스트는 실제 PostgreSQL에 연결해서 SQL을 실행하지 않는다.
이 작업 환경에는 docker/postgres/asyncpg/pip이 전혀 없어서(설치 불가) 실제 DB
검증이 불가능했다. 대신 각 마이그레이션 파일의 "-- Up"/"-- Down" 섹션을 텍스트로
파싱해 CREATE/DROP TABLE·INDEX 문을 추출하고, 그 결과로 만들어지는 "가상 스키마
상태(테이블/인덱스 이름 집합)"가 Up을 순서대로 적용했을 때의 최종 상태와 Down을
역순으로 적용했을 때 원래(빈) 상태로 정확히 돌아오는지를 검증한다.
이는 SQL 문법 오류, 실제 제약조건 충돌(FK, 타입 불일치 등), 데이터 이관(INSERT
... SELECT)의 실제 동작까지는 검증하지 못한다 — 구조적 대칭성(파일마다 만든
테이블/인덱스를 빠짐없이 되돌리는지)만 보장한다.
"""
import re
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.migrations import (  # noqa: E402
    MIGRATIONS_DIR,
    extract_down_sql,
    extract_up_sql,
    list_migration_files,
    load_migrations,
)

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE
)
DROP_TABLE_RE = re.compile(
    r"DROP TABLE(?:\s+IF EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE
)
CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+IF NOT EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)
DROP_INDEX_RE = re.compile(
    r"DROP INDEX(?:\s+IF EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE
)


def apply_sql_to_state(sql: str, tables: set, indexes: set) -> None:
    """SQL 텍스트를 파싱해 가상 스키마 상태(tables/indexes 집합)를 갱신한다.
    실제 SQL 실행이 아니라 CREATE/DROP 문의 대상 이름만 추적하는 구조적 시뮬레이션."""
    for m in CREATE_TABLE_RE.finditer(sql):
        tables.add(m.group(1).lower())
    for m in DROP_TABLE_RE.finditer(sql):
        tables.discard(m.group(1).lower())
    for m in CREATE_INDEX_RE.finditer(sql):
        indexes.add(m.group(1).lower())
    for m in DROP_INDEX_RE.finditer(sql):
        indexes.discard(m.group(1).lower())


class TestMigrationFilesStructure(unittest.TestCase):
    def test_migrations_dir_has_expected_files(self):
        files = list_migration_files()
        names = [p.name for p in files]
        self.assertEqual(
            names,
            [
                "V001__baseline_tables.sql",
                "V002__create_stocks_and_migrate.sql",
                "V003__create_stark_decisions.sql",
                "V004__add_indexes.sql",
            ],
            "migrations/ 파일 구성이 예상과 다름 (버전 순 정렬 포함)",
        )

    def test_each_file_has_nonempty_up_and_down(self):
        for path in list_migration_files():
            text = path.read_text(encoding="utf-8")
            up = extract_up_sql(text)
            down = extract_down_sql(text)
            self.assertTrue(up.strip(), f"{path.name}: Up 섹션이 비어있음")
            self.assertTrue(down.strip(), f"{path.name}: Down 섹션이 비어있음")

    def test_v003_uses_decided_at_column(self):
        text = (MIGRATIONS_DIR / "V003__create_stark_decisions.sql").read_text(encoding="utf-8")
        up = extract_up_sql(text)
        self.assertIn(
            "decided_at", up.lower(),
            "V003의 stark_decisions 테이블은 판단시간 컬럼명으로 decided_at을 반드시 사용해야 함",
        )

    def test_v001_contains_all_18_canonical_tables(self):
        """database.py(12) + dashboard/main.py(중복 제외 6) = 18개 테이블이 V001에 모두 있어야 함."""
        expected_tables = {
            # database.py 전용
            "stock_ohlcv", "trade_history", "balance_snapshot", "watchlist",
            "stock_supply", "stock_disclosure", "jarvis_memory", "stock_news_sentiment",
            # database.py / main.py 공통(중복 정본화)
            "stock_daily_ohlcv", "stock_indicators", "ml_predictions", "strategy_config",
            # main.py 전용
            "stock_master", "ml_models", "jarvis_notes", "principle_stats",
            "notifications", "trade_journal",
        }
        text = (MIGRATIONS_DIR / "V001__baseline_tables.sql").read_text(encoding="utf-8")
        up = extract_up_sql(text)
        tables, indexes = set(), set()
        apply_sql_to_state(up, tables, indexes)
        self.assertEqual(tables, expected_tables)


class TestMigrationRoundTrip(unittest.TestCase):
    """Up을 V001→V004 순서로 전부 적용한 뒤, Down을 V004→V001 역순으로 적용하면
    가상 스키마 상태(테이블/인덱스 이름 집합)가 정확히 시작 상태(빈 집합)로
    복귀하는지 검증한다."""

    def setUp(self):
        self.migrations = load_migrations()
        self.assertEqual(len(self.migrations), 4, "마이그레이션 파일 4개가 모두 로드되어야 함")

    def test_up_then_down_round_trip_restores_empty_state(self):
        tables, indexes = set(), set()
        initial = (frozenset(tables), frozenset(indexes))

        # Up: V001 -> V004 순서
        for version, up_sql, _down_sql in self.migrations:
            apply_sql_to_state(up_sql, tables, indexes)

        # 중간 상태 확인: 모든 up 적용 후 stock_master는 없고 stocks/stark_decisions는 있어야 함
        self.assertNotIn("stock_master", tables)
        self.assertIn("stocks", tables)
        self.assertIn("stark_decisions", tables)
        self.assertIn("idx_stocks_symbol_name", indexes)
        self.assertIn("idx_stark_decisions_decided_at_symbol", indexes)

        # Down: V004 -> V001 역순
        for version, _up_sql, down_sql in reversed(self.migrations):
            apply_sql_to_state(down_sql, tables, indexes)

        self.assertEqual(
            (frozenset(tables), frozenset(indexes)), initial,
            "Up 전체 적용 후 Down을 역순으로 전부 적용하면 스키마가 원상복구되어야 함",
        )

    def test_v002_stock_master_to_stocks_swap_is_symmetric(self):
        """V002 하나만 놓고 봐도 up/down이 stock_master<->stocks를 정확히 뒤집는지 확인."""
        _version, up_sql, down_sql = next(
            m for m in self.migrations if m[0].startswith("V002")
        )
        tables, indexes = {"stock_master"}, set()
        apply_sql_to_state(up_sql, tables, indexes)
        self.assertEqual(tables, {"stocks"})

        apply_sql_to_state(down_sql, tables, indexes)
        self.assertEqual(tables, {"stock_master"})


if __name__ == "__main__":
    unittest.main()
