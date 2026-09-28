"""
V009 (data_baseline + *_reliable 뷰) 검증 테스트.

한계(중요): 이 작업 환경에는 PostgreSQL/asyncpg가 없어서 실제 PostgreSQL에서는
실행하지 못했다. 대신 V009의 Up/Down SQL을 그대로 파싱해 SQLite(in-memory)에서
실행한다. 이때 PostgreSQL 전용 표기 세 가지만 SQLite용으로 치환한다:
  - CREATE OR REPLACE VIEW -> CREATE VIEW
  - TIMESTAMPTZ 타입명    -> TEXT
  - 타임존 오프셋이 붙은 시각 리터럴('YYYY-MM-DD HH:MM:SS+09') -> UTC로 정규화한 문자열
SQLite에는 timestamptz가 없으므로 테스트 데이터의 시각도 모두 UTC 'YYYY-MM-DD HH:MM:SS'
문자열로 넣어 사전순 비교가 시각 순서와 일치하게 한다. 따라서 이 테스트가 보장하는 것은
"뷰 로직(경계 포함, NULL 제외, 기준선 행 없음 -> 빈 결과, 원본 데이터 보존, Up/Down/Up
왕복)"이고, PostgreSQL의 timestamptz 파싱·CREATE OR REPLACE VIEW 문법 자체는 검증하지 못한다.
"""
import re
import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.migrations import (  # noqa: E402
    MIGRATIONS_DIR,
    extract_down_sql,
    extract_up_sql,
)

V009_PATH = MIGRATIONS_DIR / "V009__data_baseline_and_reliable_views.sql"
BASELINE_KEY = "reliable_trading_data_from"
BASELINE_UTC = "2026-09-28 02:00:00"  # 2026-09-28 11:00:00+09

_TZ_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})([+-]\d{2})'")


def _to_utc_text(match: "re.Match") -> str:
    naive = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    offset = timedelta(hours=int(match.group(2)))
    utc = (naive - offset).replace(tzinfo=timezone.utc)
    return "'" + utc.strftime("%Y-%m-%d %H:%M:%S") + "'"


def to_sqlite(sql: str) -> str:
    """PostgreSQL 전용 표기를 SQLite에서 실행 가능한 형태로 치환 (모듈 docstring 참고)."""
    sql = sql.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
    sql = re.sub(r"\bTIMESTAMPTZ\b", "TEXT", sql)
    return _TZ_LITERAL_RE.sub(_to_utc_text, sql)


def make_db() -> sqlite3.Connection:
    """원본 테이블(V001 trade_history, V003 stark_decisions)의 관련 컬럼만 흉내낸 DB."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE trade_history (
            id INTEGER PRIMARY KEY, bot TEXT NOT NULL, symbol TEXT NOT NULL,
            side TEXT NOT NULL, pnl REAL, ts TEXT
        );
        CREATE TABLE stark_decisions (
            id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
            decision TEXT NOT NULL, decided_at TEXT
        );
        """
    )
    return conn


def up(conn: sqlite3.Connection) -> None:
    conn.executescript(to_sqlite(extract_up_sql(V009_PATH.read_text(encoding="utf-8"))))


def down(conn: sqlite3.Connection) -> None:
    conn.executescript(to_sqlite(extract_down_sql(V009_PATH.read_text(encoding="utf-8"))))


def objects(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")
    return {r[0] for r in rows}


class TestV009Structure(unittest.TestCase):
    def setUp(self):
        text = V009_PATH.read_text(encoding="utf-8")
        self.up_sql = extract_up_sql(text)
        self.down_sql = extract_down_sql(text)

    def test_baseline_literal_is_2026_09_28_11_kst(self):
        self.assertIn("'2026-09-28 11:00:00+09'", self.up_sql)
        self.assertIn(f"'{BASELINE_KEY}'", self.up_sql)
        # KST 11:00 == UTC 02:00
        self.assertEqual(
            _to_utc_text(_TZ_LITERAL_RE.search("'2026-09-28 11:00:00+09'")),
            f"'{BASELINE_UTC}'",
        )

    def test_up_does_not_touch_existing_data_or_structure(self):
        up_lower = self.up_sql.lower()
        for forbidden in ("alter table", "delete from", "update ", "drop ", "truncate"):
            self.assertNotIn(forbidden, up_lower, f"V009 Up에 금지 구문: {forbidden}")

    def test_up_has_no_account_number(self):
        # 계좌번호 형태(8자리-2자리 등 긴 숫자열)가 SQL에 들어가면 안 된다.
        self.assertIsNone(re.search(r"\d{8}", self.up_sql))
        self.assertNotIn("account", self.up_sql.lower())

    def test_down_drops_only_objects_created_by_up(self):
        down_lower = self.down_sql.lower()
        self.assertIn("drop view if exists stark_decisions_reliable", down_lower)
        self.assertIn("drop view if exists trade_history_reliable", down_lower)
        self.assertIn("drop table if exists data_baseline", down_lower)
        # 뷰를 테이블보다 먼저 지워야 한다 (뷰가 data_baseline에 의존)
        self.assertLess(
            down_lower.index("drop view if exists trade_history_reliable"),
            down_lower.index("drop table if exists data_baseline"),
        )
        self.assertNotIn("drop table if exists trade_history", down_lower)
        self.assertNotIn("drop table if exists stark_decisions", down_lower)


class TestV009RoundTrip(unittest.TestCase):
    def test_up_down_up(self):
        conn = make_db()
        base = objects(conn)
        conn.execute("INSERT INTO trade_history (bot, symbol, side, ts) VALUES ('stock','005930','buy','2026-09-01 00:00:00')")
        conn.execute("INSERT INTO stark_decisions (symbol, decision, decided_at) VALUES ('005930','HOLD','2026-09-01 00:00:00')")

        up(conn)
        self.assertEqual(
            objects(conn) - base,
            {"data_baseline", "trade_history_reliable", "stark_decisions_reliable"},
        )
        rows = conn.execute("SELECT key, effective_at, note FROM data_baseline").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], BASELINE_KEY)
        self.assertEqual(rows[0][1], BASELINE_UTC)
        self.assertTrue(rows[0][2])

        down(conn)
        self.assertEqual(objects(conn), base, "Down 후 원래 상태로 복귀해야 함")
        # 기존 데이터는 Up/Down을 거쳐도 삭제되지 않는다
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM trade_history").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM stark_decisions").fetchone()[0], 1)

        up(conn)
        self.assertEqual(
            objects(conn) - base,
            {"data_baseline", "trade_history_reliable", "stark_decisions_reliable"},
        )
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM data_baseline").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM trade_history").fetchone()[0], 1)

    def test_up_is_idempotent_for_baseline_row(self):
        """INSERT ... ON CONFLICT DO NOTHING: 기준선 행이 이미 있으면 중복/덮어쓰기 없음."""
        conn = make_db()
        up(conn)
        conn.execute("UPDATE data_baseline SET note='수동 수정' WHERE key=?", (BASELINE_KEY,))
        conn.executescript(
            to_sqlite(
                "INSERT INTO data_baseline (key, effective_at, note) "
                "VALUES ('reliable_trading_data_from', '2026-09-28 11:00:00+09', 'x') "
                "ON CONFLICT (key) DO NOTHING;"
            )
        )
        row = conn.execute("SELECT COUNT(*), MAX(note) FROM data_baseline").fetchone()
        self.assertEqual(row, (1, "수동 수정"))


class TestReliableViews(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()
        up(self.conn)
        trades = [
            ("before_1s", "2026-09-28 01:59:59"),   # 기준선 1초 전 -> 제외
            ("on_boundary", BASELINE_UTC),          # 경계(==) -> 포함
            ("after_1s", "2026-09-28 02:00:01"),    # 기준선 1초 후 -> 포함
            ("old", "2026-09-01 00:00:00"),         # 옛 데이터 -> 제외
            ("future", "2026-10-15 05:00:00"),      # 이후 데이터 -> 포함
            ("null_ts", None),                      # 시각 NULL -> 제외
        ]
        for sym, ts in trades:
            self.conn.execute(
                "INSERT INTO trade_history (bot, symbol, side, pnl, ts) VALUES ('stock', ?, 'sell', 1.0, ?)",
                (sym, ts),
            )
            self.conn.execute(
                "INSERT INTO stark_decisions (symbol, decision, decided_at) VALUES (?, 'HOLD', ?)",
                (sym, ts),
            )

    def symbols(self, view: str) -> set:
        return {r[0] for r in self.conn.execute(f"SELECT symbol FROM {view}")}

    def test_trade_history_reliable_splits_at_baseline_inclusive(self):
        self.assertEqual(
            self.symbols("trade_history_reliable"), {"on_boundary", "after_1s", "future"}
        )

    def test_stark_decisions_reliable_splits_at_baseline_inclusive(self):
        self.assertEqual(
            self.symbols("stark_decisions_reliable"), {"on_boundary", "after_1s", "future"}
        )

    def test_null_time_rows_excluded_but_kept_in_base_tables(self):
        self.assertNotIn("null_ts", self.symbols("trade_history_reliable"))
        self.assertNotIn("null_ts", self.symbols("stark_decisions_reliable"))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM trade_history").fetchone()[0], 6)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM stark_decisions").fetchone()[0], 6)

    def test_missing_baseline_row_yields_empty_views(self):
        self.conn.execute("DELETE FROM data_baseline WHERE key=?", (BASELINE_KEY,))
        self.assertEqual(self.symbols("trade_history_reliable"), set())
        self.assertEqual(self.symbols("stark_decisions_reliable"), set())

    def test_other_baseline_keys_do_not_leak_into_views(self):
        self.conn.execute("DELETE FROM data_baseline WHERE key=?", (BASELINE_KEY,))
        self.conn.execute(
            "INSERT INTO data_baseline (key, effective_at) VALUES ('other', '2000-01-01 00:00:00')"
        )
        self.assertEqual(self.symbols("trade_history_reliable"), set())

    def test_moving_baseline_moves_view_cutoff(self):
        self.conn.execute(
            "UPDATE data_baseline SET effective_at='2026-10-01 00:00:00' WHERE key=?",
            (BASELINE_KEY,),
        )
        self.assertEqual(self.symbols("trade_history_reliable"), {"future"})

    def test_view_exposes_original_columns(self):
        cols = [d[0] for d in self.conn.execute("SELECT * FROM trade_history_reliable").description]
        self.assertEqual(cols, ["id", "bot", "symbol", "side", "pnl", "ts"])


if __name__ == "__main__":
    unittest.main()
