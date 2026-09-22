"""
STARK v2 DB 마이그레이션 정본 러너.

스키마 정본은 이 저장소의 migrations/*.sql 파일들이다 (V001, V002, ... 순차 적용).
각 파일은 "-- Up" / "-- Down" 섹션을 명확히 구분해서 담고 있으며,
이 모듈은 그중 "-- Up" 섹션만 순번대로 파싱·실행한다 (Down은 수동 롤백용으로,
런타임 자동 실행 대상이 아니다).

실제 적용 이력은 `schema_migrations` 테이블에 기록해 같은 마이그레이션이
두 번 실행되지 않도록 한다 (V002처럼 데이터 이관 + DROP TABLE을 포함하는
파일은 재실행하면 실패하거나 데이터가 유실될 수 있기 때문).
"""
import logging
import re
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

_UP_RE = re.compile(r"^--\s*Up\s*$", re.IGNORECASE | re.MULTILINE)
_DOWN_RE = re.compile(r"^--\s*Down\s*$", re.IGNORECASE | re.MULTILINE)


def extract_up_sql(text: str) -> str:
    """마이그레이션 파일 텍스트에서 '-- Up' 섹션(‘-- Down’ 이전까지)만 추출."""
    up_match = _UP_RE.search(text)
    if not up_match:
        return ""
    start = up_match.end()
    down_match = _DOWN_RE.search(text, pos=start)
    end = down_match.start() if down_match else len(text)
    return text[start:end].strip()


def extract_down_sql(text: str) -> str:
    """마이그레이션 파일 텍스트에서 '-- Down' 섹션(파일 끝까지)만 추출."""
    down_match = _DOWN_RE.search(text)
    if not down_match:
        return ""
    return text[down_match.end():].strip()


def list_migration_files() -> List[Path]:
    """migrations/ 디렉터리의 V*.sql 파일을 버전 순으로 정렬해 반환."""
    if not MIGRATIONS_DIR.is_dir():
        return []
    return sorted(MIGRATIONS_DIR.glob("V*.sql"), key=lambda p: p.name)


def load_migrations() -> List[Tuple[str, str, str]]:
    """(version, up_sql, down_sql) 튜플 목록을 버전 순으로 반환."""
    result = []
    for path in list_migration_files():
        text = path.read_text(encoding="utf-8")
        version = path.stem  # e.g. "V001__baseline_tables"
        result.append((version, extract_up_sql(text), extract_down_sql(text)))
    return result


async def run_migrations(pool) -> List[str]:
    """아직 적용되지 않은 마이그레이션의 Up 섹션을 버전 순서대로 실행.

    반환값: 이번 호출에서 새로 적용된 version 목록.
    """
    applied_now: List[str] = []
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        already_applied = {
            r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")
        }
        for version, up_sql, _down_sql in load_migrations():
            if version in already_applied:
                continue
            if not up_sql:
                logger.warning(f"마이그레이션 {version}: Up 섹션이 비어있어 건너뜀")
                continue
            async with conn.transaction():
                await conn.execute(up_sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1)", version
                )
            logger.info(f"✅ 마이그레이션 적용: {version}")
            applied_now.append(version)
    return applied_now
