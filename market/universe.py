"""
market/universe.py — 전종목 심볼 조회기 (Universe).

dashboard/main.py의 _resolve_stock_symbol에 있던 종목 식별 로직을 캡슐화해
이관했다. 원본은 6자리 코드가 아닌 경우 watchlist 테이블까지 뒤져 이름을
맞췄으나, watchlist(감시종목)는 사용자가 고른 부분집합일 뿐 종목 마스터가
아니므로 그 의존을 제거했다 — 이제 (1) 6자리 코드 정규식, (2) 인메모리
캐시(name_cache/code_cache), (3) stocks 테이블(정본, market/sync_batch.py가
적재) 순으로만 조회한다.

db_pool 주입 방식은 ml/model.py의 MLModelManager와 동일하게 생성자에서
받는다. 다만 MLModelManager와 달리 Universe는 캐시를 요청 사이에 계속
들고 있어야 하므로, dashboard/main.py에서는 startup() 시점에 한 번만
생성해 전역으로 공유한다.
"""
import logging
import re
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class Universe:
    """종목 코드↔이름 조회기. 인메모리 캐시 우선, DB(stocks)는 폴백/보강용."""

    def __init__(self, db_pool: Any = None):
        self.db_pool = db_pool
        self.name_cache: Dict[str, str] = {}  # {종목명: 종목코드}
        self.code_cache: Dict[str, str] = {}  # {종목코드: 종목명}

    def replace_cache(self, name_to_code: Dict[str, str]) -> None:
        """캐시 전체 교체 — market/sync_batch.py의 동기화 배치가 호출한다."""
        self.name_cache = dict(name_to_code)
        self.code_cache = {v: k for k, v in name_to_code.items()}

    def code_to_name_sync(self, code: str) -> str:
        """메모리 캐시만으로 즉시 조회 (동기, DB 접근 없음) — 실패 시 code 그대로 반환.
        급하지 않은 표시용(로그, 텔레그램 메시지 등)에 사용. 확실한 이름이 필요하면 code_to_name 사용."""
        return self.code_cache.get(code) or code

    async def code_to_name(self, code: str) -> str:
        """종목코드→이름, 메모리 캐시 실패 시 stocks DB까지 확인하는 완전한 조회."""
        name = self.code_cache.get(code)
        if name:
            return name
        if self.db_pool is None:
            return code
        try:
            async with self.db_pool.acquire() as conn:
                name = await conn.fetchval("SELECT name FROM stocks WHERE symbol=$1", code)
        except Exception:
            name = None
        return name or code

    async def resolve_symbol(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """메시지에서 종목 식별 → (symbol, name). 실패 시 (None, None).
        watchlist에는 의존하지 않는다 — stocks가 정본 종목 마스터다."""
        m = re.search(r"\b(\d{6})\b", text)
        if m:
            code = m.group(1)
            name = self.code_cache.get(code)
            if not name and self.db_pool is not None:
                try:
                    async with self.db_pool.acquire() as conn:
                        name = await conn.fetchval("SELECT name FROM stocks WHERE symbol=$1", code)
                except Exception:
                    name = None
            return code, (name or code)

        # 이름으로 찾기: 인메모리 캐시
        for name, code in self.name_cache.items():
            if name and name in text:
                return code, name

        # DB 폴백 (캐시 미로드 시): 텍스트에 포함되는 가장 긴 종목명
        if self.db_pool is not None:
            try:
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT symbol, name FROM stocks WHERE $1 LIKE '%' || name || '%' "
                        "ORDER BY LENGTH(name) DESC LIMIT 1", text)
                if rows:
                    return rows[0]["symbol"], rows[0]["name"]
            except Exception:
                pass
        return None, None
