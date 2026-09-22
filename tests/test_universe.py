"""
market/universe.py 단위 테스트: resolve_symbol의 조회 우선순위(6자리 코드 →
인메모리 캐시 → stocks DB)와, watchlist 테이블 의존을 제거했는지를 검증한다.

한계: 이 작업 환경에는 asyncpg/postgres/docker/pip이 전혀 없어 실제 DB에
연결할 수 없었다. tests/test_decision_logger.py와 동일한 방식으로
asyncpg.Pool/Connection 인터페이스(async with pool.acquire() as conn,
conn.fetchval/conn.fetch)를 흉내 내는 인메모리 FakePool/FakeConnection을
사용한다. FakeConnection은 stocks 테이블만 흉내 내며, watchlist를 언급하는
쿼리가 들어오면 즉시 assert 실패시켜 watchlist 의존이 재도입되지 않았는지도
함께 검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeStocksConnection:
    """stocks 테이블만 흉내 내는 최소 asyncpg.Connection 대역."""

    def __init__(self, rows):
        self.rows = rows  # [{"symbol": ..., "name": ...}, ...]

    async def fetchval(self, query: str, *args):
        assert "watchlist" not in query.lower(), f"watchlist 의존 금지: {query}"
        assert "stocks" in query.lower()
        code = args[0]
        for r in self.rows:
            if r["symbol"] == code:
                return r["name"]
        return None

    async def fetch(self, query: str, *args):
        assert "watchlist" not in query.lower(), f"watchlist 의존 금지: {query}"
        assert "stocks" in query.lower()
        text = args[0]
        matches = [r for r in self.rows if r["name"] in text]
        matches.sort(key=lambda r: len(r["name"]), reverse=True)
        return matches[:1]


class FakePool:
    def __init__(self, rows):
        self._conn = FakeStocksConnection(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestResolveSymbolSixDigitCode(unittest.TestCase):
    def test_hits_memory_cache_without_touching_db(self):
        """캐시에 있으면 DB 조회 없이 즉시 반환되어야 한다."""
        pool = FakePool([])  # DB가 비어 있어도 캐시로 해결되어야 함
        universe = Universe(pool)
        universe.replace_cache({"삼성전자": "005930"})

        symbol, name = asyncio.run(universe.resolve_symbol("005930 2주 매수"))
        self.assertEqual(symbol, "005930")
        self.assertEqual(name, "삼성전자")

    def test_falls_back_to_stocks_db_on_cache_miss(self):
        pool = FakePool([{"symbol": "000660", "name": "SK하이닉스"}])
        universe = Universe(pool)  # 캐시 비어있음

        symbol, name = asyncio.run(universe.resolve_symbol("000660 전량 매도"))
        self.assertEqual(symbol, "000660")
        self.assertEqual(name, "SK하이닉스")

    def test_unknown_code_returns_code_as_name(self):
        pool = FakePool([])
        universe = Universe(pool)

        symbol, name = asyncio.run(universe.resolve_symbol("999999 1주 매수"))
        self.assertEqual(symbol, "999999")
        self.assertEqual(name, "999999")


class TestResolveSymbolByName(unittest.TestCase):
    def test_name_in_text_matches_memory_cache(self):
        pool = FakePool([])
        universe = Universe(pool)
        universe.replace_cache({"삼성전자": "005930", "카카오": "035720"})

        symbol, name = asyncio.run(universe.resolve_symbol("삼성전자 3주 매수해줘"))
        self.assertEqual(symbol, "005930")
        self.assertEqual(name, "삼성전자")

    def test_name_falls_back_to_stocks_db_longest_match(self):
        pool = FakePool([
            {"symbol": "005930", "name": "삼성전자"},
            {"symbol": "005935", "name": "삼성전자우"},
        ])
        universe = Universe(pool)  # 캐시 비어있음 → DB 폴백

        symbol, name = asyncio.run(universe.resolve_symbol("삼성전자우 어때?"))
        self.assertEqual(symbol, "005935")
        self.assertEqual(name, "삼성전자우")

    def test_no_match_returns_none_none(self):
        pool = FakePool([])
        universe = Universe(pool)

        symbol, name = asyncio.run(universe.resolve_symbol("오늘 날씨 어때?"))
        self.assertIsNone(symbol)
        self.assertIsNone(name)


class TestUniverseCache(unittest.TestCase):
    def test_replace_cache_builds_both_directions(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930", "카카오": "035720"})
        self.assertEqual(universe.code_cache["005930"], "삼성전자")
        self.assertEqual(universe.name_cache["카카오"], "035720")

    def test_code_to_name_sync_uses_cache_only(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        self.assertEqual(universe.code_to_name_sync("005930"), "삼성전자")
        self.assertEqual(universe.code_to_name_sync("999999"), "999999")  # 미등록 → code 그대로

    def test_code_to_name_falls_back_to_db(self):
        pool = FakePool([{"symbol": "000660", "name": "SK하이닉스"}])
        universe = Universe(pool)

        name = asyncio.run(universe.code_to_name("000660"))
        self.assertEqual(name, "SK하이닉스")


if __name__ == "__main__":
    unittest.main()
