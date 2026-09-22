"""
market/sync_batch.py 단위 테스트:
  1) KIS 마스터(.mst) 고정폭 파싱 슬라이싱 정확도 — 실제 파일 포맷을 흉내 낸
     바이트열을 만들어 종목코드/종목명이 정확한 오프셋에서 잘리는지 검증한다.
  2) sync_stock_universe()의 오케스트레이션 — stocks 테이블이 신선하면
     네트워크(pykrx/KIS) 호출 없이 캐시만 반영하고, 오래됐거나 비어있으면
     네트워크 경로로 넘어가 DB에 upsert하고 Universe 캐시를 교체하는지 확인한다.

한계: 이 작업 환경에는 asyncpg/postgres/docker/pip이 전혀 없어 실제 DB나
실제 KIS 서버에 연결할 수 없었다. tests/test_decision_logger.py와 동일하게
asyncpg.Pool/Connection 인터페이스를 흉내 내는 FakePool을 쓰고, 네트워크가
필요한 pykrx/KIS 다운로드 함수는 monkeypatch로 대체해 순수 오케스트레이션
로직만 검증한다.
"""
import asyncio
import sys
import unittest
from datetime import timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import market.sync_batch as sync_batch  # noqa: E402
from market.sync_batch import _parse_kis_master_bytes, sync_stock_universe  # noqa: E402
from market.universe import Universe  # noqa: E402


def _build_mst_line(code: str, name: str, name_byte_width: int = 40) -> bytes:
    """KIS .mst 고정폭 포맷 한 줄을 합성한다.
    레이아웃(파싱 코드 기준): [0:9] 미사용 프리픽스, [9:21] 표준코드(ISIN류, 12바이트,
    끝 6자리가 단축코드), [21:61] 종목명(cp949, 40바이트 고정폭, 공백 패딩)."""
    prefix = b"P" * 9
    code_part = ("0" * (12 - len(code)) + code).encode("ascii")
    assert len(code_part) == 12
    name_bytes = name.encode("cp949")
    assert len(name_bytes) <= name_byte_width
    name_field = name_bytes + b" " * (name_byte_width - len(name_bytes))
    return prefix + code_part + name_field


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeConnection:
    """stocks 테이블만 흉내 내는 최소 asyncpg.Connection 대역."""

    def __init__(self, rows, age):
        self.rows = list(rows)  # [{"symbol":..., "name":...}, ...]
        self.age = age  # timedelta | None
        self.upserted = []  # executemany로 넘어온 (symbol, name) 튜플 누적

    async def fetch(self, query: str, *args):
        assert "stocks" in query.lower()
        return [dict(r) for r in self.rows]

    async def fetchval(self, query: str, *args):
        assert "stocks" in query.lower()
        return self.age

    async def executemany(self, query: str, seq):
        assert "INSERT INTO stocks" in query
        self.upserted.extend(seq)


class FakePool:
    def __init__(self, rows, age):
        self._conn = FakeConnection(rows, age)

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestParseKisMasterBytes(unittest.TestCase):
    def test_slices_code_and_name_at_exact_offsets(self):
        line = _build_mst_line("005930", "삼성전자")
        raw = line + b"\n" + _build_mst_line("000660", "SK하이닉스")

        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)

        self.assertEqual(added, 2)
        self.assertEqual(name_map["삼성전자"], "005930")
        self.assertEqual(name_map["SK하이닉스"], "000660")

    def test_skips_short_lines(self):
        name_map = {}
        added = _parse_kis_master_bytes(b"short\n", "cp949", name_map)
        self.assertEqual(added, 0)
        self.assertEqual(name_map, {})

    def test_does_not_break_on_multibyte_boundary(self):
        """멀티바이트(cp949) 종목명을 바이트 오프셋으로 정확히 잘라내는지 확인.
        텍스트로 먼저 디코딩한 뒤 문자 단위로 자르면 이 케이스에서 깨진다."""
        line = _build_mst_line("035720", "카카오뱅크")
        name_map = {}
        added = _parse_kis_master_bytes(line, "cp949", name_map)
        self.assertEqual(added, 1)
        self.assertEqual(name_map["카카오뱅크"], "035720")

    def test_duplicate_code_is_not_added_twice(self):
        raw = _build_mst_line("005930", "삼성전자") + b"\n" + _build_mst_line("005930", "삼성전자")
        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 1)
        self.assertEqual(len(name_map), 1)


class TestSyncStockUniverse(unittest.TestCase):
    def test_fresh_db_skips_network_and_uses_cache(self):
        """stocks가 7일 이내·2000개 이상이면 pykrx/KIS를 호출하지 않아야 한다."""
        rows = [{"symbol": f"{i:06d}", "name": f"종목{i}"} for i in range(2000)]
        pool = FakePool(rows, age=timedelta(days=1))
        universe = Universe(pool)

        async def _fail_pykrx():
            raise AssertionError("신선한 DB가 있는데 pykrx를 호출하면 안 됨")

        async def _fail_kis():
            raise AssertionError("신선한 DB가 있는데 KIS 마스터를 호출하면 안 됨")

        orig_pykrx, orig_kis = sync_batch._fetch_from_pykrx, sync_batch._fetch_from_kis_master
        sync_batch._fetch_from_pykrx = _fail_pykrx
        sync_batch._fetch_from_kis_master = _fail_kis
        try:
            count = asyncio.run(sync_stock_universe(pool, universe))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx
            sync_batch._fetch_from_kis_master = orig_kis

        self.assertEqual(count, 2000)
        self.assertEqual(universe.code_cache["000001"], "종목1")

    def test_stale_db_triggers_network_refresh_and_persists(self):
        """stocks가 비어있으면(또는 오래됐으면) pykrx/KIS 경로로 넘어가 DB에 upsert해야 한다."""
        pool = FakePool([], age=None)
        universe = Universe(pool)

        # pykrx가 충분히(2000개 이상) 성공하면 KIS 마스터 폴백은 필요 없어야 한다.
        pykrx_result = {f"종목{i}": f"{i:06d}" for i in range(2000)}

        async def _fake_pykrx():
            return pykrx_result

        async def _fake_kis():
            raise AssertionError("pykrx가 충분하면 KIS 마스터까지 호출할 필요 없음")

        orig_pykrx, orig_kis = sync_batch._fetch_from_pykrx, sync_batch._fetch_from_kis_master
        sync_batch._fetch_from_pykrx = _fake_pykrx
        sync_batch._fetch_from_kis_master = _fake_kis
        try:
            count = asyncio.run(sync_stock_universe(pool, universe))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx
            sync_batch._fetch_from_kis_master = orig_kis

        self.assertEqual(count, 2000)
        self.assertEqual(universe.code_cache["000000"], "종목0")
        self.assertIn(("000000", "종목0"), pool._conn.upserted)

    def test_insufficient_pykrx_falls_back_to_kis_master(self):
        """pykrx 결과가 2000개 미만이면 KIS 마스터 보완이 호출돼야 한다."""
        pool = FakePool([], age=None)
        universe = Universe(pool)

        async def _fake_pykrx():
            return {"삼성전자": "005930"}  # 1개뿐 → 부족

        async def _fake_kis():
            return {"카카오": "035720"}

        orig_pykrx, orig_kis = sync_batch._fetch_from_pykrx, sync_batch._fetch_from_kis_master
        sync_batch._fetch_from_pykrx = _fake_pykrx
        sync_batch._fetch_from_kis_master = _fake_kis
        try:
            count = asyncio.run(sync_stock_universe(pool, universe))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx
            sync_batch._fetch_from_kis_master = orig_kis

        self.assertEqual(count, 2)
        self.assertEqual(universe.code_cache["005930"], "삼성전자")
        self.assertEqual(universe.code_cache["035720"], "카카오")

    def test_force_refreshes_even_when_db_is_fresh(self):
        rows = [{"symbol": f"{i:06d}", "name": f"종목{i}"} for i in range(2000)]
        pool = FakePool(rows, age=timedelta(days=1))
        universe = Universe(pool)

        async def _fake_pykrx():
            return {"신규종목": "999999"}

        orig_pykrx = sync_batch._fetch_from_pykrx
        sync_batch._fetch_from_pykrx = _fake_pykrx
        try:
            count = asyncio.run(sync_stock_universe(pool, universe, force=True))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx

        # force=True면 신선한 DB라도 네트워크 갱신 결과(1개)로 캐시가 교체돼야 함
        self.assertEqual(count, 1)
        self.assertEqual(universe.code_cache, {"999999": "신규종목"})


if __name__ == "__main__":
    unittest.main()
