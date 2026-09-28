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


def _build_mst_line(code: str, name: str, isin: str = "", tail: bytes = b"ST1002700130000 NN5YYY" + b"0" * 30) -> bytes:
    """실제 KIS .mst 한 줄의 레이아웃을 재현한다(2026-09-28 서버 파일로 확인):
    [0:9] 단축코드(우측 공백 패딩), [9:21] 표준코드(ISIN, 12바이트), [21:61] 종목명(cp949 40바이트,
    공백 패딩), 이후 고정폭 속성 필드. 예) b'005930   KR7005930003삼성전자...'.
    ISIN을 생략하면 실제 주식처럼 'KR7'+코드+'000' 형태(체크숫자는 무시)를 쓴다."""
    isin = isin or f"KR7{code}003"
    assert len(isin) == 12
    name_bytes = name.encode("cp949")
    assert len(name_bytes) <= 40
    return (code.encode("ascii").ljust(9) + isin.encode("ascii")
            + name_bytes.ljust(40) + tail)


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
    def test_reads_short_code_not_isin_digits(self):
        """회귀: ISIN(KR7005930003)의 숫자 뒤 6자리(930003)가 아니라 단축코드(005930)를 읽어야 한다."""
        raw = (_build_mst_line("005930", "삼성전자", isin="KR7005930003") + b"\n"
               + _build_mst_line("000660", "SK하이닉스", isin="KR7000660001") + b"\n"
               + _build_mst_line("035720", "카카오", isin="KR7035720002"))

        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)

        self.assertEqual(added, 3)
        self.assertEqual(name_map["삼성전자"], "005930")
        self.assertEqual(name_map["SK하이닉스"], "000660")
        self.assertEqual(name_map["카카오"], "035720")
        self.assertNotIn("930003", name_map.values())
        self.assertNotIn("660001", name_map.values())

    def test_real_file_line_bytes(self):
        """실제 kospi_code.mst의 삼성전자 줄 앞부분 원본 바이트."""
        real_head = b"005930   KR7005930003\xbb\xef\xbc\xba\xc0\xfc\xc0\xda" + b" " * 32
        line = real_head + b"ST1002700130000 NN5YYY YYNNNNNNN0NNNNNNNY0002855000000100001"
        name_map = {}
        _parse_kis_master_bytes(line, "cp949", name_map)
        self.assertEqual(name_map, {"삼성전자": "005930"})

    def test_accepts_alphanumeric_six_char_code(self):
        """신규 상장 종목은 0001A0 처럼 영문이 섞인 단축코드를 쓴다(ISIN도 KR70001A0001)."""
        raw = (_build_mst_line("0001A0", "덕양에너젠", isin="KR70001A0001") + b"\n"
               + _build_mst_line("147760", "피엠티"))
        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 2)
        self.assertEqual(name_map["덕양에너젠"], "0001A0")
        self.assertEqual(name_map["피엠티"], "147760")

    def test_skips_non_six_char_codes(self):
        """ETN(7자)·수익증권/워런트(9자)는 9바이트 필드를 자르지 않고 통째로 읽어 6자 검사에서 제외한다.
        앞 6자만 자르면 F70100 같은 코드로 대량 중복/오염된다."""
        raw = (_build_mst_line("Q500067", "신한 레버리지 10년 국채선물 ETN", isin="KRG500000671") + b"\n"
               + _build_mst_line("F70100030", "한투한미핵심성장포커스1(A)", isin="KR5701000303") + b"\n"
               + _build_mst_line("F70100031", "한투한미핵심성장포커스1(A-e)", isin="KR5701000311") + b"\n"
               + _build_mst_line("J0036221D", "KG모빌리티 122WR", isin="KRA0036221D6"))
        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 0)
        self.assertEqual(name_map, {})

    def test_keeps_name_starting_with_digit(self):
        """'3S', '1Q ...' 처럼 숫자로 시작하는 실제 종목명을 버리지 않는다."""
        raw = _build_mst_line("060310", "3S") + b"\n" + _build_mst_line("0000D0", "1Q 미국S&P500")
        name_map = {}
        _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(name_map, {"3S": "060310", "1Q 미국S&P500": "0000D0"})

    def test_skips_short_lines(self):
        name_map = {}
        added = _parse_kis_master_bytes(b"short\n", "cp949", name_map)
        self.assertEqual(added, 0)
        self.assertEqual(name_map, {})

    def test_does_not_break_on_multibyte_boundary(self):
        """멀티바이트(cp949) 종목명을 바이트 오프셋으로 정확히 잘라내는지 확인.
        텍스트로 먼저 디코딩한 뒤 문자 단위로 자르면 이 케이스에서 깨진다."""
        line = _build_mst_line("323410", "카카오뱅크")
        name_map = {}
        added = _parse_kis_master_bytes(line, "cp949", name_map)
        self.assertEqual(added, 1)
        self.assertEqual(name_map["카카오뱅크"], "323410")

    def test_duplicate_code_is_not_added_twice(self):
        raw = _build_mst_line("005930", "삼성전자") + b"\n" + _build_mst_line("005930", "삼성전자")
        name_map = {}
        added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 1)
        self.assertEqual(len(name_map), 1)

    def test_same_name_different_code_is_not_silently_dropped(self):
        """이름이 같고 코드가 다른 두 종목: 한쪽이 name→code 맵에서 조용히 덮여 사라지면 안 된다."""
        raw = _build_mst_line("111111", "동명종목") + b"\n" + _build_mst_line("222222", "동명종목")
        name_map = {}
        with self.assertLogs("market.sync_batch", level="WARNING"):
            added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 2)
        self.assertEqual(sorted(name_map.values()), ["111111", "222222"])
        self.assertEqual(name_map["동명종목"], "111111")

    def test_existing_entries_are_not_overwritten(self):
        """미리 채워진 name_map(pykrx 결과)의 종목/코드는 KIS가 덮어쓰지 않는다."""
        name_map = {"삼성전자": "005930"}
        raw = _build_mst_line("005930", "삼성전자(KIS표기)") + b"\n" + _build_mst_line("000660", "SK하이닉스")
        added = _parse_kis_master_bytes(raw, "cp949", name_map)
        self.assertEqual(added, 1)
        self.assertEqual(name_map, {"삼성전자": "005930", "SK하이닉스": "000660"})


class TestMergeKisSupplement(unittest.TestCase):
    def test_pykrx_wins_and_kis_only_adds_missing(self):
        pykrx = {"삼성전자": "005930", "카카오": "035720"}
        kis = {"삼성전자": "999999", "삼성전자우": "005935", "카카오": "035720", "피엠티": "147760"}
        added = sync_batch._merge_kis_supplement(pykrx, kis)
        # 같은 이름의 KIS 항목(999999)은 pykrx 값을 덮지 않고 이름(코드)로 구분되어 추가된다
        self.assertEqual(pykrx["삼성전자"], "005930")
        self.assertEqual(pykrx["삼성전자우"], "005935")
        self.assertEqual(pykrx["피엠티"], "147760")
        self.assertEqual(pykrx["삼성전자(999999)"], "999999")
        self.assertEqual(added, 3)

    def test_code_already_in_pykrx_under_other_name_is_skipped(self):
        pykrx = {"삼성전자": "005930"}
        added = sync_batch._merge_kis_supplement(pykrx, {"삼성전자 보통주": "005930"})
        self.assertEqual(added, 0)
        self.assertEqual(pykrx, {"삼성전자": "005930"})


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

    def test_kis_supplement_does_not_overwrite_pykrx(self):
        """pykrx 결과가 부족해 KIS로 보완할 때, 같은 종목명/코드는 pykrx 값이 유지되어야 한다."""
        pool = FakePool([], age=None)
        universe = Universe(pool)

        async def _fake_pykrx():
            return {"삼성전자": "005930"}

        async def _fake_kis():
            return {"삼성전자": "930003", "삼성전자우": "005935", "피엠티": "147760"}

        orig_pykrx, orig_kis = sync_batch._fetch_from_pykrx, sync_batch._fetch_from_kis_master
        sync_batch._fetch_from_pykrx = _fake_pykrx
        sync_batch._fetch_from_kis_master = _fake_kis
        try:
            asyncio.run(sync_stock_universe(pool, universe))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx
            sync_batch._fetch_from_kis_master = orig_kis

        self.assertEqual(universe.code_cache["005930"], "삼성전자")
        self.assertEqual(universe.code_cache["005935"], "삼성전자우")
        self.assertEqual(universe.code_cache["147760"], "피엠티")
        self.assertEqual(universe.code_cache["930003"], "삼성전자(930003)")  # 코드가 다르면 덮지 않고 구분

    def test_force_refreshes_even_when_db_is_fresh(self):
        rows = [{"symbol": f"{i:06d}", "name": f"종목{i}"} for i in range(2000)]
        pool = FakePool(rows, age=timedelta(days=1))
        universe = Universe(pool)

        async def _fake_pykrx():
            return {"신규종목": "999999"}

        async def _fake_kis():
            return {}

        orig_pykrx, orig_kis = sync_batch._fetch_from_pykrx, sync_batch._fetch_from_kis_master
        sync_batch._fetch_from_pykrx = _fake_pykrx
        sync_batch._fetch_from_kis_master = _fake_kis
        try:
            count = asyncio.run(sync_stock_universe(pool, universe, force=True))
        finally:
            sync_batch._fetch_from_pykrx = orig_pykrx
            sync_batch._fetch_from_kis_master = orig_kis

        # force=True면 신선한 DB라도 네트워크 갱신 결과(1개)로 캐시가 교체돼야 함
        self.assertEqual(count, 1)
        self.assertEqual(universe.code_cache, {"999999": "신규종목"})


if __name__ == "__main__":
    unittest.main()
