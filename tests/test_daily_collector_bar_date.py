"""
data_collector/collectors/daily_collector.py 일봉 저장 날짜 기준 회귀 테스트
([AT] fix/collector-daily-bar-date).

조사 배경: data_collector/main.py의 일봉 수집 트리거(서버 시간 16:00 이후,
커밋 016e5b7 주석 참고)는 실측상 TZ 미설정으로 UTC를 반환하는 서버 로컬
datetime.now()를 쓴다. 이 서버 시각이 KST 새벽(00:00~09:00)에 걸리면 서버
날짜(UTC)와 KST 날짜가 하루 어긋난다.

조사 결론: 이 어긋남은 수집 "트리거 시점"에만 영향을 주고, 실제로 DB에
저장되는 일봉의 날짜에는 영향이 없다 — get_daily_ohlcv()는 캔들의 날짜를
KIS 응답의 stck_bsop_date(영업일)에서만 가져오고, save_daily_ohlcv()는 그
문자열을 그대로 파싱해 ts로 저장한다. datetime.now()는 쿼리 범위의
start/end 산출에만 쓰이며 저장되는 날짜 값 자체에는 섞이지 않는다.
따라서 코드는 수정하지 않았다.

이 테스트는 그 불변조건에 대한 회귀 가드다: 서버 시계가 UTC이고 실제로는
KST 월요일(연휴 뒤 첫 거래일) 새벽인 상황을 시계 고정으로 재현한다. 이때
서버 시계의 naive 날짜(일요일)는 오늘(월요일)과도, 실제 마지막 거래일
(금요일)과도 다르므로 날짜 산정이 조금이라도 서버 시계에 의존하면 바로
드러난다. (검증: save_daily_ohlcv가 날짜를 datetime.now()에서 구하도록
임시로 바꿔보면 이 테스트가 실패한다 — PR 설명 참고.)
"""
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import data_collector.collectors.daily_collector as daily_collector_mod  # noqa: E402
from data_collector.collectors.daily_collector import DailyCollector  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.calls = []

    async def execute(self, query, *args):
        self.calls.append(args)
        return "INSERT 0"


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class _FakeJsonResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload


class _FakeKisSession:
    """get_daily_ohlcv()가 기대하는 aiohttp 세션 대역. inquire-daily-itemchartprice
    GET 호출에 고정된 output2 목록을 돌려준다."""

    def __init__(self, output2):
        self._output2 = output2

    def get(self, url, headers=None, params=None, timeout=None):
        return _FakeJsonResponse({"output2": self._output2})


class TestDailyCollectorStoresKisBusinessDate(unittest.IsolatedAsyncioTestCase):
    async def test_save_uses_kis_business_date_not_server_clock_during_kst_dawn(self):
        """실측 운영 시나리오 재현: 서버 로컬 시계는 TZ 미설정으로 UTC를 반환하고,
        실제 KST는 연휴(토·일) 뒤 첫 거래일 월요일 새벽 03:00다. 서버 시계의 naive
        날짜(일요일, UTC)는 오늘(월요일)과도 실제 마지막 거래일(금요일)과도 다르므로,
        저장되는 날짜가 조금이라도 서버 시계에서 파생되면 바로 드러난다."""
        # 실제 KST 2026-10-12(월) 03:00 == UTC 2026-10-11(일) 18:00.
        server_naive_now = datetime(2026, 10, 11, 18, 0, 0)
        # 주말 휴장이라 KIS는 가장 최근 영업일(금요일 2026-10-09)까지만 응답한다.
        kis_output2 = [{
            "stck_bsop_date": "20261009",
            "stck_oprc": "70000", "stck_hgpr": "71000", "stck_lwpr": "69500",
            "stck_clpr": "70500", "acml_vol": "12345678", "prdy_ctrt": "1.23",
        }]

        collector = DailyCollector()
        collector.session = _FakeKisSession(kis_output2)
        collector.access_token = "FAKE-TOKEN"
        fake_conn = _FakeConn()

        with patch.object(daily_collector_mod, "datetime") as mock_dt, \
             patch.object(daily_collector_mod.db, "pool", _FakePool(fake_conn)):
            mock_dt.now.return_value = server_naive_now
            mock_dt.strptime = datetime.strptime

            candles = await collector.get_daily_ohlcv("005930", days=200)
            self.assertEqual(candles, [{
                "date": "20261009",
                "open": 70000, "high": 71000, "low": 69500, "close": 70500,
                "volume": 12345678, "change_rate": 1.23,
            }])

            await collector.save_daily_ohlcv("005930", candles)

        self.assertEqual(len(fake_conn.calls), 1)
        saved_symbol, saved_ts = fake_conn.calls[0][0], fake_conn.calls[0][1]
        self.assertEqual(saved_symbol, "005930")
        self.assertEqual(saved_ts, datetime(2026, 10, 9))
        # 서버 시계(naive UTC, 일요일 10/11)에서 날짜를 구했다면 나왔을 잘못된 값이
        # 아님을 명시적으로 확인한다.
        self.assertNotEqual(saved_ts.date(), server_naive_now.date())


if __name__ == "__main__":
    unittest.main()
