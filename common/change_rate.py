"""
common/change_rate.py - [AT] fix/change-rate-calc (PM 승인)

일봉 change_rate(전일 종가 대비 등락률 %) 계산 공용 함수.

배경: KIS 일봉 조회 응답(FHKST03010100 등)에는 prdy_ctrt(전일대비율) 키가 없는데도
data_collector/collectors/daily_collector.py, stock_trader/kis_trader.py,
data_collector/bulk_fetch.py 세 곳이 전부 그 키를 그대로 읽어 change_rate가 항상
0.00으로 저장됐다. 이 모듈은 응답 값을 믿지 않고, 같이 조회된 인접 일봉의 종가로
직접 계산한다 — 세 곳이 전부 이 함수 하나를 쓴다(계산 로직 중복 금지, PM 지시).
"""
from typing import Any, Dict, List, Optional


def compute_change_rate(prev_close: Optional[float], close: Optional[float]) -> float:
    """전일 종가(prev_close) 대비 당일 종가(close)의 등락률(%), 소수 둘째자리 반올림.
    close가 0/None이면 0.0. prev_close가 없거나(조회 구간의 가장 오래된 행) 0/None이면
    0.0(전일 비교 불가)."""
    if not close or not prev_close:
        return 0.0
    return round((close - prev_close) / prev_close * 100, 2)


def fill_change_rates(
    candles: List[Dict[str, Any]],
    date_key: str = "date",
    close_key: str = "close",
    out_key: str = "change_rate",
) -> List[Dict[str, Any]]:
    """candles(dict 리스트)의 각 행에 전일 종가 기준 change_rate를 채워 그대로 반환한다.

    같은 dict 객체를 그 자리에서 수정하므로 candles 리스트 자체의 순서는 바꾸지 않는다
    — date_key 기준으로 정렬한 사본으로만 "전일"을 가린다. 호출부 세 곳의 기존 반환
    순서(data_collector/collectors/daily_collector.py는 오래된순, stock_trader/
    kis_trader.py·data_collector/bulk_fetch.py는 KIS 응답 그대로인 최신순)가 서로 달라도
    이 함수가 내부적으로 정렬해서 계산하므로 호출부가 순서를 맞출 필요가 없다."""
    ordered = sorted(candles, key=lambda c: c.get(date_key) or "")
    prev_close: Optional[float] = None
    for c in ordered:
        c[out_key] = compute_change_rate(prev_close, c.get(close_key))
        prev_close = c.get(close_key)
    return candles
