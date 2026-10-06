"""
KRX(한국거래소) 휴장일 판별.

토·일은 항상 휴장이고, 그 외에 아래 _HOLIDAYS에 명시된 평일도 휴장이다.
_HOLIDAYS에 없는 평일은 거래일로 간주한다 — 즉 CALENDAR_COVERS_THROUGH를
넘어서는 날짜는 공휴일 반영 없이 평일=거래일로 동작한다(이전과 같은
한계로 자연 축소, 완전 실패하지 않음). is_calendar_stale()로 이 상태를
감지해 호출측(서비스)이 경고/알림을 보낼 수 있게 한다.

출처(2026년 10~12월, PM 확인 사실과 일치):
- 10/5(월) 개천절 대체공휴일, 10/9(금) 한글날, 12/25(금) 성탄절,
  12/31(목) 연말 휴장일(한국거래소 업무규정상 매년 12월 마지막 거래일
  다음날 휴장) — markethours.io/market-holidays/krx/2026,
  calendarlabs.com/krx-market-holidays-2026 등 복수 소스 교차 확인.

2027년은 한국거래소가 아직 공식 고시하지 않아(통상 전년 12월 중순 이후
고시) 넣지 않았다. 리서치로 추정한 2027년 날짜는 보고서에 별도로 적었다
— 확정되면 이 파일에 추가하고 CALENDAR_COVERS_THROUGH를 갱신해야 한다.
"""
from datetime import date

CALENDAR_COVERS_THROUGH = date(2026, 12, 31)

_HOLIDAYS: dict[date, str] = {
    date(2026, 10, 5): "개천절 대체공휴일",
    date(2026, 10, 9): "한글날",
    date(2026, 12, 25): "성탄절",
    date(2026, 12, 31): "연말 휴장일",
}


def is_trading_day(d: date) -> bool:
    """토·일이면 False, 명시된 휴장일이면 False, 그 외 True."""
    if d.weekday() >= 5:
        return False
    return d not in _HOLIDAYS


def holiday_reason(d: date) -> str | None:
    """d가 명시된 휴장일이면 사유 문자열, 아니면 None(주말 포함)."""
    return _HOLIDAYS.get(d)


def is_calendar_stale(d: date) -> bool:
    """d가 이 달력이 보장하는 마지막 날짜(CALENDAR_COVERS_THROUGH)를 넘었는지."""
    return d > CALENDAR_COVERS_THROUGH
