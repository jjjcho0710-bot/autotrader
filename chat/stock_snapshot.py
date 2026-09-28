"""
chat/stock_snapshot.py - 웹/텔레그램 채팅용 종목 스냅샷 (순수 계산·포맷 모듈)

감시/보유 종목이 아니어도 실제 시세·일봉을 근거로 답하도록, 일봉 목록(+선택적 현재가 응답)에서
스냅샷 dict를 계산하고 프롬프트 블록 문자열로 만든다. 네트워크·DB 호출은 하지 않는다
(조회는 dashboard/main.py의 _build_stock_snapshot이 기존 KIS 조회 함수를 재사용해 수행).

지표는 ml/features.py 가 노출하는 calc_rsi, sma 를 그대로 재사용한다.
"""
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from ml.features import calc_rsi, sma

KST = timezone(timedelta(hours=9))

MARKET_OPEN = (9, 0)
MARKET_CLOSE = (15, 30)
WEEK52_MIN_ROWS = 250  # 일봉이 이만큼 이상일 때만 52주 고저를 표시

SNAPSHOT_HEADER = "[종목 스냅샷 — 사실 데이터]"
SNAPSHOT_FAIL_HEADER = "[종목 스냅샷 — 조회 실패]"
WEB_INFO_HEADER = "ℹ️ 웹 정보(검증 안 됨)"
OLD_RECORD_TAG = "(옛 계좌 기록, 참고용)"

# 사용자가 과거 기록을 직접 묻는 표현 — 이때만 기준선 이전 매매 기억을 프롬프트에 남긴다
_PAST_KEYWORDS = (
    "옛 계좌", "예전", "과거", "지난 매매", "지난번", "그때", "저번",
    "이전 매매", "매매 기록", "매매기록", "거래 내역", "거래내역", "히스토리", "복기",
)
# 사용자가 뉴스/이슈를 직접 묻는 표현 — 이때만 스냅샷이 있어도 웹 조사를 허용한다
_NEWS_KEYWORDS = ("뉴스", "이슈", "공시", "소식", "재료", "호재", "악재", "왜 올", "왜 떨", "왜 빠", "왜 급")


def wants_past_records(user_msg: str) -> bool:
    return any(k in (user_msg or "") for k in _PAST_KEYWORDS)


def asks_news(user_msg: str) -> bool:
    return any(k in (user_msg or "") for k in _NEWS_KEYWORDS)


def is_market_hours(now: datetime) -> bool:
    """평일 09:00~15:30 KST (공휴일은 판별하지 않음 — 데이터 날짜로 build_snapshot에서 재확인)"""
    now = now.astimezone(KST)
    if now.weekday() >= 5:
        return False
    hm = (now.hour, now.minute)
    return MARKET_OPEN <= hm <= MARKET_CLOSE


def _fmt_date(yyyymmdd: str) -> str:
    s = str(yyyymmdd or "")
    return f"{s[4:6]}/{s[6:8]}" if len(s) >= 8 else s


def _pos(price: float, ma: Optional[float]) -> str:
    if ma is None:
        return "산출 불가"
    return "위" if price > ma else "아래" if price < ma else "같음"


def build_snapshot(symbol: str, name: str, rows: List[Dict], quote: Optional[Dict] = None,
                   now: Optional[datetime] = None) -> Dict:
    """일봉(오래된→최신) + 선택적 현재가 응답으로 스냅샷 계산. rows가 비면 ok=False.

    quote: KIS inquire-price output(stck_prpr 등). 장중(라벨이 '현재가')일 때만 마지막 봉에 덮어쓴다.
    """
    now = (now or datetime.now(KST)).astimezone(KST)
    if not rows:
        return {"ok": False, "symbol": symbol, "name": name, "error": "일봉 데이터 없음"}

    rows = [dict(r) for r in rows]
    last = rows[-1]
    last_date = str(last.get("date", ""))
    today = now.strftime("%Y%m%d")
    # 시간대가 장중이어도 마지막 봉이 오늘이 아니면(휴장일·데이터 지연) 종가로 표시한다
    live = is_market_hours(now) and last_date == today

    quote_used = False
    if live and quote:
        try:
            q_price = int(quote.get("stck_prpr", 0) or 0)
        except (TypeError, ValueError):
            q_price = 0
        if q_price > 0:
            last["close"] = q_price
            for k_src, k_dst in (("stck_oprc", "open"), ("stck_hgpr", "high"), ("stck_lwpr", "low"),
                                 ("acml_vol", "vol")):
                try:
                    v = int(quote.get(k_src, 0) or 0)
                except (TypeError, ValueError):
                    v = 0
                if v > 0:
                    last[k_dst] = v
            quote_used = True

    closes = [float(r["close"]) for r in rows]
    vols = [float(r.get("vol", 0) or 0) for r in rows]
    price = int(last["close"])

    prev_close = int(rows[-2]["close"]) if len(rows) >= 2 else None
    change = change_pct = None
    if prev_close:
        change = price - prev_close
        change_pct = round(change / prev_close * 100, 2)

    vol_ratio = None
    if len(vols) >= 21:
        avg20 = sma(vols[:-1], 20)[-1]  # 최신 봉을 뺀 직전 20거래일 평균
        if avg20 and avg20 > 0:
            vol_ratio = round(vols[-1] / avg20, 2)

    ma5 = sma(closes, 5)[-1]
    ma20 = sma(closes, 20)[-1]
    rsi14 = calc_rsi(closes, 14)[-1]

    tail20 = rows[-20:] if len(rows) >= 20 else None
    snap = {
        "ok": True,
        "symbol": symbol,
        "name": name,
        "live": live,
        "quote_used": quote_used,
        "price": price,
        "prev_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "open": int(last.get("open", 0) or 0),
        "high": int(last.get("high", 0) or 0),
        "low": int(last.get("low", 0) or 0),
        "volume": int(last.get("vol", 0) or 0),
        "vol_ratio": vol_ratio,
        "closes5": [{"date": r["date"], "close": int(r["close"])} for r in rows[-5:]],
        "ma5": round(ma5) if ma5 is not None else None,
        "ma20": round(ma20) if ma20 is not None else None,
        "rsi14": round(rsi14, 1) if rsi14 is not None else None,
        "high20": max(int(r["high"]) for r in tail20) if tail20 else None,
        "low20": min((int(r["low"]) for r in tail20 if int(r["low"]) > 0), default=None) if tail20 else None,
        "data_date": last_date,
        "as_of": now.strftime("%Y-%m-%d %H:%M"),
        "as_of_hm": now.strftime("%H:%M"),
    }
    if len(rows) >= WEEK52_MIN_ROWS:
        tail52 = rows[-WEEK52_MIN_ROWS:]
        snap["high52"] = max(int(r["high"]) for r in tail52)
        snap["low52"] = min((int(r["low"]) for r in tail52 if int(r["low"]) > 0), default=None)
    return snap


def build_failure(symbol: str, name: str, error: str = "", last_daily_date: str = "") -> Dict:
    return {"ok": False, "symbol": symbol, "name": name, "error": (error or "").strip()[:100],
            "last_daily_date": last_daily_date or ""}


def format_snapshot(snap: Dict) -> str:
    """스냅샷 dict → 프롬프트 블록. 실패 스냅샷은 숫자 없이 실패 사실만 담는다."""
    name, symbol = snap.get("name") or snap.get("symbol"), snap.get("symbol")
    if not snap.get("ok"):
        last = snap.get("last_daily_date") or ""
        last_disp = _fmt_date(last) if re.fullmatch(r"\d{8}", last) else (last or "없음")
        last_txt = f"가진 마지막 일봉 날짜: {last_disp}"
        reason = f" (사유: {snap['error']})" if snap.get("error") else ""
        return (f"{SNAPSHOT_FAIL_HEADER}\n"
                f"종목: {name}({symbol})\n"
                f"이 종목의 시세·일봉 조회에 실패했다{reason}. {last_txt}\n"
                "※ 가격·등락률·거래량·이동평균 등 어떤 숫자도 지어내지 마라. "
                "조회에 실패해 지금은 확인하지 못했다고만 답하라.")

    p = snap["price"]
    label = (f"현재가({snap['as_of_hm']} 조회)" if snap["live"]
             else f"종가({_fmt_date(snap['data_date'])} 기준)")
    lines = [SNAPSHOT_HEADER, f"종목: {name}({symbol})"]
    if snap["change"] is not None:
        lines.append(f"{label}: {p:,}원 | 전일 대비 {snap['change']:+,}원 ({snap['change_pct']:+.2f}%)")
    else:
        lines.append(f"{label}: {p:,}원 | 전일 대비: 산출 불가(일봉 1개)")
    lines.append(f"시가 {snap['open']:,} / 고가 {snap['high']:,} / 저가 {snap['low']:,}")
    vol = f"거래량 {snap['volume']:,}주"
    if snap["vol_ratio"] is not None:
        vol += f" (직전 20거래일 평균 대비 {snap['vol_ratio']:.2f}배" + (", 장중 누적 기준)" if snap["live"] else ")")
    else:
        vol += " (20일 평균 대비: 산출 불가)"
    lines.append(vol)
    lines.append("최근 5거래일 종가: " + " → ".join(
        f"{_fmt_date(c['date'])} {c['close']:,}" for c in snap["closes5"]))
    ma5, ma20 = snap["ma5"], snap["ma20"]
    lines.append(
        f"MA5 {ma5:,}({_pos(p, ma5)}) / MA20 {ma20:,}({_pos(p, ma20)})" if ma5 and ma20
        else f"MA5 {f'{ma5:,}' if ma5 else '산출 불가'} / MA20 {f'{ma20:,}' if ma20 else '산출 불가'}")
    lines.append(f"RSI14: {snap['rsi14']}" if snap["rsi14"] is not None else "RSI14: 산출 불가(일봉 부족)")
    if snap["high20"] is not None and snap["low20"] is not None:
        lines.append(f"최근 20일 고가 {snap['high20']:,} / 저가 {snap['low20']:,}")
    if snap.get("high52") is not None and snap.get("low52") is not None:
        lines.append(f"52주 고가 {snap['high52']:,} / 저가 {snap['low52']:,}")
    src = "" if not snap["live"] or snap["quote_used"] else " (현재가 조회 실패 — 일봉 기준)"
    lines.append(f"데이터 기준: 일봉 마지막 날짜 {_fmt_date(snap['data_date'])}, 조회 {snap['as_of']} KST{src}")
    return "\n".join(lines)


def format_baseline_kst(baseline: datetime) -> str:
    return baseline.astimezone(KST).strftime("%m/%d %H:%M")


def build_answer_rules(*, has_snapshot: bool, baseline: datetime, past_asked: bool,
                       news_asked: bool) -> str:
    """채팅 프롬프트 답변 규칙. (b)(c)는 항상, (a)(d)는 종목 스냅샷이 있을 때(성공/실패 무관) 추가."""
    base = format_baseline_kst(baseline)
    rules = [
        "[답변 규칙 — 단정 금지]\n"
        "- '오를 것이다/내릴 것이다' 같은 단정 표현을 쓰지 마라. ML 매수확률·ML 예측은 검증되지 않았으므로 "
        "이 답변의 근거로 쓰지 마라.\n"
        f"- 기준선({base} KST, data_baseline.reliable_trading_data_from) 이전의 매매 기록·손익·교훈은 옛 계좌와 "
        "버그 구간 데이터다. "
        + ("사용자가 과거 기록을 직접 물었으므로 언급하되 반드시 '" + OLD_RECORD_TAG + "'라고 표시하라."
           if past_asked else
           "사용자가 과거를 직접 묻지 않는 한 언급하지 마라. 언급해야 하면 '" + OLD_RECORD_TAG + "'라고 표시하라.")
    ]
    if has_snapshot:
        rules.append(
            "[종목 답변 규칙 — 위 종목 스냅샷이 있을 때]\n"
            "1) 수치 먼저: 종가(또는 현재가)·등락률·거래량 배수·최근 5거래일 흐름·이평선(MA5/MA20)·RSI 위치를 "
            "스냅샷 숫자 그대로 제시한다. 스냅샷에 없는 숫자는 쓰지 않는다.\n"
            "2) 그다음 짧은 해석 1~2문장.\n"
            "3) '향후'는 예측이 아니라 시나리오로 쓴다. 스냅샷에 있는 가격대(최근 20일 고가/저가, MA5/MA20, "
            "52주 고저가 있으면 그것)만 기준으로 '이 가격 아래로 밀리면 …/이 가격 위로 회복하면 …' 형식으로 쓴다. "
            "스냅샷에 없는 가격대는 만들지 마라.\n"
            "4) 스냅샷이 '조회 실패'이면 위 지시대로 실패 사실만 말하고 숫자를 지어내지 마라.\n"
            "5) 감시 추가 제안 문구는 시스템이 마지막 줄에 붙이니 네가 쓰지 마라.\n"
            "6) 웹 조사가 붙으면 스냅샷 숫자와 어긋나는 웹 숫자는 무시한다(웹 정보는 보조).")
        rules.append(
            "[웹 조사 태그] 스냅샷이 있으면 [[NEED_SEARCH]]를 쓰지 마라."
            + (" 단, 사용자가 뉴스·이슈·공시를 물었으므로 필요하면 마지막 줄에 [[NEED_SEARCH: 종목명]]을 붙여도 된다."
               if news_asked else ""))
    return "\n".join(rules)


def watch_suggestion(name: str) -> str:
    """감시 대상이 아닌 종목에 붙이는 마지막 줄 (자동 추가하지 않는다)"""
    return f'감시 종목에 추가할까요? → "감시 추가 {name}"'


def should_suggest_watch(*, resolved: bool, tracked: Optional[bool], reply: str, already_added: bool) -> bool:
    """tracked: True=보유/감시, False=아님, None=판별 실패(제안하지 않음)"""
    if not resolved or tracked is not False or already_added:
        return False
    r = reply or ""
    if r.startswith("❌") or "감시 추가" in r:
        return False
    return True


# ── 웹 조사 보조 정보 ────────────────────────────────────────────

_WEB_LINE_RE = re.compile(
    r"^\s*[-•*]?\s*(\d{4}[-./]\d{1,2}[-./]\d{1,2})\s*\|\s*([^|]{2,40}?)\s*\|\s*(.{4,300})$")


def filter_web_items(text: str) -> str:
    """'날짜 | 출처 | 내용' 형식의 줄만 남긴다. 하나도 없으면 빈 문자열."""
    items = []
    for line in (text or "").splitlines():
        m = _WEB_LINE_RE.match(line)
        if m:
            items.append(f"- {m.group(1)} ({m.group(2).strip()}) {m.group(3).strip()}")
    return "\n".join(items[:6])


def format_web_info(items: str) -> str:
    if not items:
        return ""
    return f"{WEB_INFO_HEADER}\n{items}"
