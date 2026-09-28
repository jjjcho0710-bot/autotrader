"""
ML 학습 결과 텔레그램 보고서 본문 생성 (순수 함수 — I/O 없음, 테스트 용이).

results 항목: {"symbol": str, "name": str|None, "accuracy": float, "samples": int}  (학습 성공 종목만)
meta: {"total": 감시 종목 수, "insufficient": 데이터 부족 실패 수, "other": 기타 실패 수}
"""
import html

MIN_SAMPLES = 150       # 학습 표본이 이보다 적으면 "표본 적음" 표시
COIN_FLIP_PCT = 50.0    # 이 미만은 동전 던지기보다 낮음
USEFUL_PCT = 55.0       # 이 이상만 참고 가치


def build_ml_report_text(results: list, meta: dict, now) -> str:
    total = int(meta.get("total", 0))
    insufficient = int(meta.get("insufficient", 0))
    other = int(meta.get("other", 0))
    ok = len(results)
    failed = insufficient + other

    lines = [
        f"🎓 ML 학습 결과 ({now.strftime('%m/%d %H:%M')})",
        f"감시 {total}종목 중 학습 성공 {ok}, 실패 {failed} (데이터 부족 {insufficient}, 기타 {other})",
        "",
        "📖 읽는 법",
        "· 각 숫자 = 과거 데이터로 시험했을 때 5거래일 뒤 0.5% 넘게 오를지를 맞힌 비율",
        "· 50%는 동전 던지기와 같은 수준이고 55% 미만은 참고하지 말 것",
        "· 수익률이 아니라 예측 정확도이고, 시험 표본이 적어 ±15%p 흔들림",
        "",
        "📊 결과 (정확도 높은 순)",
    ]

    ranked = sorted(results, key=lambda r: (-float(r["accuracy"]), str(r["symbol"])))
    for r in ranked:
        label = html.escape(str(r.get("name") or r["symbol"]))
        line = f"· {label} {float(r['accuracy']):.1f}%"
        if float(r["accuracy"]) < COIN_FLIP_PCT:
            line += " (동전보다 낮음)"
        if int(r.get("samples") or 0) < MIN_SAMPLES:
            line += " ⚠️표본 적음"
        lines.append(line)
    if not ranked:
        lines.append("· 학습에 성공한 종목이 없습니다")

    lines.append("")
    if ranked:
        accs = [float(r["accuracy"]) for r in ranked]
        lines.append(
            f"요약: 평균 {sum(accs) / len(accs):.1f}% · "
            f"{int(USEFUL_PCT)}% 이상 {sum(a >= USEFUL_PCT for a in accs)}종목 · "
            f"{int(COIN_FLIP_PCT)}% 미만 {sum(a < COIN_FLIP_PCT for a in accs)}종목"
        )
    else:
        lines.append("요약: 학습 성공 종목 없음")
    return "\n".join(lines)
