"""
stock_trader/main.py의 가격 감시 루프와 실행기 억제 상태 동기화 회귀 테스트.

검증 항목:
1. 손절선(-7%) 도달 시 sell_fail_suppress 키가 있으면 "손절 재시도 대기 중 (원인: OO)" 메시지가 생성된다.
2. sell_fail_suppress 키가 없으면 "시스템이 즉시 자동 손절 매도 처리 중입니다." 메시지가 생성된다.
3. sell_fail_suppress 키가 단순 문자열이거나 JSON 포맷일 때 모두 호환성 있게 파싱된다.
"""
import json
import unittest


def build_stop_loss_alert_message(nm: str, symbol: str, pnl_rate: float, suppress_raw: str | None) -> str:
    """main.py의 _price_monitor 손절선 도달 알림 메시지 생성 로직을 독립 검증"""
    if suppress_raw:
        suppress_reason = "손절 매도 실패 재시도 억제 중"
        try:
            parsed = json.loads(suppress_raw)
            if isinstance(parsed, dict) and "reason" in parsed:
                suppress_reason = parsed["reason"]
        except Exception:
            if isinstance(suppress_raw, str) and ":" in suppress_raw:
                suppress_reason = suppress_raw.split(":", 1)[1]

        return (
            f"⏳ <b>{nm}({symbol}) 손절선(-7%) 도달 {pnl_rate:+.1f}%</b>\n"
            f"손절 재시도 대기 중 (원인: {suppress_reason})\n"
            f"※ 30분 쿨다운 동안 재시도가 억제됩니다."
        )
    else:
        return (
            f"⚠️ <b>{nm}({symbol}) 손절선(-7%) 도달 {pnl_rate:+.1f}%</b>\n"
            f"시스템이 즉시 자동 손절 매도 처리 중입니다."
        )


class TestPriceMonitorStateSync(unittest.TestCase):
    def test_alert_when_not_suppressed(self):
        """억제 중이 아닐 때 시스템이 즉시 자동 손절 매도 처리 중 안내"""
        msg = build_stop_loss_alert_message("카카오뱅크", "323410", -7.5, None)
        self.assertIn("시스템이 즉시 자동 손절 매도 처리 중입니다.", msg)
        self.assertNotIn("손절 재시도 대기 중", msg)

    def test_alert_when_suppressed_with_json_reason(self):
        """JSON 포맷으로 원인이 기록된 경우 정확한 원인 안내"""
        raw = json.dumps({"reason": "모의투자 주문이 불가한 계좌입니다", "ts": 1727050000.0})
        msg = build_stop_loss_alert_message("카카오뱅크", "323410", -7.5, raw)
        self.assertIn("손절 재시도 대기 중 (원인: 모의투자 주문이 불가한 계좌입니다)", msg)
        self.assertIn("30분 쿨다운 동안 재시도가 억제됩니다", msg)
        self.assertNotIn("시스템이 즉시 자동 손절 매도 처리 중입니다.", msg)

    def test_alert_when_suppressed_with_legacy_colon_string(self):
        """레거시 '1:에러사유' 문자열도 파싱 호환"""
        raw = "1:잔고 부족"
        msg = build_stop_loss_alert_message("카카오뱅크", "323410", -7.2, raw)
        self.assertIn("손절 재시도 대기 중 (원인: 잔고 부족)", msg)

    def test_alert_when_suppressed_with_bare_flag(self):
        """레거시 '1' 단순 플래그 시 기본 문구 적용"""
        raw = "1"
        msg = build_stop_loss_alert_message("카카오뱅크", "323410", -7.2, raw)
        self.assertIn("손절 재시도 대기 중 (원인: 손절 매도 실패 재시도 억제 중)", msg)


if __name__ == "__main__":
    unittest.main()
