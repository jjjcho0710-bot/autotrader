"""
tests/test_alert_throttle.py - 알림 스로틀링, 원인별 실패 스로틀 및 회귀 테스트
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# 필요 외부 모듈 모킹
for mod_name in [
    "fastapi",
    "fastapi.staticfiles",
    "fastapi.responses",
    "fastapi.middleware.cors",
    "asyncpg",
    "redis",
    "redis.asyncio",
    "aiohttp",
    "google",
    "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

from common.alert_throttle import (
    _MEMORY_CACHE,
    _MEMORY_CAUSE_CACHE,
    _MEMORY_DROP_CACHE,
    check_cause_fail_throttle,
    reset_cause_fail_throttle,
    reset_symbol_alert,
    should_send_drop_alert,
    should_send_symbol_alert,
)


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = str(value)

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)


class TestAlertThrottle(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _MEMORY_CACHE.clear()
        _MEMORY_CAUSE_CACHE.clear()
        self.redis = FakeRedis()

    async def test_state_change_allows_first_alert(self):
        """초기 상태에서 DROP 진입 시 즉시 1회 발송 허용"""
        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )
        self.assertTrue(should_send)

    async def test_same_state_within_interval_suppressed(self):
        """동일 상태(DROP -> DROP)가 1시간 미만이면 억제"""
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        should_send_5m = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1300.0,
        )
        self.assertFalse(should_send_5m)

    async def test_same_state_after_interval_allowed(self):
        """동일 상태가 1시간(3600초) 이상 지속되면 주기적 1회 발송 허용"""
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=4601.0,
        )
        self.assertTrue(should_send)

    async def test_normal_recovery_is_silent_and_resets(self):
        """NORMAL 복귀 시 알림 미발송 및 상태 갱신, 이후 재급락 시 즉시 발송"""
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        should_send_normal = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="NORMAL",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1500.0,
        )
        self.assertFalse(should_send_normal)

        should_send_re_drop = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1560.0,
        )
        self.assertTrue(should_send_re_drop)

    async def test_reset_symbol_alert(self):
        """청산 시 스로틀 초기화"""
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        await reset_symbol_alert("005930", redis_client=self.redis)

        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1010.0,
        )
        self.assertTrue(should_send)


class TestShouldSendDropAlert(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _MEMORY_DROP_CACHE.clear()
        self.redis = FakeRedis()

    async def test_first_alert_always_sent(self):
        """직전 기록이 없으면(첫 급락 알림) 즉시 발송하고 직전값은 None"""
        should_send, prev = await should_send_drop_alert(
            symbol="005930", pnl_rate=-4.0, redis_client=self.redis
        )
        self.assertTrue(should_send)
        self.assertIsNone(prev)

    async def test_small_additional_drop_is_suppressed(self):
        """직전 알림 대비 1%p 미만 추가 하락이면 재알림 억제"""
        await should_send_drop_alert(symbol="005930", pnl_rate=-4.0, redis_client=self.redis)

        should_send, prev = await should_send_drop_alert(
            symbol="005930", pnl_rate=-4.8, redis_client=self.redis
        )
        self.assertFalse(should_send)
        self.assertEqual(prev, -4.0)

    async def test_drop_of_at_least_threshold_allows_alert(self):
        """직전 알림 대비 1%p 이상 추가 하락이면 재알림 허용"""
        await should_send_drop_alert(symbol="005930", pnl_rate=-4.0, redis_client=self.redis)

        should_send, prev = await should_send_drop_alert(
            symbol="005930", pnl_rate=-5.0, redis_client=self.redis
        )
        self.assertTrue(should_send)
        self.assertEqual(prev, -4.0)

    async def test_recovery_then_re_entry_still_requires_threshold(self):
        """정상 복귀 후 재진입해도(이 함수가 초기화되지 않으므로) 1%p 조건을 그대로 따름"""
        await should_send_drop_alert(symbol="005930", pnl_rate=-4.0, redis_client=self.redis)

        # NORMAL 복귀는 should_send_symbol_alert 쪽 상태만 갱신하고
        # drop_alert_last 기록은 그대로 유지되어야 한다(별도 초기화 호출 없음)
        should_send_re_entry, prev = await should_send_drop_alert(
            symbol="005930", pnl_rate=-4.5, redis_client=self.redis
        )
        self.assertFalse(should_send_re_entry)
        self.assertEqual(prev, -4.0)

    async def test_reset_symbol_alert_clears_drop_record(self):
        """종목 청산(reset_symbol_alert) 시 급락 알림 기준값도 초기화되어 다음 알림은 첫 알림으로 취급"""
        await should_send_drop_alert(symbol="005930", pnl_rate=-4.0, redis_client=self.redis)

        await reset_symbol_alert("005930", redis_client=self.redis)

        should_send, prev = await should_send_drop_alert(
            symbol="005930", pnl_rate=-4.2, redis_client=self.redis
        )
        self.assertTrue(should_send)
        self.assertIsNone(prev)

    async def test_stop_loss_state_repeats_are_still_time_throttled_not_magnitude(self):
        """STOP_LOSS는 이 1%p 억제 대상이 아니며 기존 상태전이/1시간 규칙만 적용된다"""
        # 최초 STOP_LOSS 진입: 즉시 발송
        send1 = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="STOP_LOSS",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0,
        )
        self.assertTrue(send1)

        # 손익률이 미세하게(0.1%p) 더 나빠져도 동일 상태·1시간 미경과면 억제(매그니튜드 규칙 미적용)
        send2 = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="STOP_LOSS",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1030.0,
        )
        self.assertFalse(send2)


class TestCauseFailThrottle(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _MEMORY_CAUSE_CACHE.clear()
        self.redis = FakeRedis()

    async def test_same_cause_different_symbols_bundled_within_1hr(self):
        """종목이 달라도 같은 원인이면 1시간에 1건으로 묶고 개별 발송 억제"""
        # 1번째 종목 실패 -> 즉시 1회 발송
        send1, disp1 = await check_cause_fail_throttle(
            action_kr="매수", symbol="298040", name="서전기전",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0
        )
        self.assertTrue(send1)
        self.assertEqual(disp1, "서전기전")

        # 2번째 종목 같은 원인 실패 (10초 후) -> 억제
        send2, disp2 = await check_cause_fail_throttle(
            action_kr="매수", symbol="299170", name="더블유에스아이",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1010.0
        )
        self.assertFalse(send2)

        # 3번째 종목 같은 원인 실패 (20초 후) -> 억제
        send3, disp3 = await check_cause_fail_throttle(
            action_kr="매수", symbol="268280", name="셀리드",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1020.0
        )
        self.assertFalse(send3)

    async def test_cause_change_triggers_immediate_alert(self):
        """원인이 바뀌면 종목 불문 즉시 발송"""
        send1, disp1 = await check_cause_fail_throttle(
            action_kr="매수", symbol="298040", name="서전기전",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0
        )
        self.assertTrue(send1)

        # 다른 원인 (예: 예수금 부족) 발생 시 즉시 발송
        send2, disp2 = await check_cause_fail_throttle(
            action_kr="매수", symbol="005930", name="삼성전자",
            err="주문가능금액(예수금)이 부족합니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1010.0
        )
        self.assertTrue(send2)
        self.assertEqual(disp2, "삼성전자")

    async def test_same_cause_summary_after_1hr(self):
        """1시간 경과 후 동일 원인이 지속되면 'N종목' 요약 문구로 발송"""
        # 1시간 내 3종목 누적
        await check_cause_fail_throttle(
            action_kr="매수", symbol="298040", name="서전기전",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0
        )
        await check_cause_fail_throttle(
            action_kr="매수", symbol="299170", name="더블유에스아이",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1010.0
        )
        await check_cause_fail_throttle(
            action_kr="매수", symbol="268280", name="셀리드",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1020.0
        )

        # 1시간 1초 경과 후 실패 발생 -> 'N종목' 요약 발송
        send_summary, disp_summary = await check_cause_fail_throttle(
            action_kr="매수", symbol="323410", name="카카오뱅크",
            err="모의투자 주문이 불가한 계좌입니다",
            min_interval_sec=3600, redis_client=self.redis, now_ts=4601.0
        )
        self.assertTrue(send_summary)
        self.assertIn("외", disp_summary)
        self.assertIn("총", disp_summary)

    async def test_scenario_identical_cause_10_calls_triggers_1_alert(self):
        """시나리오 1: 동일 원인 10건 연속 발생 시 1건(원인 개수 1건)만 발송"""
        sent_count = 0
        for i in range(10):
            send, _ = await check_cause_fail_throttle(
                action_kr="매수", symbol=f"0000{i:02d}", name=f"종목{i}",
                err="모의투자 주문이 불가한 계좌입니다",
                min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0 + i
            )
            if send:
                sent_count += 1
        self.assertEqual(sent_count, 1)

    async def test_scenario_alternating_causes_10_calls_triggers_2_alerts(self):
        """시나리오 2: 원인 2개가 번갈아 10건 발생해도 원인별 독립 키로 2건(원인 개수 2건)만 발송"""
        sent_count = 0
        causes = [
            "모의투자 주문이 불가한 계좌입니다",
            "주문가능금액(예수금)이 부족합니다",
        ]
        for i in range(10):
            err = causes[i % 2]
            send, _ = await check_cause_fail_throttle(
                action_kr="매수", symbol=f"0000{i:02d}", name=f"종목{i}",
                err=err,
                min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0 + i
            )
            if send:
                sent_count += 1
        self.assertEqual(sent_count, 2)

    async def test_scenario_mixed_amounts_10_calls_triggers_1_alert(self):
        """시나리오 3: 에러 문구에 서로 다른 금액/숫자가 섞여 있어도 정규화되어 1건(원인 개수 1건)만 발송"""
        sent_count = 0
        for i in range(10):
            amount = (i + 1) * 10000
            err = f"주문가능금액 {amount:,}원이 필요하지만 잔액이 부족합니다"
            send, _ = await check_cause_fail_throttle(
                action_kr="매수", symbol=f"0000{i:02d}", name=f"종목{i}",
                err=err,
                min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0 + i
            )
            if send:
                sent_count += 1
        self.assertEqual(sent_count, 1)


class TestRegressionNoSuppression(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _MEMORY_CACHE.clear()
        _MEMORY_CAUSE_CACHE.clear()
        self.redis = FakeRedis()

    async def test_execution_order_success_is_never_suppressed(self):
        """체결 알림(매수/매도 완료)은 어떠한 경우에도 억제되지 않고 즉시 발송되어야 함"""
        import stark.execution_guard as eg

        sent_messages = []

        async def fake_send_telegram(msg):
            sent_messages.append(msg)

        async def fake_code_to_name(sym):
            return "삼성전자" if sym == "005930" else "SK하이닉스"

        async def fake_save_memory(**kw):
            pass

        async def fake_log_journal(*args):
            pass

        # 연속 3회 매수 성공 체결 시뮬레이션
        for i in range(3):
            signal = {
                "bot": "stock_trader", "symbol": "005930", "name": "삼성전자",
                "action": "buy", "price": 70000, "qty": 1, "strategy": "MA"
            }
            decision = {"reply": "체결 승인", "is_small": False}

            async def fake_order_ok(*args, **kw):
                return {"success": True, "order_no": f"000{i}"}

            res = await eg.execute(
                signal, decision, pool=None, redis=self.redis,
                kis_order_fn=fake_order_ok,
                send_telegram_fn=fake_send_telegram,
                log_journal_fn=fake_log_journal,
                save_trade_memory_fn=fake_save_memory,
                code_to_name_fn=fake_code_to_name
            )
            self.assertTrue(res["success"])
            self.assertTrue(res["executed"])

        # 체결 알림은 3회 모두 발송되어야 함
        self.assertEqual(len(sent_messages), 3)
        for msg in sent_messages:
            self.assertIn("매수 완료", msg)

    async def test_state_transitions_are_never_suppressed(self):
        """상태 전이 알림(NORMAL -> DROP, DROP -> STOP_LOSS 등)은 시간 간격과 무관하게 절대 억제되지 않음"""
        # 1. NORMAL -> DROP: 즉시 발송
        send1 = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="DROP",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1000.0
        )
        self.assertTrue(send1)

        # 2. DROP -> STOP_LOSS (불과 5초 후 전이): 즉시 발송
        send2 = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="STOP_LOSS",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1005.0
        )
        self.assertTrue(send2)

        # 3. STOP_LOSS -> NORMAL (반등 후 복귀): 무알림 상태 갱신
        send_norm = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="NORMAL",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1050.0
        )
        self.assertFalse(send_norm)

        # 4. NORMAL -> DROP (복귀 후 재급락, 불과 10초 후): 즉시 발송
        send3 = await should_send_symbol_alert(
            symbol="005930", alert_type="price_monitor", new_state="DROP",
            min_interval_sec=3600, redis_client=self.redis, now_ts=1060.0
        )
        self.assertTrue(send3)


if __name__ == "__main__":
    unittest.main()
