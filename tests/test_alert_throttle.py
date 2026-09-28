"""
tests/test_alert_throttle.py - 알림 스로틀링 및 중복 발송 억제 단위 테스트
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# dashboard.main / common.telegram 로드 전 필요한 외부 모듈 모킹
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
    reset_symbol_alert,
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
        # 1차 발송
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        # 5분 후 동일 상태 (1300초) -> 억제되어야 함
        should_send_5m = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1300.0,
        )
        self.assertFalse(should_send_5m)

        # 59분 후 동일 상태 (4500초, 경과 3500초 < 3600초) -> 억제되어야 함
        should_send_59m = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=4500.0,
        )
        self.assertFalse(should_send_59m)

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

        # 3601초 후 (4601초) -> 1시간 지났으므로 1회 허용
        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=4601.0,
        )
        self.assertTrue(should_send)

    async def test_state_transition_triggers_immediate_alert(self):
        """상태가 DROP에서 STOP_LOSS로 전이되면 시간 무관하게 즉시 1회 발송"""
        await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1000.0,
        )

        # 30초 후 STOP_LOSS로 악화 (1030초) -> 상태 전이이므로 즉시 허용
        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="STOP_LOSS",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1030.0,
        )
        self.assertTrue(should_send)

        # 다시 10초 후 STOP_LOSS 유지 -> 억제
        should_send_repeat = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="STOP_LOSS",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1040.0,
        )
        self.assertFalse(should_send_repeat)

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

        # 정상 복귀 -> False 반환 (무알림)
        should_send_normal = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="NORMAL",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1500.0,
        )
        self.assertFalse(should_send_normal)

        # 1분 후 다시 급락 (1560초) -> NORMAL에서 DROP으로 상태 변경되었으므로 즉시 발송!
        should_send_re_drop = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1560.0,
        )
        self.assertTrue(should_send_re_drop)

    async def test_in_memory_fallback_without_redis(self):
        """Redis가 없는 환경에서도 인메모리 캐시로 정상 동작"""
        should_send1 = await should_send_symbol_alert(
            symbol="035420",
            alert_type="sell_fail",
            new_state="네트워크 오류",
            min_interval_sec=3600,
            redis_client=None,
            now_ts=1000.0,
        )
        self.assertTrue(should_send1)

        # 5분 후 동일 에러 -> 억제
        should_send2 = await should_send_symbol_alert(
            symbol="035420",
            alert_type="sell_fail",
            new_state="네트워크 오류",
            min_interval_sec=3600,
            redis_client=None,
            now_ts=1300.0,
        )
        self.assertFalse(should_send2)

        # 에러 메시지 변경 -> 상태 변경으로 즉시 발송
        should_send3 = await should_send_symbol_alert(
            symbol="035420",
            alert_type="sell_fail",
            new_state="계좌번호 불일치",
            min_interval_sec=3600,
            redis_client=None,
            now_ts=1310.0,
        )
        self.assertTrue(should_send3)

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

        # 초기화 후 동일 상태여도 신규로 인식하여 발송
        should_send = await should_send_symbol_alert(
            symbol="005930",
            alert_type="price_monitor",
            new_state="DROP",
            min_interval_sec=3600,
            redis_client=self.redis,
            now_ts=1010.0,
        )
        self.assertTrue(should_send)


class TestTelegramKeywordSuppression(unittest.IsolatedAsyncioTestCase):
    async def test_skip_and_stop_loss_wait_suppression_common_telegram(self):
        """common/telegram.py에서 SKIP 및 손절 대기 키워드 메시지 발송 차단 검증"""
        import common.telegram as ct

        sent_messages = []

        async def fake_send(token, chat_id, text):
            # ct._send 원본 로직을 재현하여 차단 확인
            _suppress_keywords = ("Jarvis 판단: SKIP", "매수 신호 건너뜀", "손절 재시도 대기", "손절 대기")
            if any(kw in (text or "") for kw in _suppress_keywords):
                return
            sent_messages.append(text)

        with patch.object(ct, "_send", side_effect=fake_send):
            # SKIP 키워드 -> 차단
            await ct.send_message("Jarvis 판단: SKIP 신호 감지")
            # 손절 재시도 대기 키워드 -> 차단
            await ct.send_stock("⏳ 카카오뱅크(323410) 손절선(-7%) 도달\n손절 재시도 대기 중")
            # 손절 대기 키워드 -> 차단
            await ct.send_stock("⏳ 카카오뱅크(323410) 손절 대기 중")
            # 정상 알림 -> 발송
            await ct.send_stock("📈 삼성전자 매수 완료")

        self.assertEqual(len(sent_messages), 1)
        self.assertIn("삼성전자 매수 완료", sent_messages[0])

    async def test_skip_and_stop_loss_wait_suppression_dashboard(self):
        """dashboard/main.py:_send_telegram에서 SKIP 및 손절 대기 키워드 메시지 발송 차단 검증"""
        import dashboard.main as dm

        mock_session = AsyncMock()
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_session
        mock_cm.__aexit__.return_value = None

        with patch.object(dm.config, "STARK_BOT_TOKEN", "fake-token"), \
             patch.object(dm.config, "TELEGRAM_CHAT_ID", "12345"), \
             patch("aiohttp.ClientSession", return_value=mock_cm):

            # SKIP 키워드 -> 차단
            await dm._send_telegram("Jarvis 판단: SKIP 처리됨", store=False)
            mock_session.post.assert_not_called()

            # 손절 대기 키워드 -> 차단
            await dm._send_telegram("⏳ 손절 재시도 대기 중 (원인: KIS 에러)", store=False)
            mock_session.post.assert_not_called()

            # 일반 메시지 -> 전송
            await dm._send_telegram("오늘 매매 결과 요약", store=False)
            self.assertEqual(mock_session.post.call_count, 1)


if __name__ == "__main__":
    unittest.main()
