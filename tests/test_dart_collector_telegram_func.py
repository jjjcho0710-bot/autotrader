"""
data_collector/collectors/dart_collector.py telegram_func 호출부 시그니처 호환성 회귀 테스트
([AT] feat/telegram-routing 후속 수정).

버그: dart_collector.collect_and_alert()가 telegram_func(msg, dest="personal")로 호출했는데,
data_collector/main.py(60·140행)가 실제로 주입하는 함수는 common.telegram.send_stock(text)
— dest 인자를 받지 않아 공시 알림이 발송될 때마다 TypeError("unexpected keyword argument
'dest'")가 났다. dashboard/main.py(2348행)가 주입하는 _send_telegram은 dest를 지원해 문제가
없었지만, data_collector 쪽 운영 경로는 항상 깨졌다.

수정: telegram_func을 dest 없이 호출하도록 되돌리고, 목적지는 각 주입 함수의 기본값에
맡긴다(send_stock은 항상 개인방, _send_telegram은 dest 미지정 시 기본값이 개인방이라
결과는 동일하다).
"""
import inspect
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from data_collector.collectors.dart_collector import DARTCollector  # noqa: E402
from common.telegram import send_stock  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    async def execute(self, *a, **kw):
        return "INSERT 0"


class _FakePool:
    def acquire(self):
        return _AcquireCtx(_FakeConn())


class TestDartCollectorTelegramFuncSignature(unittest.IsolatedAsyncioTestCase):
    async def test_collect_and_alert_accepts_dest_unaware_telegram_func(self):
        """실제 운영 경로(data_collector/main.py)가 주입하는 common.telegram.send_stock(text)
        처럼 dest를 받지 않는 함수를 telegram_func으로 넘겨도 TypeError 없이 알림을 보내야
        한다. 가짜 함수가 실제 send_stock과 같은 시그니처(위치 인자 1개, dest 없음)인지도
        inspect.signature로 함께 확인한다."""
        collector = DARTCollector()
        collector.api_key = "FAKE_KEY"

        async def fake_get_recent(symbol, days=1):
            return [{"symbol": symbol, "corp_name": "테스트기업", "report_name": "유상증자 결정",
                     "rcept_dt": "20261001", "rcept_no": f"RC{symbol}", "is_important": True}]

        calls = []

        async def fake_send_stock(text):  # 실제 send_stock(text: str)과 동일한 시그니처
            calls.append(text)

        self.assertEqual(
            list(inspect.signature(send_stock).parameters.keys()),
            list(inspect.signature(fake_send_stock).parameters.keys()),
            "가짜 telegram_func 시그니처가 실제 common.telegram.send_stock과 다르면 이 테스트가"
            " 버그를 재현하지 못한다",
        )

        with patch.object(collector, "get_recent_disclosures", new=fake_get_recent), \
             patch("data_collector.collectors.dart_collector.db") as fake_db:
            fake_db.pool = _FakePool()
            try:
                await collector.collect_and_alert(["005930"], telegram_func=fake_send_stock)
            except TypeError as e:
                self.fail(f"telegram_func 호출이 TypeError를 던짐(시그니처 불일치 회귀): {e}")

        self.assertEqual(len(calls), 1)
        self.assertIn("중요 공시 알림", calls[0])
        self.assertIn("테스트기업", calls[0])

    async def test_real_send_stock_does_not_accept_dest_kwarg(self):
        """common.telegram.send_stock은 dest 인자를 받지 않는다는 사실 자체를 문서화한다 —
        이 시그니처와 안 맞는 호출이 바로 이번 버그였다."""
        with self.assertRaises(TypeError):
            await send_stock("x", dest="personal")


if __name__ == "__main__":
    unittest.main()
