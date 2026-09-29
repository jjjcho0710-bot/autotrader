"""
tests/test_closing_report_dedup.py - 마감 결산 등 일일 스케줄러 트리거 중복 실행 방지 테스트

배경
- dashboard/main.py의 _jarvis_scheduler는 "오늘 이미 실행했는지"를 로컬 Python 변수
  (last_closing 등)로만 추적했다. 15:40~15:45 KST 사이 dashboard가 재배포되면 이 변수가
  초기화되어, 같은 날 마감 결산(및 다른 일일 트리거)이 중복 발송되는 버그가 있었다.
- 수정: _daily_gate(name, today) 헬퍼가 Redis SETNX(nx=True, TTL 48시간)로 "오늘 실행 여부"를
  영구 기록한다. 로컬 변수가 사라지는 재배포/재시작에도 Redis 키가 살아남아 중복을 막는다.

검증 범위
- _daily_gate: 최초 호출은 True + Redis에 nx=True로 키 기록, 같은 날 재호출(=프로세스가
  재시작돼도 로컬 상태 없이 동일한 외부 Redis만 남아있는 상황을 시뮬레이션)은 False,
  날짜가 바뀌면 다시 True. redis_client가 없거나 오류 시에는 fail-open(True)으로 동작해
  Redis 장애가 리포트 자체를 막지 않는다.
- _jarvis_scheduler 소스 검사: 마감 결산(closing)을 포함한 모든 "하루 한 번" 트리거가
  더 이상 last_* 로컬 변수가 아니라 _daily_gate를 사용하는지 확인한다(동일 패턴 전수 수정 검증).
"""
import inspect
import re
import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _stub_missing_runtime_deps():
    """asyncpg/redis/aiohttp/fastapi가 없는 테스트 환경에서도 import 되도록 더미로 대체한다."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = types.ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub
    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        stub = types.ModuleType("aiohttp")
        stub.ClientSession = object
        stub.TCPConnector = lambda *a, **kw: None
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub
    for mod_name in [
        "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
        "google", "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = mock.MagicMock()


_stub_missing_runtime_deps()


class _FakeRedisGate:
    """실제 Redis의 SET NX EX 동작을 흉내내는 인메모리 대역.
    dashboard 프로세스가 재시작돼도 이 인스턴스는 그대로 유지된다고 가정하면,
    실제 운영 환경에서 재배포에도 Redis가 살아남는 상황과 동일하다."""

    def __init__(self):
        self.store = {}
        self.set_calls = []

    async def set(self, key, value, nx=False, ex=None):
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True


class _BoomRedis:
    async def set(self, *a, **kw):
        raise ConnectionError("redis down")


class TestDailyGate(unittest.IsolatedAsyncioTestCase):
    async def test_first_call_returns_true_and_sets_key_with_nx_and_ttl(self):
        import dashboard.main as dm
        fake = _FakeRedisGate()
        today = date(2026, 9, 29)

        with mock.patch.object(dm, "redis_client", fake):
            result = await dm._daily_gate("closing", today)

        self.assertTrue(result)
        self.assertEqual(len(fake.set_calls), 1)
        call = fake.set_calls[0]
        self.assertEqual(call["key"], "jarvis:sched:closing:2026-09-29")
        self.assertTrue(call["nx"])
        self.assertIsNotNone(call["ex"])
        self.assertGreaterEqual(call["ex"], 24 * 3600)
        self.assertLessEqual(call["ex"], 48 * 3600)

    async def test_second_call_same_day_returns_false_simulating_restart(self):
        """핵심 회귀 테스트: 15:40~15:45 사이 재배포로 프로세스가 재시작돼도
        (로컬 last_closing 변수는 이제 존재하지 않음) 같은 날 두 번째 트리거는
        Redis에 남은 키 때문에 차단되어야 한다 — 마감 결산 중복 발송 재발 방지."""
        import dashboard.main as dm
        fake = _FakeRedisGate()  # 프로세스 재시작에도 살아남는 외부 Redis 역할
        today = date(2026, 9, 29)

        # 1차 프로세스: 15:40 창에서 첫 트리거 → 실행돼야 함
        with mock.patch.object(dm, "redis_client", fake):
            first = await dm._daily_gate("closing", today)
        self.assertTrue(first)

        # "재배포로 재시작" 시뮬레이션: 새로운 호출 컨텍스트(로컬 변수 없음)이지만
        # 동일한 Redis 인스턴스를 그대로 사용 → 같은 날 재실행은 막혀야 함
        with mock.patch.object(dm, "redis_client", fake):
            second = await dm._daily_gate("closing", today)
        self.assertFalse(second)

        # 같은 날 세 번째 호출도 계속 차단
        with mock.patch.object(dm, "redis_client", fake):
            third = await dm._daily_gate("closing", today)
        self.assertFalse(third)

    async def test_different_gate_names_are_independent(self):
        import dashboard.main as dm
        fake = _FakeRedisGate()
        today = date(2026, 9, 29)
        with mock.patch.object(dm, "redis_client", fake):
            self.assertTrue(await dm._daily_gate("closing", today))
            self.assertTrue(await dm._daily_gate("morning", today))
            self.assertFalse(await dm._daily_gate("closing", today))

    async def test_new_day_allows_new_run(self):
        import dashboard.main as dm
        fake = _FakeRedisGate()
        with mock.patch.object(dm, "redis_client", fake):
            self.assertTrue(await dm._daily_gate("closing", date(2026, 9, 29)))
            self.assertFalse(await dm._daily_gate("closing", date(2026, 9, 29)))
            self.assertTrue(await dm._daily_gate("closing", date(2026, 9, 30)))

    async def test_missing_redis_client_fails_open(self):
        """Redis 미설정 환경(local dev 등)에서는 기존처럼 항상 실행되어야 한다(가용성 우선)."""
        import dashboard.main as dm
        with mock.patch.object(dm, "redis_client", None):
            self.assertTrue(await dm._daily_gate("closing", date(2026, 9, 29)))
            self.assertTrue(await dm._daily_gate("closing", date(2026, 9, 29)))

    async def test_redis_error_fails_open(self):
        """Redis 장애 시에도 리포트 자체가 막히면 안 되므로 fail-open(True)."""
        import dashboard.main as dm
        with mock.patch.object(dm, "redis_client", _BoomRedis()):
            result = await dm._daily_gate("closing", date(2026, 9, 29))
        self.assertTrue(result)


class TestSchedulerUsesRedisGate(unittest.TestCase):
    """_jarvis_scheduler 소스 검사: 로컬 변수 기반 dedup(last_*)이 남아있지 않고,
    보고된 버그(마감 결산)를 포함한 모든 '하루 한 번' 트리거가 _daily_gate를 쓰는지 확인."""

    @classmethod
    def setUpClass(cls):
        import dashboard.main as dm
        cls.src = inspect.getsource(dm._jarvis_scheduler)

    def test_closing_report_trigger_uses_daily_gate(self):
        """보고된 버그: 15:40 마감 결산 트리거가 더 이상 last_closing 로컬 변수를 쓰지 않음."""
        self.assertIn('await _daily_gate("closing", today)', self.src)
        self.assertNotIn("last_closing", self.src)

    def test_no_leftover_local_dedup_variables(self):
        """같은 패턴(last_* 로컬 변수로 '오늘 이미 처리함' 추적)이 다른 트리거에도 남아있지 않음."""
        self.assertIsNone(
            re.search(r"\blast_\w+\s*=\s*None\b", self.src),
            "last_* 로컬 dedup 변수가 아직 남아있음 — 재배포 시 초기화되어 중복 실행될 수 있음",
        )
        self.assertIsNone(
            re.search(r"\blast_\w+\s*!=\s*today\b", self.src),
            "last_* != today 형태의 로컬 dedup 조건이 아직 남아있음",
        )

    def test_all_known_daily_triggers_migrated_to_redis_gate(self):
        """근처의 다른 '하루 한 번' 트리거들도 모두 동일하게 고쳐졌는지 확인."""
        expected_gate_names = [
            "daily_report",  # 21:00 통합 일일보고
            "wk_review",     # 토 10:00 주간복습
            "wk_preview",    # 일 20:00 다음주예습
            "queue_run",     # 09:01 장외 승인 예약 실행
            "adv_11",        # 11:00 능동 제안
            "adv_14",        # 14:00 능동 제안
            "scan_0930",     # 09:30 보충 스캔
            "scan_1030",     # 10:30 보충 스캔
            "scan_1300",     # 13:00 보충 스캔
            "morning",       # 08:30 장 시작 전 루틴
            "closing",       # 15:40 장 마감 후 자동 분석 (보고된 버그)
        ]
        for name in expected_gate_names:
            with self.subTest(gate=name):
                self.assertIn(f'_daily_gate("{name}", today)', self.src)


if __name__ == "__main__":
    unittest.main()
