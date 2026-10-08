"""
dashboard KIS 토큰 재사용 테스트 ([AT] fix/dashboard-kis-token-reuse)

배경: dashboard/main.py에 get_kis_token()이라는 공용 캐시 함수가 있음에도,
get_single_price(/api/price/{symbol}), _ask_gemini_direct(시세 키워드 응답),
_get_stock_balance_raw(/api/balance/stock)는 각자 /oauth2/tokenP를 직접 호출해
호출마다 새 토큰을 발급받고 있었다. 또한 get_kis_token() 자체도 동시에 여러
호출이 캐시 미스 상태로 들어오면 각각 새 토큰을 발급할 수 있었다(락 없음).

이 테스트는
1) get_kis_token()이 만료 전까지 캐시된 토큰을 재사용하고, 동시 호출이 들어와도
   실제 발급은 1번만 일어나는지,
2) 위 세 호출 경로가 각자 /oauth2/tokenP를 치지 않고 get_kis_token()을 통해서만
   토큰을 받는지
를 검증한다. 수정 전 코드로 되돌리면 — call 사이트는 FakeSession에 post()가
없어서 실패하고, get_kis_token()의 락을 빼면 동시 호출 시 발급 카운터가
1을 넘어가서 — 이 테스트들이 깨진다.
"""
import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class _FakeFastAPI:
    """app.get/app.post 같은 라우트 데코레이터가 원본 함수를 그대로 반환하도록 하는
    대역 — get_single_price처럼 @app.get으로 직접 데코레이트된 함수를 테스트에서
    그대로 호출하려면 필요하다(일반 MagicMock은 데코레이터가 함수를 MagicMock으로
    바꿔버려 원본을 잃는다)."""

    def __init__(self, *a, **kw):
        pass

    def _decorator_factory(self, *a, **kw):
        def deco(fn):
            return fn
        return deco

    get = _decorator_factory
    post = _decorator_factory
    delete = _decorator_factory
    put = _decorator_factory
    api_route = _decorator_factory
    on_event = _decorator_factory
    websocket = _decorator_factory

    def add_middleware(self, *a, **kw):
        pass

    def mount(self, *a, **kw):
        pass


# dashboard.main 로드 전 필요한 외부 모듈 모킹 (test_dashboard_chart_rate_limit.py와 동일 패턴)
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

sys.modules["fastapi"].FastAPI = _FakeFastAPI

# pytest가 tests/ 전체를 한 프로세스로 수집하면 다른 테스트 파일(예:
# test_dashboard_chart_rate_limit.py)이 dashboard.main을 먼저 import해 일반
# MagicMock app으로 @app.get 라우트를 이미 MagicMock으로 덮어썼을 수 있다. 그 경우
# 위에서 FastAPI를 바꿔도 소급 적용되지 않으므로, 이미 로드돼 있으면 _FakeFastAPI로
# 강제 재실행(reload)해 get_single_price 같은 라우트 함수가 원본 그대로 남게 한다.
import importlib

if "dashboard.main" in sys.modules:
    importlib.reload(sys.modules["dashboard.main"])


class _FakeTokenResp:
    def __init__(self, token: str):
        self._token = token

    async def json(self):
        return {"access_token": self._token}


class _FakeTokenSession:
    """get_kis_token()이 기대하는 aiohttp 세션 대역 — post()만 지원하고
    호출 횟수를 counter에 기록한다. await asyncio.sleep(0)으로 실제 이벤트
    루프에 양보해 동시 호출 레이스를 재현한다."""

    def __init__(self, counter: dict, token: str):
        self._counter = counter
        self._token = token

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, timeout=None):
        self._counter["count"] += 1
        await asyncio.sleep(0)
        return _FakeTokenResp(self._token)


class _FakeGetResp:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload


class _GetOnlySession:
    """call-site 테스트용 — get()만 지원한다. 되돌린 코드가 여기서 post()를
    부르면 AttributeError가 나서 수정 전 동작을 재현한다."""

    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self._status = status
        self.get_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        self.get_calls += 1
        return _FakeGetResp(self._payload, self._status)


class _BaseDashboardTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self._patches = []

        # 모듈 전역 캐시/락 상태를 테스트마다 초기화
        dm._kis_token_cache["token"] = ""
        dm._kis_token_cache["expires"] = 0
        dm._kis_token_lock = None
        dm.redis_client = None

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def _start(self, p):
        m = p.start()
        self._patches.append(p)
        return m


class TestGetKisTokenSharedCache(_BaseDashboardTestCase):
    def _patch_token_session(self, counter: dict, token: str = "TOK"):
        self._start(patch("aiohttp.ClientSession",
                           MagicMock(side_effect=lambda *a, **kw: _FakeTokenSession(counter, token))))

    async def test_sequential_calls_within_expiry_issue_once(self):
        """만료 전 같은 토큰을 두 번 요청하면 발급 호출은 1번뿐이어야 한다."""
        counter = {"count": 0}
        self._patch_token_session(counter, "TOK-A")

        t1 = await self.dm.get_kis_token()
        t2 = await self.dm.get_kis_token()

        self.assertEqual(t1, "TOK-A")
        self.assertEqual(t2, "TOK-A")
        self.assertEqual(counter["count"], 1)

    async def test_concurrent_calls_issue_token_once(self):
        """캐시가 비어 있을 때 동시에 여러 곳에서 요청해도 실제 발급은 1번만 일어나야 한다."""
        counter = {"count": 0}
        self._patch_token_session(counter, "TOK-B")

        results = await asyncio.gather(*[self.dm.get_kis_token() for _ in range(5)])

        self.assertEqual(counter["count"], 1)
        self.assertTrue(all(r == "TOK-B" for r in results))


class TestKisTokenCallSitesUseSharedCache(_BaseDashboardTestCase):
    """get_single_price / _ask_gemini_direct / _get_stock_balance_raw가 각자
    /oauth2/tokenP를 치지 않고 get_kis_token()을 통해서만 토큰을 받는지 확인."""

    async def test_get_single_price_reuses_shared_token(self):
        fake = _GetOnlySession({
            "output": {"stck_prpr": "70000", "prdy_ctrt": "1.23", "hts_kor_isnm": "삼성전자"}
        })
        self._start(patch("aiohttp.ClientSession", MagicMock(return_value=fake)))
        mock_get_token = self._start(patch.object(self.dm, "get_kis_token", new=AsyncMock(return_value="TOK")))

        result = await self.dm.get_single_price("005930")

        mock_get_token.assert_awaited_once()
        self.assertTrue(result["success"])
        self.assertEqual(result["price"], 70000)
        self.assertEqual(fake.get_calls, 1)

    async def test_ask_gemini_direct_price_lookup_reuses_shared_token(self):
        self._start(patch.object(self.dm.universe, "resolve_symbol",
                                  new=AsyncMock(return_value=("005930", "삼성전자"))))
        self._start(patch.object(self.dm, "get_portfolio_context", new=AsyncMock(return_value="")))
        fake = _GetOnlySession({"output": {"stck_prpr": "70000", "prdy_ctrt": "1.23"}})
        self._start(patch("aiohttp.ClientSession", MagicMock(return_value=fake)))
        mock_get_token = self._start(patch.object(self.dm, "get_kis_token", new=AsyncMock(return_value="TOK")))

        await self.dm._ask_gemini_direct("삼성전자 현재가 알려줘")

        mock_get_token.assert_awaited_once()
        self.assertEqual(fake.get_calls, 1)

    async def test_get_stock_balance_raw_reuses_shared_token(self):
        self.dm._stock_balance_cache = None
        self.dm._stock_balance_cache_ts = 0.0
        self.dm._stock_balance_lock = None

        fake = _GetOnlySession({
            "rt_cd": "0",
            "output": {"ord_psbl_cash": "1000000", "tot_evlu_amt": "5000000"},
        })
        self._start(patch("aiohttp.ClientSession", MagicMock(return_value=fake)))
        mock_get_token = self._start(patch.object(self.dm, "get_kis_token", new=AsyncMock(return_value="TOK")))

        result = await self.dm._get_stock_balance_raw()

        mock_get_token.assert_awaited_once()
        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["cash"], 1000000)
        self.assertEqual(fake.get_calls, 1)


if __name__ == "__main__":
    unittest.main()
