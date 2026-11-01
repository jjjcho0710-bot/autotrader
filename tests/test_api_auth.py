"""[AT] feat/api-auth-phase1 — 관리자 토큰/텔레그램 webhook secret/내부 호출 헤더."""
import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from common import api_auth  # noqa: E402

TOK = "s3cret-token"
ENV = {"DASHBOARD_ADMIN_TOKEN": TOK}
H_OK = {"x-admin-token": TOK}


def chk(method, path, query=None, headers=None, env=ENV):
    return api_auth.check_request(method, path, query or {}, headers or {}, env)


class TestProtectedPaths(unittest.TestCase):
    PROTECTED = [
        ("POST", "/api/trade/execute"), ("POST", "/api/jarvis/signal"), ("POST", "/api/jarvis/chat"),
        ("POST", "/api/jarvis/action"), ("POST", "/api/strategies/update"), ("POST", "/api/strategies/add"),
        ("POST", "/api/strategy/fix_units"), ("GET", "/api/strategy/fix_units"),
        ("POST", "/api/jarvis/reconcile_trades/delete"), ("GET", "/api/jarvis/reconcile_trades/delete"),
        ("DELETE", "/api/jarvis/history"), ("POST", "/api/watchlist"), ("DELETE", "/api/watchlist/005930"),
        ("POST", "/api/watchlist/cleanup"), ("GET", "/api/watchlist/cleanup"),
        ("POST", "/api/jarvis/knowledge/delete"), ("POST", "/api/jarvis/knowledge/clean_bad_lessons"),
        ("GET", "/api/jarvis/knowledge/clean_placeholders"),
    ]

    def test_missing_token_is_401(self):
        for m, path in self.PROTECTED:
            self.assertEqual(chk(m, path)[0], 401, (m, path))

    def test_wrong_token_is_401(self):
        for m, path in self.PROTECTED:
            self.assertEqual(chk(m, path, headers={"x-admin-token": "nope"})[0], 401, (m, path))

    def test_correct_token_passes(self):
        for m, path in self.PROTECTED:
            self.assertIsNone(chk(m, path, headers=H_OK), (m, path))

    def test_trailing_slash_is_normalized(self):
        self.assertEqual(chk("POST", "/api/jarvis/signal/")[0], 401)

    def test_reconcile_period_only_protected_when_apply(self):
        self.assertIsNone(chk("GET", "/api/jarvis/reconcile_trades_period"))
        self.assertIsNone(chk("GET", "/api/jarvis/reconcile_trades_period", {"apply": "0"}))
        self.assertEqual(chk("GET", "/api/jarvis/reconcile_trades_period", {"apply": "1"})[0], 401)
        self.assertIsNone(chk("GET", "/api/jarvis/reconcile_trades_period", {"apply": "1"}, H_OK))

    def test_read_only_and_unlisted_paths_open(self):
        for m, path in [("GET", "/api/jarvis/history"), ("GET", "/api/watchlist"), ("GET", "/api/jarvis/knowledge"),
                        ("GET", "/api/positions/stock"), ("GET", "/api/stock/name"), ("POST", "/api/jarvis/memory"),
                        ("POST", "/api/jarvis/exit_decision"), ("GET", "/"), ("OPTIONS", "/api/jarvis/signal")]:
            self.assertIsNone(chk(m, path), (m, path))

    def test_unset_token_keeps_old_behavior(self):
        for m, path in self.PROTECTED:
            self.assertIsNone(chk(m, path, env={}), (m, path))
        self.assertIsNone(chk("POST", "/api/jarvis/signal", env={"DASHBOARD_ADMIN_TOKEN": ""}))


class TestWebhookSecret(unittest.TestCase):
    ENVW = {"TELEGRAM_WEBHOOK_SECRET": "whsec"}

    def test_mismatch_and_missing_403(self):
        self.assertEqual(chk("POST", "/api/telegram/webhook", env=self.ENVW)[0], 403)
        self.assertEqual(chk("POST", "/api/telegram/webhook", env=self.ENVW,
                             headers={"x-telegram-bot-api-secret-token": "bad"})[0], 403)

    def test_match_passes(self):
        self.assertIsNone(chk("POST", "/api/telegram/webhook", env=self.ENVW,
                              headers={"x-telegram-bot-api-secret-token": "whsec"}))

    def test_unset_secret_passes(self):
        self.assertIsNone(chk("POST", "/api/telegram/webhook", env={}))

    def test_admin_token_does_not_apply_to_webhook(self):
        env = {"DASHBOARD_ADMIN_TOKEN": TOK}
        self.assertIsNone(chk("POST", "/api/telegram/webhook", env=env))


class TestMiddlewareAsgi(unittest.IsolatedAsyncioTestCase):
    async def _call(self, method, path, headers=None, qs=b"", env=ENV):
        reached = []

        async def app(scope, receive, send):
            reached.append(1)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})
        sent = []

        async def send(msg):
            sent.append(msg)

        async def receive():
            return {"type": "http.request"}
        scope = {"type": "http", "method": method, "path": path, "query_string": qs,
                 "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]}
        with patch.dict("os.environ", env, clear=True):
            await api_auth.AdminAuthMiddleware(app)(scope, receive, send)
        return sent, reached

    async def test_denied_returns_401_json_and_does_not_reach_app(self):
        sent, reached = await self._call("POST", "/api/jarvis/signal")
        self.assertEqual(sent[0]["status"], 401)
        self.assertEqual(reached, [])
        self.assertFalse(json.loads(sent[1]["body"])["success"])
        self.assertNotIn(TOK.encode(), sent[1]["body"])

    async def test_allowed_reaches_app(self):
        sent, reached = await self._call("POST", "/api/jarvis/signal", {"X-Admin-Token": TOK})
        self.assertEqual(sent[0]["status"], 200)
        self.assertEqual(reached, [1])

    async def test_unset_env_reaches_app(self):
        sent, reached = await self._call("POST", "/api/jarvis/signal", env={})
        self.assertEqual(reached, [1])

    async def test_apply_query_is_checked(self):
        sent, reached = await self._call("GET", "/api/jarvis/reconcile_trades_period", qs=b"apply=1&confirm=x")
        self.assertEqual(sent[0]["status"], 401)

    async def test_webhook_403(self):
        sent, _ = await self._call("POST", "/api/telegram/webhook", env={"TELEGRAM_WEBHOOK_SECRET": "w"})
        self.assertEqual(sent[0]["status"], 403)

    async def test_non_http_scope_passthrough(self):
        called = []

        async def app(scope, receive, send):
            called.append(scope["type"])
        await api_auth.AdminAuthMiddleware(app)({"type": "lifespan"}, None, None)
        self.assertEqual(called, ["lifespan"])


class TestInternalCallHeaders(unittest.TestCase):
    def test_admin_headers(self):
        self.assertEqual(api_auth.admin_headers(ENV), {"X-Admin-Token": TOK})
        self.assertEqual(api_auth.admin_headers({}), {})

    def test_every_stock_trader_dashboard_call_sends_header(self):
        """stock_trader/main.py 의 DASHBOARD_URL 호출은 모두 headers=_dashboard_headers() 를 가져야 한다."""
        src = (REPO_ROOT / "stock_trader" / "main.py").read_text(encoding="utf-8")
        lines = src.split("\n")
        idx = [i for i, l in enumerate(lines) if "{DASHBOARD_URL}/api/" in l]
        self.assertGreaterEqual(len(idx), 5)
        for i in idx:
            window = "\n".join(lines[i:i + 8])
            self.assertIn("headers=_dashboard_headers()", window, lines[i].strip())

    def test_stock_trader_helper_reads_env(self):
        from tests import test_stock_trader_stop_loss  # noqa: F401  (asyncpg/redis/aiohttp 스텁 설치)
        import main as st
        with patch.dict("os.environ", ENV):
            self.assertEqual(st._dashboard_headers(), {"X-Admin-Token": TOK})
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(st._dashboard_headers(), {})

    def test_set_webhook_payload_includes_secret_only_when_set(self):
        from bot import telegram_bot
        with patch.dict("os.environ", {"TELEGRAM_WEBHOOK_SECRET": "w"}):
            self.assertEqual(telegram_bot._set_webhook_payload("https://x/y")["secret_token"], "w")
        with patch.dict("os.environ", {}, clear=True):
            self.assertNotIn("secret_token", telegram_bot._set_webhook_payload("https://x/y"))


class TestRouteDefinitions(unittest.TestCase):
    def test_reconcile_delete_is_post_only(self):
        src = (REPO_ROOT / "dashboard" / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.post("/api/jarvis/reconcile_trades/delete")', src)
        self.assertNotIn('api_route("/api/jarvis/reconcile_trades/delete"', src)

    def test_middleware_registered(self):
        src = (REPO_ROOT / "dashboard" / "main.py").read_text(encoding="utf-8")
        self.assertIn("app.add_middleware(api_auth.AdminAuthMiddleware)", src)


if __name__ == "__main__":
    unittest.main()
