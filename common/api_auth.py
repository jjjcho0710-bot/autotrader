"""대시보드 변경성 API 관리자 토큰 인증 + 텔레그램 webhook secret 검증 ([AT] feat/api-auth-phase1).

- DASHBOARD_ADMIN_TOKEN 이 설정돼 있을 때만 활성. 보호 경로는 X-Admin-Token 헤더가 토큰과
  hmac.compare_digest 로 일치해야 하며 누락/불일치는 401. 미설정이면 기존 동작 그대로(통과).
- TELEGRAM_WEBHOOK_SECRET 이 설정돼 있으면 /api/telegram/webhook 요청의
  X-Telegram-Bot-Api-Secret-Token 헤더를 검증, 불일치/누락은 403.
- 순수 ASGI 미들웨어(AdminAuthMiddleware)라 FastAPI 의존이 없고 SSE 스트리밍에 영향이 없다.
- 요청 시점마다 환경변수를 읽는다(재시작 없이 값 변경 반영은 아니지만 테스트가 쉽다).
토큰 값은 로그·응답에 절대 남기지 않는다."""
import hmac
import json
import logging
import os
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import parse_qs

logger = logging.getLogger("common.api_auth")

ADMIN_TOKEN_ENV = "DASHBOARD_ADMIN_TOKEN"
ADMIN_HEADER = "x-admin-token"
WEBHOOK_SECRET_ENV = "TELEGRAM_WEBHOOK_SECRET"
WEBHOOK_SECRET_HEADER = "x-telegram-bot-api-secret-token"
WEBHOOK_PATH = "/api/telegram/webhook"

# 메서드 무관하게 보호하는 경로(정확히 일치)
PROTECTED_ANY_METHOD = frozenset({
    "/api/trade/execute",
    "/api/jarvis/signal",
    "/api/jarvis/chat",
    "/api/jarvis/action",
    "/api/strategies/update",
    "/api/strategies/add",
    "/api/strategy/fix_units",
    "/api/jarvis/reconcile_trades/delete",
    "/api/watchlist/cleanup",
    "/api/jarvis/knowledge/delete",
    "/api/jarvis/knowledge/clean_bad_lessons",
    "/api/jarvis/knowledge/clean_placeholders",
})
# (메서드, 경로) 단위 보호
PROTECTED_METHOD_PATH = frozenset({
    ("DELETE", "/api/jarvis/history"),
    ("POST", "/api/watchlist"),
})
# 메서드 + 경로 접두사 보호: DELETE /api/watchlist/{symbol}
PROTECTED_METHOD_PREFIX = (("DELETE", "/api/watchlist/"),)


def _norm_path(path: str) -> str:
    p = path or "/"
    return p.rstrip("/") or "/"


def _truthy_apply(value: Optional[str]) -> bool:
    return (value or "").strip().lower() not in ("", "0", "false", "no")


def is_protected(method: str, path: str, query: Mapping[str, str]) -> bool:
    m, p = (method or "").upper(), _norm_path(path)
    if p in PROTECTED_ANY_METHOD:
        return True
    if (m, p) in PROTECTED_METHOD_PATH:
        return True
    if any(m == pm and p.startswith(pref) for pm, pref in PROTECTED_METHOD_PREFIX):
        return True
    # 정산 보정 적용(apply=1)만 보호 — 드라이런은 읽기 전용이라 열어 둔다.
    if p == "/api/jarvis/reconcile_trades_period" and _truthy_apply(query.get("apply")):
        return True
    return False


def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").encode("utf-8"), (b or "").encode("utf-8"))


def check_request(method: str, path: str, query: Mapping[str, str],
                  headers: Mapping[str, str], env: Optional[Mapping[str, str]] = None
                  ) -> Optional[Tuple[int, str]]:
    """None = 통과, (상태코드, 사유) = 거부. headers 키는 소문자여야 한다."""
    env = os.environ if env is None else env
    m, p = (method or "").upper(), _norm_path(path)
    if m == "OPTIONS":
        return None  # CORS preflight

    secret = env.get(WEBHOOK_SECRET_ENV, "")
    if secret and p == WEBHOOK_PATH:
        if not _eq(headers.get(WEBHOOK_SECRET_HEADER, ""), secret):
            logger.warning("텔레그램 webhook secret 불일치/누락 — 403")
            return 403, "invalid webhook secret"
        return None

    token = env.get(ADMIN_TOKEN_ENV, "")
    if not token:
        return None  # 미설정 = 기존 동작 유지
    if not is_protected(m, p, query):
        return None
    if not _eq(headers.get(ADMIN_HEADER, ""), token):
        logger.warning(f"관리자 토큰 불일치/누락 — 401: {m} {p}")
        return 401, "unauthorized: X-Admin-Token required"
    return None


class AdminAuthMiddleware:
    """순수 ASGI 미들웨어."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            headers = {k.decode("latin-1").lower(): v.decode("latin-1")
                       for k, v in scope.get("headers", [])}
            qs = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            query = {k: v[0] for k, v in qs.items() if v}
            denial = check_request(scope.get("method", "GET"), scope.get("path", "/"), query, headers)
            if denial:
                status, msg = denial
                body = json.dumps({"success": False, "error": msg}).encode()
                await send({"type": "http.response.start", "status": status, "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode())]})
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)


def admin_headers(env: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """내부 호출(stock_trader → dashboard)에 붙일 헤더. 토큰 미설정이면 빈 dict."""
    env = os.environ if env is None else env
    token = env.get(ADMIN_TOKEN_ENV, "")
    return {"X-Admin-Token": token} if token else {}


def log_startup_warnings(log: Any = None) -> None:
    log = log or logger
    if not os.environ.get(ADMIN_TOKEN_ENV):
        log.warning("⚠️ DASHBOARD_ADMIN_TOKEN 미설정 — 변경성 API가 인증 없이 열려 있습니다(기존 동작)")
    if not os.environ.get(WEBHOOK_SECRET_ENV):
        log.warning("⚠️ TELEGRAM_WEBHOOK_SECRET 미설정 — 텔레그램 webhook 요청을 검증하지 않습니다")
