"""
router/handlers/watchlist_handler.py - 감시종목 추가/삭제/조회 처리기

dashboard/main.py의 _handle_watchlist_command(구 3852행)와, _jarvis_chat_impl 내
"(종목) 감시/관심 추가" 확정 명령 정규식 블록(구 4361행)을 이관.
종목 식별은 market/universe.py의 Universe(name_cache/resolve_symbol)를 그대로 활용한다
— watchlist 테이블 자체에는 더 이상 조회 의존성이 없다(STOCK_NAME_MAP은 우선순위 매칭용 보조 사전).
"""
import logging
import re
from typing import Any, Optional

logger = logging.getLogger("router.handlers.watchlist")


async def add_symbol(pool: Any, symbol: str, name: str, *, priority: bool = False) -> None:
    """watchlist upsert 공용 헬퍼 — 정규식 확정추가 블록과 [[ACTION]] watch_add/watch_interest가 공유"""
    if priority:
        await _execute(pool, """
            INSERT INTO watchlist (symbol, name, is_active, priority) VALUES ($1, $2, TRUE, TRUE)
            ON CONFLICT (symbol) DO UPDATE SET is_active=TRUE, priority=TRUE, name=EXCLUDED.name
        """, symbol, name)
    else:
        await _execute(pool, """
            INSERT INTO watchlist (symbol, name, is_active) VALUES ($1, $2, TRUE)
            ON CONFLICT (symbol) DO UPDATE SET is_active=TRUE, name=EXCLUDED.name
        """, symbol, name)


async def _execute(pool: Any, query: str, *args) -> None:
    async with pool.acquire() as conn:
        await conn.execute(query, *args)


async def handle_command(msg: str, pool: Any, universe: Any, stock_name_map: dict) -> Optional[str]:
    """감시 종목 추가/삭제/조회 명령 감지 후 실행"""
    msg_lower = msg.lower().strip()

    # ── 조회 ──────────────────────────────────────────
    if any(k in msg_lower for k in ["감시 종목 보여", "감시종목 보여", "감시 종목 목록", "watchlist"]):
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE ORDER BY created_at")
            if not rows:
                return "📋 현재 감시 종목이 없어요."
            lines = [f"  {r['symbol']} {r['name'] or ''}" for r in rows]
            return "📋 **현재 감시 종목**\n" + "\n".join(lines)
        except Exception as e:
            return f"❌ 조회 실패: {e}"

    # ── 추가 ──────────────────────────────────────────
    is_add = any(k in msg_lower for k in ["감시 종목 추가", "감시종목 추가", "추가해줘", "추가해", "등록해", "감시해줘"])
    if is_add:
        # stock_name_map에서 종목명 매칭
        matched_symbol, matched_name = None, None
        for name_key, (symbol, name) in stock_name_map.items():
            if name_key in msg_lower:
                matched_symbol, matched_name = symbol, name
                break

        # 사전에 없으면 메모리 캐시에서 빠른 검색
        if not matched_symbol:
            # 정확한 종목명 매칭
            for stock_name, ticker in universe.name_cache.items():
                if stock_name in msg:
                    matched_symbol = ticker
                    matched_name = stock_name
                    break
            # 부분 매칭 (앞 2글자 이상)
            if not matched_symbol:
                for stock_name, ticker in universe.name_cache.items():
                    if len(stock_name) >= 2 and stock_name[:2] in msg and len(stock_name) >= 2:
                        words = [w for w in msg_lower.split() if len(w) >= 2]
                        if any(stock_name.startswith(w) or w in stock_name for w in words):
                            matched_symbol = ticker
                            matched_name = stock_name
                            break

        if matched_symbol:
            try:
                async with pool.acquire() as conn:
                    existing = [r["symbol"] for r in await conn.fetch(
                        "SELECT symbol FROM watchlist WHERE is_active=TRUE"
                    )]
                    if matched_symbol in existing:
                        return f"📋 **{matched_name}**({matched_symbol})은 이미 감시 종목이에요."
                    await conn.execute("""
                        INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                        VALUES ($1, $2, 'jarvis', $3, TRUE)
                        ON CONFLICT (symbol) DO UPDATE
                        SET is_active=TRUE, name=$2, added_by='jarvis', reason=$3, updated_at=NOW()
                    """, matched_symbol, matched_name, msg)
                return f"✅ **{matched_name}**({matched_symbol})을 감시 종목에 추가했어요!"
            except Exception as e:
                return f"❌ 추가 실패: {e}"

        # 6자리 코드 직접 입력
        codes = re.findall(r'\b\d{6}\b', msg)
        if codes:
            results = []
            for code in codes:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO watchlist (symbol, added_by, reason, is_active)
                            VALUES ($1, 'jarvis', $2, TRUE)
                            ON CONFLICT (symbol) DO UPDATE
                            SET is_active=TRUE, added_by='jarvis', updated_at=NOW()
                        """, code, msg)
                    results.append(f"✅ {code} 추가")
                except Exception as e:
                    results.append(f"❌ {code} 실패: {e}")
            return "\n".join(results)

        return None  # AI 대화로 넘김

    # ── 삭제/제거 ──────────────────────────────────────
    is_remove = any(k in msg_lower for k in ["감시 종목 제거", "감시종목 제거", "제거해줘", "삭제해줘", "빼줘"])
    if is_remove:
        for name_key, (symbol, name) in stock_name_map.items():
            if name_key in msg_lower:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", symbol
                        )
                    return f"🗑️ **{name}**({symbol})을 감시 종목에서 제거했어요."
                except Exception as e:
                    return f"❌ {name} 제거 실패: {e}"

        codes = re.findall(r'\b\d{6}\b', msg)
        if codes:
            results = []
            for code in codes:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", code
                        )
                    results.append(f"🗑️ {code} 제거")
                except Exception as e:
                    results.append(f"❌ {code} 실패: {e}")
            return "\n".join(results)

        return "❓ 종목명을 찾지 못했어요. 예: '삼성전자 감시 종목 제거해줘'"

    return None  # 일반 채팅으로 처리


async def handle_quick_add(user_msg: str, pool: Any, universe: Any, web_research_fn) -> Optional[str]:
    """확정 명령 "(종목) 감시/관심 추가": Open-WebUI를 거치지 않고 즉시 watchlist에 반영"""
    m = re.search(r"(.+?)\s*(감시|관심)\s*(종목)?\s*(추가|등록|넣어)", user_msg)
    if not m or "http" in user_msg:
        return None
    symbol, name = await universe.resolve_symbol(m.group(1))
    if symbol:
        try:
            await add_symbol(pool, symbol, name)
            return f"👁️ {name}({symbol}) 감시종목에 추가했어요. 다음 스캔부터 신호 감시합니다."
        except Exception as e:
            return f"❌ 감시 추가 실패: {str(e)[:80]}"
    q = m.group(1).strip()
    reply = await web_research_fn(q, name=q)
    return reply + "\n\n(시스템 종목코드를 못 찾아 감시 추가는 안 됐어요. 정식 종목명으로 다시 시도해보세요.)"
