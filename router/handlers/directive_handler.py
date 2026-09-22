"""
router/handlers/directive_handler.py - 지시사항 저장/목록/취소 처리기

dashboard/main.py의 _handle_directive_command(구 4130행)·_get_active_directives(구 4115행)·
_stamp_plan_change(구 4104행)를 이관. jarvis_notes(category='directive') 테이블을 그대로 사용하며,
pool/redis는 호출부(dashboard/main.py)가 들고 있는 것을 그대로 주입받는다
(market/universe.py, stark/decision_logger.py와 동일한 DI 관례).
"""
import logging
import re
from typing import Any, Optional

logger = logging.getLogger("router.handlers.directive")


async def get_active_directives(pool: Any, limit: int = 10) -> str:
    """활성 지시사항 텍스트 (판단·작전 프롬프트 주입용)"""
    if pool is None:
        return ""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, content FROM jarvis_notes
                WHERE category='directive' AND is_active=TRUE
                ORDER BY created_at DESC LIMIT $1""", limit)
        if not rows:
            return ""
        return "\n".join(f"- (#{r['id']}) {r['content']}" for r in rows)
    except Exception:
        return ""


async def stamp_plan_change(redis: Any, note: str) -> None:
    """지시 변경 시 오늘의 작전 상단에 변경 메모 삽입 → 이후 판단에서 옛 규칙 무력화"""
    from datetime import datetime, timedelta, timezone
    KST = timezone(timedelta(hours=9))
    try:
        cur = await redis.get("jarvis:daily_plan")
        cur = cur if isinstance(cur, str) else (cur or b"").decode()
        stamp = f"※ [{datetime.now(KST).strftime('%H:%M')} 지시 변경] {note} — 이전 작전의 상충 규칙은 무효.\n"
        await redis.setex("jarvis:daily_plan", 60 * 60 * 12, (stamp + cur)[:2000])
    except Exception:
        pass


async def handle(user_msg: str, pool: Any, redis: Any) -> Optional[str]:
    """지시사항 저장/목록/취소. 해당 없으면 None (일반 채팅으로 넘김)"""
    msg = user_msg.strip()

    # 목록
    if msg in ("지시 목록", "지시목록", "지시사항 목록", "지시사항"):
        txt = await get_active_directives(pool, 20)
        return f"📌 활성 지시사항:\n{txt}" if txt else "📌 활성 지시사항이 없습니다."

    # 취소: "지시 취소 12" / "지시 삭제 12"
    m = re.match(r"지시\s*(취소|삭제)\s*#?(\d+)", msg)
    if m:
        did = int(m.group(2))
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE jarvis_notes SET is_active=FALSE WHERE id=$1 AND category='directive'", did)
        await stamp_plan_change(redis, f"지시 #{did} 취소됨")
        return f"🗑️ 지시 #{did} 를 해제했습니다."

    # 저장: "지시: ..." / "지시 ..." / "앞으로 ..." / "내일부터 ..."
    directive = None
    if msg.startswith("지시:"):
        directive = msg[3:].strip()
    elif msg.startswith("지시 ") and len(msg) > 4:
        directive = msg[3:].strip()
    elif msg.startswith(("앞으로 ", "내일부터 ", "오늘부터 ")):
        directive = msg
    if directive and len(directive) >= 4:
        async with pool.acquire() as conn:
            did = await conn.fetchval(
                "INSERT INTO jarvis_notes (category, content, is_active) VALUES ('directive', $1, TRUE) RETURNING id",
                directive[:300])
        await stamp_plan_change(redis, f"지시 추가 #{did}: {directive[:80]}")
        return (f"📌 지시 #{did} 저장 완료 — 다음 매매 판단부터 즉시 반영됩니다.\n"
                f"\"{directive[:100]}\"\n(해제: '지시 취소 {did}')")
    return None


async def save_directive_with_conflict_resolution(pool: Any, redis: Any, content: str) -> Optional[dict]:
    """[[ACTION]] 프로토콜에서 AI가 판단한 지시를 저장.
    '해제/완화'성 지시면 같은 핵심어(예: 5만원)의 옛 지시를 함께 비활성화해 충돌을 막는다.
    반환: {"id": 신규지시id, "deactivated": [해제된 지시id 문자열들]} 또는 저장 안 했으면 None."""
    if not content or len(content) < 4:
        return None
    deactivated = []
    is_relax = any(k in content for k in ("해제", "취소", "풀", "허용", "포함", "완화", "없애")) or \
        (re.search(r"(초과|이상|넘)", content) and re.search(r"(제안|매수|가능|사도|살 수)", content))
    async with pool.acquire() as conn:
        if is_relax:
            keys = set(re.findall(r"\d+\s*만\s*원|\d+\s*종목|\d+\s*%", content))
            if keys:
                olds = await conn.fetch(
                    "SELECT id, content FROM jarvis_notes "
                    "WHERE category='directive' AND is_active=TRUE")
                for o in olds:
                    oc = o["content"].replace(" ", "")
                    if any(k.replace(" ", "") in oc for k in keys) and \
                       not any(x in o["content"] for x in ("해제", "취소", "허용", "포함")):
                        await conn.execute(
                            "UPDATE jarvis_notes SET is_active=FALSE WHERE id=$1", o["id"])
                        deactivated.append(f"#{o['id']}")
        did = await conn.fetchval(
            "INSERT INTO jarvis_notes (category, content, is_active) "
            "VALUES ('directive', $1, TRUE) RETURNING id", content[:300])
    await stamp_plan_change(
        redis, f"지시 추가 #{did}: {content[:80]}"
        + (f" / 해제: {', '.join(deactivated)}" if deactivated else ""))
    return {"id": did, "deactivated": deactivated}
