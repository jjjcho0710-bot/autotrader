"""
router/handlers/knowledge_handler.py - 학습 지식 조회/삭제 처리기

dashboard/main.py의 _jarvis_chat_impl 내 지식 목록(구 4379행)·지식 삭제(구 4394행) 블록을 이관.
원본은 jarvis_notes(category='knowledge') 테이블만 직접 조회했으나, learning/repository.py가
관리하는 신규 구조화 테이블 learning_rules는 전혀 노출되지 않는 문제가 있었다
(learning/curator.py의 판단용 지식 주입에는 이미 learning_rules가 반영되고 있었음에도
사람이 "지식 보여줘"로 조회하면 안 보였다). learning/repository.get_knowledge_entries로
두 출처를 합쳐 노출하고, 삭제도 두 테이블 모두 지원한다.
"""
import logging
import re
from typing import Any, Optional

from learning.repository import deactivate_knowledge_entry, get_knowledge_entries

logger = logging.getLogger("router.handlers.knowledge")

_LIST_RE = re.compile(r"(학습|배운|지식).*(내용|목록|뭐|알려|정리|보여)")
_DELETE_RE = re.compile(r"지식\s*삭제\s*(R?\d+)", re.I)


async def handle_list(user_msg: str, pool: Any) -> Optional[str]:
    """학습 지식 목록 (확정 명령, AI 미경유)"""
    if "http" in user_msg or not _LIST_RE.search(user_msg):
        return None
    try:
        entries = await get_knowledge_entries(limit=20, pool=pool)
    except Exception as e:
        logger.warning(f"지식 목록 오류: {e}")
        return None
    if not entries:
        return "📚 아직 학습한 자료가 없어요. URL과 함께 '이거 배워'라고 보내주세요."
    lines = [f"{e['display_id']} {e['text']}" for e in entries]
    return ("📚 학습한 매매 원칙 (" + str(len(entries)) + "건)\n" + "\n".join(lines)
            + "\n\n(제외: '지식 삭제 N' 또는 '지식 삭제 RN')")


async def handle_delete(user_msg: str, pool: Any) -> Optional[str]:
    """지식 삭제 N / 지식 삭제 RN"""
    m = _DELETE_RE.search(user_msg)
    if not m:
        return None
    display_id = m.group(1)
    try:
        ok = await deactivate_knowledge_entry(display_id, pool=pool)
    except Exception:
        ok = False
    if ok:
        return f"🗑️ 지식 #{display_id} 제외했어요."
    return None  # 못 찾은 경우 원본과 동일하게 조용히 다음 라우팅 단계로 넘김
