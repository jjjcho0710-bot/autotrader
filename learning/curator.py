"""
learning/curator.py - STARK v2 학습 지식 큐레이션 및 조회 모듈

기능:
1. get_jarvis_knowledge:
   - 신규 구조화 테이블 learning_rules (is_active=TRUE) 우선 조회
   - 없으면 jarvis_notes의 knowledge_core -> knowledge 순으로 폴백
2. jarvis_knowledge_curate:
   - 축적된 지식을 바탕으로 중복 통합, 카테고리 분류([진입]/[청산]/[리스크]/[습관])
   - 핵심 10~20개 정제본(core) 생성 후 저장 및 알림
"""
import logging
from typing import Optional

logger = logging.getLogger("learning.curator")


async def get_jarvis_knowledge(limit: int = 5, pool=None) -> str:
    """
    학습 지식 조회:
    1. STARK v2 신규 구조화 테이블 learning_rules (is_active=TRUE) 우선
    2. 없으면 jarvis_notes의 core(정제본 10개)
    3. 없으면 jarvis_notes의 최근 raw N건
    """
    if not pool:
        return ""

    try:
        async with pool.acquire() as conn:
            # 1순위: learning_rules 활성 규칙 조회
            try:
                rules = await conn.fetch(
                    """
                    SELECT rule_id, rule_text
                    FROM learning_rules
                    WHERE is_active = TRUE
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
                if rules:
                    return "\n".join(f"- R{r['rule_id']} {r['rule_text']}" for r in rules)
            except Exception as re:
                logger.debug(f"learning_rules 조회 폴백: {re}")

            # 2순위: 레거시 core 20개
            core = await conn.fetch(
                """
                SELECT id, content FROM jarvis_notes
                WHERE category='knowledge_core' AND is_active=TRUE
                ORDER BY id LIMIT 20
                """
            )
            if core:
                return "\n".join(f"- K{r['id']} {r['content']}" for r in core)

            # 3순위: 레거시 raw N건
            rows = await conn.fetch(
                """
                SELECT content FROM jarvis_notes
                WHERE category='knowledge' AND is_active=TRUE
                ORDER BY created_at DESC LIMIT $1
                """,
                limit,
            )
            if rows:
                return "\n".join(f"- {r['content']}" for r in rows)

        return ""
    except Exception as e:
        logger.error(f"지식 로드 오류: {e}")
        return ""


async def get_position_management_principles(limit: int = 10, pool=None) -> str:
    """보유중 종목 추가매수(물타기) 원칙 조회.

    jarvis_notes의 category='position_management'(is_active=TRUE)에서만 가져온다.
    신규 진입 판단용 지식(get_jarvis_knowledge, knowledge_core)과는 완전히 별도로
    관리해서, 프롬프트에서도 서로 다른 섹션으로 구분해 보여줄 수 있게 한다
    (진입 원칙을 보유중 종목 추가매수 근거로 오인용하는 것을 막기 위함)."""
    if not pool:
        return ""

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, content FROM jarvis_notes
                WHERE category='position_management' AND is_active=TRUE
                ORDER BY id LIMIT $1
                """,
                limit,
            )
        if rows:
            return "\n".join(f"- P{r['id']} {r['content']}" for r in rows)
        return ""
    except Exception as e:
        logger.error(f"보유중 종목 추가매수 원칙 로드 오류: {e}")
        return ""


async def jarvis_knowledge_curate(
    pool=None,
    ask_llm_fn=None,
    send_telegram_fn=None,
) -> str:
    """
    지식 정리: 중복 통합·상충 해소·카테고리 분류 → 핵심 10~20개 정제본(core) 저장
    """
    if not pool:
        return ""

    try:
        async with pool.acquire() as conn:
            # learning_rules 우선 수집 후 부족하면 jarvis_notes 수집
            raw_rules = []
            try:
                lr_rows = await conn.fetch(
                    """
                    SELECT rule_id, rule_text FROM learning_rules
                    WHERE is_active = TRUE
                    ORDER BY created_at DESC LIMIT 50
                    """
                )
                raw_rules = [f"- [신규] {r['rule_text']}" for r in lr_rows]
            except Exception:
                pass

            jn_rows = await conn.fetch(
                """
                SELECT id, content FROM jarvis_notes
                WHERE category='knowledge' AND is_active=TRUE
                ORDER BY created_at DESC LIMIT 80
                """
            )
            raw_rules.extend(f"- {r['content']}" for r in jn_rows)

        if len(raw_rules) < 3:
            logger.info("📚 지식 정리: 자료 부족 — 스킵")
            return ""

        raw = "\n".join(raw_rules[:80])

        # 원칙 성과 통계 (지난 정제본의 실전 적중률)
        stats_txt = "(아직 없음)"
        try:
            async with pool.acquire() as conn:
                st = await conn.fetch(
                    """
                    SELECT ps.principle_id, ps.applied, ps.hits, n.content
                    FROM principle_stats ps JOIN jarvis_notes n ON n.id = ps.principle_id
                    WHERE ps.applied > 0 ORDER BY ps.applied DESC LIMIT 30
                    """
                )
            if st:
                # (적용, 적중)이 완전히 같은 원칙들은 매번 같은 판단에 함께 인용되어 통계가
                # 사실상 중복 집계된 것이므로, 한 줄로 합쳐서 보여준다(K79/K80/K82/K83이
                # 전부 43%로 찍히는 식의 착시 방지)
                groups: dict[tuple, list] = {}
                for r in st:
                    groups.setdefault((r["applied"], r["hits"]), []).append(r)
                lines = []
                for (applied, hits), rs in sorted(groups.items(), key=lambda kv: -kv[0][0]):
                    ids = "/".join(f"K{r['principle_id']}" for r in rs)
                    content = rs[0]["content"][:40]
                    lines.append(
                        f"- {ids} {content}: 적용 {applied}회, 적중 {hits}회 "
                        f"({hits/max(1, applied)*100:.0f}%)"
                    )
                stats_txt = "\n".join(lines)
        except Exception:
            pass

        prompt = f"""다음은 자동매매 AI가 여러 자료에서 학습한 매매 원칙 목록이다.

{raw}

작업:
1) 중복·유사 원칙은 하나로 통합, 서로 상충하는 것은 더 보수적/검증된 쪽을 택하라.
2) 각 원칙을 [진입]/[청산]/[리스크]/[습관] 중 하나로 분류하라.
3) 분류별로 실전 판단에 가장 유용한 원칙을 최대 5개씩(총 최대 20개) 남겨라.
   원칙이 부족하면 있는 만큼만 출력하고, 빈 자리를 "(없음)"·"추후 추가 필요" 같은 자리채움 문구로 절대 채우지 마라.
   아래 [원칙 성과 통계]가 있으면 적중률 낮은 원칙은 제외하고 높은 원칙은 반드시 유지하라.
출력 형식: 각 줄 "[분류] 원칙 내용(40자 이내)" — 다른 말 없이 실제 원칙 줄만.

[원칙 성과 통계 — 지난 정제본 기준]
{stats_txt}"""

        out = ""
        if ask_llm_fn:
            out = await ask_llm_fn(prompt, session_id="daily_plan")

        if not out or out.startswith("❌"):
            return ""

        placeholder = ("없음", "추후", "추가 필요", "해당 없", "N/A", "없습니다")
        lines = [
            ln.strip() for ln in out.split("\n")
            if ln.strip().startswith("[") and len(ln.strip()) > 6
            and not any(pz in ln for pz in placeholder)
        ][:20]

        if not lines:
            return ""

        async with pool.acquire() as conn:
            await conn.execute("UPDATE jarvis_notes SET is_active=FALSE WHERE category='knowledge_core'")
            for ln in lines:
                await conn.execute(
                    "INSERT INTO jarvis_notes (category, content, is_active) VALUES ('knowledge_core', $1, TRUE)",
                    ln[:200],
                )

        msg = f"📚 지식 정리 완료: {len(raw_rules)}개 → 핵심 {len(lines)}개\n" + "\n".join(lines)
        if stats_txt != "(아직 없음)":
            msg += "\n\n📊 원칙 성과(적용순)\n" + stats_txt[:600]

        if send_telegram_fn:
            await send_telegram_fn(msg, dest="personal")

        return msg

    except Exception as e:
        logger.error(f"지식 정리 오류: {e}")
        return ""
