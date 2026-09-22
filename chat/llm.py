"""
chat/llm.py - STARK v2 LLM(Open-WebUI) 호출 모듈

핵심 원칙:
1. 순수 LLM 호출 인터페이스 전담:
   - 이전 레거시 코드의 치명적 결함이었던 내부 _save_chat_history 자동 호출을 전면 제거.
   - 시스템 프롬프트(daily_plan, signal, status 등)가 사용자 대화 메모리로 오염 적재되는 것을 원천 차단.
2. 사고 과정([[FINAL]]) 분리 및 메타 마커 제거 프로토콜 유지.
3. Open-WebUI 호출 실패 시 fallback_fn(Gemini 등) 연동.
"""
import asyncio
import logging
import os
import re
from chat.memory import get_chat_history

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger("chat.llm")


async def ask_openwebui(
    message: str,
    session_id: str = "telegram",
    model: str = None,
    fallback_fn=None,
    pool=None,
    redis=None,
) -> str:
    """
    Open-WebUI 모델 호출 (순수 LLM 인터페이스).
    내부에서 대화 저장을 자동으로 수행하지 않음.
    """
    openwebui_url = os.getenv("OPENWEBUI_URL", "https://open-webui-production-5843.up.railway.app")
    openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
    jarvis_model = model or os.getenv("JARVIS_MODEL", "autotrader-jarvis")

    if not openwebui_token:
        if fallback_fn:
            return await fallback_fn(message)
        return "❌ OPENWEBUI_API_TOKEN 미설정"

    if not aiohttp:
        logger.warning("aiohttp 미설치 환경 — fallback_fn 시도")
        if fallback_fn:
            return await fallback_fn(message)
        return "❌ aiohttp 모듈이 설치되어 있지 않습니다."

    try:
        # 이전 대화 히스토리 로드 (순수 대화 세션)
        history = await get_chat_history(session_id, max_turns=8, pool=pool, redis=redis)

        # 현재 메시지 추가
        messages = history + [{"role": "user", "content": message}]

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openwebui_token}",
        }
        payload = {
            "model": jarvis_model,
            "messages": messages,
            "stream": False,
        }

        # 사고과정(THINK) 유출 차단 프로토콜 추가
        prompt_with_proto = (
            message
            + "\n\n[출력 프로토콜] 사고 과정이 필요하면 내부적으로만 하라. "
            "출력의 맨 마지막에 [[FINAL]] 표식 뒤에 최종 답변만 써라. "
            "[[FINAL]] 이전의 모든 내용은 사용자에게 표시되지 않는다."
        )
        payload["messages"][-1]["content"] = prompt_with_proto

        data = None
        last_err = None
        for _attempt in range(2):  # 1회 재시도
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{openwebui_url}/api/chat/completions",
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=90),
                    ) as res:
                        data = await res.json()
                if data and data.get("choices"):
                    break
                last_err = Exception(f"응답 없음: {data.get('error', data) if data else 'no data'}")
            except Exception as _e:
                last_err = _e
                await asyncio.sleep(2)

        if not data or not data.get("choices"):
            raise last_err or Exception("응답 없음")

        reply = data["choices"][0]["message"]["content"]

        # [[FINAL]] 이후만 사용 (사고과정 제거)
        final_m = re.search(r"\[\[\s*FINAL\s*\]\]", reply, re.IGNORECASE)
        if final_m:
            reply = reply[final_m.end():].strip()
        else:
            # 메타 유출 감지: THINK / [최종 답변 구성] / "규칙을 지킨다" 등
            head = reply.strip()[:300]
            meta_markers = ("THINK", "[최종", "답변 구성", "라고 답변", "규칙을 지킨")
            if any(m in head for m in meta_markers):
                parts = [p.strip() for p in reply.replace("\r", "").split("\n\n") if p.strip()]
                for cand in reversed(parts):
                    if not any(m in cand[:80] for m in meta_markers) and not cand.startswith('"'):
                        reply = cand
                        break

        # [STARK v2 중요]
        # 여기서 _save_chat_history를 호출하지 않는다!
        # 대화 저장은 실제 사용자 응답을 처리하는 상위 엔트리에서
        # 순수 발화 원문과 답변만 선별하여 저장한다.

        return reply

    except Exception as e:
        logger.error(f"Open-WebUI 호출 실패: {e}")
        if fallback_fn:
            return await fallback_fn(message)
        return f"❌ 호출 실패: {e}"
