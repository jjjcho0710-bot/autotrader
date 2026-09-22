"""
learning/collector.py - STARK v2 학습 데이터 수집 및 원문 전문 보존 파이프라인

기능:
1. extract_youtube_id: 유튜브 URL 파싱
2. fetch_learning_text: 유튜브 자막 또는 웹 문서 본문 추출
3. gemini_watch_youtube: 자막 차단 시 Gemini 비디오 시청 폴백
4. learn_from_url:
   - 원문 전문(자막/본문)을 learning_sources 테이블에 영구 보존
   - LLM(ask_llm_fn)을 통해 핵심 원칙 추출
   - 추출된 원칙을 learning_rules 테이블에 외래키(source_id)와 함께 저장
   - 레거시 호환을 위해 jarvis_notes에도 보존
"""
import asyncio
import logging
import os
import re
import ssl
from typing import Tuple, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

from learning.repository import save_learning_source, save_learning_rules

logger = logging.getLogger("learning.collector")

LEARN_PROMPT_BASE = """이 주식 투자 학습 자료의 내용을 바탕으로,
자비스(자동매매 AI)가 실전 매수·매도 판단에 적용할 수 있는 핵심 원칙을
정확히 3~5개, 각 1줄(40자 이내)로 뽑아라. 각 줄은 "원칙: "으로 시작.
근거 없는 낙관·종목 추천·광고성 내용은 제외하라."""


def extract_youtube_id(url: str) -> str:
    """유튜브 URL에서 비디오 ID 추출"""
    m = re.search(r"(?:v=|youtu\.be/|shorts/|embed/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else ""


async def fetch_learning_text(url: str) -> Tuple[str, str]:
    """URL → (제목힌트, 본문텍스트). 유튜브는 자막, 그 외는 웹 본문"""
    vid = extract_youtube_id(url)
    if vid:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi

            def _get():
                try:
                    api = YouTubeTranscriptApi()
                    if hasattr(api, "fetch"):
                        try:
                            ft = api.fetch(vid, languages=["ko", "en"])
                        except Exception:
                            tl = api.list(vid)
                            try:
                                ft = tl.find_transcript(["ko"]).fetch()
                            except Exception:
                                ft = tl.find_generated_transcript(["ko", "en"]).fetch()
                        return " ".join(getattr(x, "text", None) or x.get("text", "") for x in ft)
                except TypeError:
                    pass
                tl = YouTubeTranscriptApi.list_transcripts(vid)
                try:
                    t = tl.find_transcript(["ko"])
                except Exception:
                    t = tl.find_generated_transcript(["ko", "en"])
                return " ".join(x["text"] for x in t.fetch())

            loop = asyncio.get_event_loop()
            txt = await loop.run_in_executor(None, _get)
            return (f"유튜브 {vid}", txt)
        except Exception as e:
            raise RuntimeError(f"자막을 가져올 수 없어요 (자막 없는 영상이거나 차단): {str(e)[:80]}")

    # 일반 웹
    if not aiohttp:
        raise RuntimeError("aiohttp 모듈이 설치되어 있지 않아 웹 페이지를 가져올 수 없습니다.")

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx)) as sess:
            r = await sess.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=12),
            )
            html = await r.text()
        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
        title = (re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I) or [None, ""])[1]
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        return ((title or "웹 문서").strip()[:60], text)
    except Exception as e:
        raise RuntimeError(f"페이지를 읽을 수 없어요: {str(e)[:80]}")


async def gemini_watch_youtube(url: str, prompt: str) -> str:
    """Gemini API로 유튜브 영상 직접 시청·요약 (자막 차단 시 폴백)"""
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY 미설정 — 영상 직접 시청 불가")
    if not aiohttp:
        raise RuntimeError("aiohttp 모듈 미설정 — Gemini API 호출 불가")

    body = {
        "contents": [
            {
                "parts": [
                    {"file_data": {"file_uri": url}},
                    {"text": prompt},
                ]
            }
        ]
    }
    async with aiohttp.ClientSession() as sess:
        r = await sess.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
            params={"key": key},
            json=body,
            timeout=aiohttp.ClientTimeout(total=180),
        )
        data = await r.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        raise RuntimeError(f"Gemini 시청 실패: {str(data)[:120]}")


async def learn_from_url(
    url: str,
    hint: str = "",
    pool=None,
    ask_llm_fn=None,
) -> str:
    """
    URL 학습:
    1. 자막/본문 텍스트 추출 (또는 Gemini 비디오 직접 시청)
    2. [STARK v2 핵심] 원문 전문을 learning_sources 테이블에 영구 보존
    3. 원칙 요약 추출 후 learning_rules 테이블에 외래키(source_id)와 함께 저장
    4. jarvis_notes 레거시 호환 저장
    """
    learn_prompt = LEARN_PROMPT_BASE
    if hint:
        learn_prompt += f"\n특히 주인이 강조한 관점: {hint}"

    title, text = "", ""
    vid = extract_youtube_id(url)
    source_type = "YOUTUBE" if vid else "WEB_ARTICLE"

    try:
        title, text = await fetch_learning_text(url)
    except Exception as fe:
        if vid:
            # 자막 차단/없음 → Gemini 영상 직접 시청 폴백
            clean_url = f"https://www.youtube.com/watch?v={vid}"
            try:
                out = await gemini_watch_youtube(clean_url, learn_prompt)
            except Exception as ge:
                logger.error(f"Gemini 영상 시청 실패: {ge}")
                return f"❌ 학습 실패 (자막 및 영상 분석 모두 실패): {ge}"

            principles = [
                ln.strip() for ln in out.split("\n")
                if "원칙" in ln and len(ln.strip()) > 6
            ][:5]
            if not principles:
                return "⚠️ 영상에서 유효한 원칙을 추출하지 못했어요."

            # learning_sources 및 learning_rules 저장
            source_id = await save_learning_source(
                source_type="YOUTUBE",
                url=clean_url,
                content_raw=out,  # 영상 시청 결과 텍스트를 원문 대용으로 보존
                title=f"유튜브 {vid}",
                hint=hint,
                pool=pool,
            )
            if source_id:
                await save_learning_rules(source_id, principles, pool=pool)

            # 레거시 jarvis_notes 호환
            if pool:
                try:
                    async with pool.acquire() as conn:
                        for p_ in principles:
                            await conn.execute(
                                "INSERT INTO jarvis_notes (category, content, is_active) VALUES ('knowledge', $1, TRUE)",
                                f"[유튜브 {vid}] {p_[:200]}",
                            )
                except Exception as ne:
                    logger.debug(f"jarvis_notes 저장 예외: {ne}")

            return (
                f"📚 학습 완료 (영상 직접 시청) — 지식 {len(principles)}건 저장 (원문 전문 보존 완료)\n"
                + "\n".join(principles)
                + "\n(이후 매수 판단·작전에 반영됩니다)"
            )
        raise fe

    if len(text) < 200:
        return "⚠️ 학습할 내용이 너무 적어요 (자막/본문 부족)."

    # [STARK v2 핵심] 원문 전문을 자르지 않고 learning_sources에 먼저 영구 보존
    source_id = await save_learning_source(
        source_type=source_type,
        url=url,
        content_raw=text,
        title=title,
        hint=hint,
        pool=pool,
    )

    # LLM 요약용으로는 18,000자로 슬라이싱
    summary_text = text[:18000]
    prompt = f"""{learn_prompt}

[자료 내용 — {title}]
{summary_text}"""

    out = ""
    if ask_llm_fn:
        out = await ask_llm_fn(prompt, session_id="daily_plan")

    if not out or out.startswith("❌"):
        return "❌ 요약에 실패했어요."

    principles = [
        ln.strip() for ln in out.split("\n")
        if "원칙" in ln and len(ln.strip()) > 6
    ][:5]
    if not principles:
        return "⚠️ 유효한 원칙을 추출하지 못했어요."

    # 추출된 원칙을 learning_rules에 저장
    if source_id:
        await save_learning_rules(source_id, principles, pool=pool)

    # 레거시 jarvis_notes 호환 저장
    if pool:
        try:
            async with pool.acquire() as conn:
                for p_ in principles:
                    await conn.execute(
                        "INSERT INTO jarvis_notes (category, content, is_active) VALUES ('knowledge', $1, TRUE)",
                        f"[{title[:30]}] {p_[:200]}",
                    )
        except Exception as ne:
            logger.debug(f"jarvis_notes 저장 예외: {ne}")

    return (
        f"📚 학습 완료 — 지식 {len(principles)}건 저장 (원문 전문 보존 ID: {source_id or 'DB미연결'})\n"
        + "\n".join(principles)
        + "\n(이후 매수 판단·작전에 반영됩니다)"
    )
