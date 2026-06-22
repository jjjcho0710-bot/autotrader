"""
뉴스 감성 분석 수집기
네이버 금융 뉴스 RSS + Gemini 감성 분석
"""
import logging
import aiohttp
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from common.config import config
from common.database import db

logger = logging.getLogger(__name__)

# 종목코드 → 네이버 금융 종목명 매핑
STOCK_NAME_MAP = {
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "035420": "NAVER",
    "035720": "카카오",
    "005380": "현대차",
    "068270": "셀트리온",
    "373220": "LG에너지솔루션",
    "323410": "카카오뱅크",
    "207940": "삼성바이오로직스",
    "051910": "LG화학",
    "006400": "삼성SDI",
    "005490": "POSCO홀딩스",
    "000270": "기아",
    "096770": "SK이노베이션",
    "015760": "한국전력",
    "017670": "SK텔레콤",
    "030200": "KT",
    "105560": "KB금융",
    "055550": "신한지주",
    "086790": "하나금융지주",
}


class NewsCollector:
    """뉴스 수집 + Gemini 감성 분석"""

    async def get_news(self, symbol: str, limit: int = 5) -> list:
        """네이버 금융 뉴스 RSS 수집"""
        name = STOCK_NAME_MAP.get(symbol, symbol)
        try:
            url = f"https://finance.naver.com/item/news_news.naver?code={symbol}&page=1&sm=title_entity_id.basic&clusterId="
            headers = {"User-Agent": "Mozilla/5.0"}

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    html = await resp.text()

            # 뉴스 제목 추출
            import re
            titles = re.findall(r'<title[^>]*>(.*?)</title>', html)
            links = re.findall(r'href="(/item/news_read[^"]+)"', html)

            news_list = []
            for i, title in enumerate(titles[:limit]):
                title = re.sub(r'<[^>]+>', '', title).strip()
                if title and len(title) > 5:
                    news_list.append({
                        "title": title,
                        "symbol": symbol,
                        "name": name,
                    })

            return news_list

        except Exception as e:
            logger.debug(f"뉴스 수집 실패 [{symbol}]: {e}")
            return []

    async def get_news_rss(self, query: str, limit: int = 5) -> list:
        """Open-WebUI 웹 검색으로 뉴스 수집"""
        try:
            import os
            openwebui_url = os.getenv("OPENWEBUI_URL", "")
            openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
            jarvis_model = os.getenv("JARVIS_MODEL", "autotrader-jarvis")

            if not openwebui_url or not openwebui_token:
                return []

            prompt = f"{query} 관련 최신 뉴스 5개 제목만 간단히 알려줘. 번호 매겨서."

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{openwebui_url}/api/chat/completions",
                    headers={
                        "Authorization": f"Bearer {openwebui_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": jarvis_model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    data = await resp.json()

            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                return []

            # 뉴스 제목 파싱
            import re
            lines = [l.strip() for l in content.split('\n') if l.strip()]
            news_list = []
            for line in lines[:limit]:
                # 번호 제거
                title = re.sub(r'^[\d\.\)]+\s*', '', line).strip()
                if title and len(title) > 5:
                    news_list.append({
                        "title": title,
                        "description": "",
                        "pubDate": "",
                    })
            return news_list

        except Exception as e:
            logger.debug(f"뉴스 수집 실패: {e}")
            return []

    async def _get_naver_news(self, query: str, limit: int = 5) -> list:
        return []

    async def analyze_sentiment(self, news_list: list, symbol: str) -> dict:
        """Open-WebUI로 뉴스 감성 분석"""
        if not news_list:
            return {"score": 0, "signal": "NEUTRAL", "reason": "뉴스 없음"}

        try:
            import os
            openwebui_url = os.getenv("OPENWEBUI_URL", "")
            openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
            jarvis_model = os.getenv("JARVIS_MODEL", "autotrader-jarvis")

            name = STOCK_NAME_MAP.get(symbol, symbol)
            titles = "\n".join([f"- {n['title']}" for n in news_list[:5]])

            prompt = f"""다음 주식 뉴스를 분석해서 JSON만 출력해줘. 다른 말은 하지 마.

종목: {name}({symbol})
뉴스:
{titles}

출력형식 (JSON만, 다른 텍스트 없이):
{{"score": 1, "signal": "BUY", "summary": "긍정적"}}"""

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{openwebui_url}/api/chat/completions",
                    headers={
                        "Authorization": f"Bearer {openwebui_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": jarvis_model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    data = await resp.json()

            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                return {"score": 0, "signal": "NEUTRAL", "reason": "응답 없음"}

            import json, re
            # JSON 블록 추출 (```json ... ``` 포함)
            content = re.sub(r'```json|```', '', content).strip()
            match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
            if match:
                try:
                    result = json.loads(match.group())
                    score = int(result.get("score", 0))
                    score = max(-2, min(2, score))  # -2~2 범위 제한
                    signal = result.get("signal", "NEUTRAL").upper()
                    if signal not in ["BUY", "SELL", "NEUTRAL"]:
                        signal = "NEUTRAL"
                    return {
                        "score": score,
                        "signal": signal,
                        "reason": result.get("summary", ""),
                    }
                except:
                    pass

            # JSON 파싱 실패시 텍스트에서 감성 추출
            if any(w in content for w in ["긍정", "상승", "매수", "호재"]):
                return {"score": 1, "signal": "BUY", "reason": "긍정적 뉴스"}
            elif any(w in content for w in ["부정", "하락", "매도", "악재"]):
                return {"score": -1, "signal": "SELL", "reason": "부정적 뉴스"}
            return {"score": 0, "signal": "NEUTRAL", "reason": "중립적 뉴스"}

    async def collect_and_save(self, symbols: list[str]) -> dict:
        """전체 종목 뉴스 수집 + 감성 분석 + DB 저장"""
        logger.info(f"📰 뉴스 감성 분석 시작 — {len(symbols)}종목")
        results = {}

        for symbol in symbols:
            try:
                name = STOCK_NAME_MAP.get(symbol, symbol)
                # 뉴스 수집
                news = await self.get_news_rss(f"{name} 주식", limit=5)
                if not news:
                    continue

                # 감성 분석
                sentiment = await self.analyze_sentiment(news, symbol)

                # DB 저장
                async with db.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO stock_news_sentiment
                        (symbol, date, sentiment_score, signal, summary, news_count)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (symbol, date) DO UPDATE
                        SET sentiment_score=$3, signal=$4, summary=$5, news_count=$6
                    """,
                        symbol,
                        datetime.now().date(),
                        sentiment["score"],
                        sentiment["signal"],
                        sentiment["reason"],
                        len(news),
                    )

                results[symbol] = sentiment
                logger.info(f"  📰 {name}: {sentiment['signal']} ({sentiment['score']:+d}) - {sentiment['reason']}")

            except Exception as e:
                logger.debug(f"뉴스 처리 실패 [{symbol}]: {e}")
                continue

        logger.info(f"✅ 뉴스 감성 분석 완료: {len(results)}종목")
        return results

    async def get_sentiment_score(self, symbol: str) -> dict:
        """종목 감성 점수 조회"""
        try:
            async with db.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT sentiment_score, signal, summary, date
                    FROM stock_news_sentiment
                    WHERE symbol=$1
                    ORDER BY date DESC LIMIT 1
                """, symbol)

            if not row:
                return {"score": 0, "signal": "NEUTRAL", "reason": "뉴스 데이터 없음"}

            # 3일 이상 지난 데이터면 무시
            if (datetime.now().date() - row["date"]).days > 3:
                return {"score": 0, "signal": "NEUTRAL", "reason": "뉴스 데이터 오래됨"}

            return {
                "score": row["sentiment_score"],
                "signal": row["signal"],
                "reason": row["summary"],
            }
        except Exception as e:
            return {"score": 0, "signal": "NEUTRAL", "reason": f"조회 실패: {e}"}
