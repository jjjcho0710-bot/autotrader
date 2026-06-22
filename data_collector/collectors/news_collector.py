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
        """네이버 뉴스 검색 RSS"""
        try:
            url = "https://openapi.naver.com/v1/search/news.json"
            headers = {
                "X-Naver-Client-Id": config.NAVER_CLIENT_ID if hasattr(config, 'NAVER_CLIENT_ID') else "",
                "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET if hasattr(config, 'NAVER_CLIENT_SECRET') else "",
            }
            import os
            headers["X-Naver-Client-Id"] = os.getenv("NAVER_CLIENT_ID", "")
            headers["X-Naver-Client-Secret"] = os.getenv("NAVER_CLIENT_SECRET", "")

            if not headers["X-Naver-Client-Id"]:
                # API 키 없으면 RSS로 대체
                return await self._get_news_simple(query, limit)

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    params={"query": query, "display": limit, "sort": "date"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()

            return [
                {"title": item.get("title", "").replace("<b>", "").replace("</b>", ""),
                 "description": item.get("description", ""),
                 "pubDate": item.get("pubDate", "")}
                for item in data.get("items", [])
            ]

        except Exception as e:
            logger.debug(f"네이버 뉴스 검색 실패: {e}")
            return await self._get_news_simple(query, limit)

    async def _get_news_simple(self, query: str, limit: int = 5) -> list:
        """Jarvis 웹 검색으로 뉴스 수집 (fallback)"""
        try:
            import aiohttp
            url = f"https://search.naver.com/search.naver?where=news&query={query}+주식&sort=1"
            headers = {"User-Agent": "Mozilla/5.0"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    html = await resp.text()

            import re
            titles = re.findall(r'class="news_tit"[^>]*title="([^"]+)"', html)
            return [{"title": t, "description": "", "pubDate": ""} for t in titles[:limit]]
        except:
            return []

    async def analyze_sentiment(self, news_list: list, symbol: str) -> dict:
        """Gemini로 뉴스 감성 분석"""
        if not news_list:
            return {"score": 0, "signal": "NEUTRAL", "reason": "뉴스 없음"}

        try:
            import google.generativeai as genai
            genai.configure(api_key=config.GEMINI_API_KEY)
            model = genai.GenerativeModel("gemini-2.5-flash")

            name = STOCK_NAME_MAP.get(symbol, symbol)
            titles = "\n".join([f"- {n['title']}" for n in news_list[:5]])

            prompt = f"""다음 {name}({symbol}) 관련 뉴스 제목들을 분석해서 주식 투자 관점에서 감성을 평가해줘.

뉴스:
{titles}

응답 형식 (JSON만):
{{"score": -2~2 정수, "signal": "BUY/SELL/NEUTRAL", "summary": "한줄요약"}}

score: 2(매우긍정) 1(긍정) 0(중립) -1(부정) -2(매우부정)"""

            resp = model.generate_content(prompt)
            import json, re
            text = resp.text.strip()
            # JSON 추출
            match = re.search(r'\{.*?\}', text, re.DOTALL)
            if match:
                result = json.loads(match.group())
                return {
                    "score": result.get("score", 0),
                    "signal": result.get("signal", "NEUTRAL"),
                    "reason": result.get("summary", ""),
                }
        except Exception as e:
            logger.debug(f"감성 분석 실패 [{symbol}]: {e}")

        return {"score": 0, "signal": "NEUTRAL", "reason": "분석 실패"}

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
