"""
DART 전자공시 수집기
금융감독원 DART API를 이용해 실시간 공시 수집
무료 API: https://opendart.fss.or.kr
"""
import logging
import aiohttp
from datetime import datetime, timedelta

from common.config import config
from common.database import db

logger = logging.getLogger(__name__)

# 중요 공시 유형
IMPORTANT_DISCLOSURES = {
    "A": "정기공시",       # 사업보고서, 분기보고서
    "B": "주요사항보고",   # 유상증자, 전환사채 등
    "C": "발행공시",       # 증권신고서
    "D": "지분공시",       # 대량보유, 임원주요주주
    "F": "외부감사",       # 감사보고서
}

# 즉시 알림 필요한 공시 키워드
ALERT_KEYWORDS = [
    "유상증자", "무상증자", "전환사채", "신주인수권",
    "합병", "분할", "영업양수도", "자기주식취득",
    "대표이사변경", "최대주주변경", "상장폐지",
    "감사의견", "불성실공시", "조회공시",
    "실적발표", "영업이익", "당기순이익",
]


class DARTCollector:
    """DART 전자공시 수집기"""

    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self):
        self.api_key = config.DART_API_KEY if hasattr(config, 'DART_API_KEY') else ""
        import os
        self.api_key = os.getenv("DART_API_KEY", "")

    async def get_corp_code(self, symbol: str) -> str:
        """종목코드 → DART 고유번호 변환"""
        try:
            # DART 기업 고유번호 조회
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/company.json",
                    params={"crtfc_key": self.api_key, "stock_code": symbol}
                ) as resp:
                    data = await resp.json()
                    if data.get("status") == "000":
                        return data.get("corp_code", "")
        except Exception as e:
            logger.debug(f"DART 기업코드 조회 실패 [{symbol}]: {e}")
        return ""

    async def get_recent_disclosures(self, symbol: str = None, days: int = 1) -> list:
        """최근 공시 목록 조회"""
        if not self.api_key:
            return []

        try:
            today = datetime.now().strftime("%Y%m%d")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

            params = {
                "crtfc_key": self.api_key,
                "bgn_de": from_date,
                "end_de": today,
                "last_reprt_at": "N",
                "pblntf_ty": "A,B,D",  # 정기공시, 주요사항, 지분공시
                "page_count": 40,
            }

            if symbol:
                params["stock_code"] = symbol

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/list.json",
                    params=params
                ) as resp:
                    data = await resp.json()

            if data.get("status") != "000":
                return []

            disclosures = []
            for item in data.get("list", []):
                report_nm = item.get("report_nm", "")
                is_important = any(kw in report_nm for kw in ALERT_KEYWORDS)
                disclosures.append({
                    "symbol": item.get("stock_code", ""),
                    "corp_name": item.get("corp_name", ""),
                    "report_name": report_nm,
                    "rcept_dt": item.get("rcept_dt", ""),
                    "rcept_no": item.get("rcept_no", ""),
                    "is_important": is_important,
                })

            return disclosures

        except Exception as e:
            logger.error(f"DART 공시 조회 오류: {e}")
            return []

    async def collect_and_alert(self, symbols: list[str], telegram_func=None):
        """공시 수집 + 중요 공시 텔레그램 알림"""
        if not self.api_key:
            logger.warning("DART_API_KEY 없음 → 공시 수집 스킵")
            return

        logger.info(f"📋 DART 공시 수집 시작 — {len(symbols)}종목")
        all_disclosures = []

        for symbol in symbols:
            disclosures = await self.get_recent_disclosures(symbol, days=1)
            all_disclosures.extend(disclosures)

        if not all_disclosures:
            logger.info("📋 오늘 새 공시 없음")
            return

        # DB 저장
        try:
            async with db.pool.acquire() as conn:
                for d in all_disclosures:
                    await conn.execute("""
                        INSERT INTO stock_disclosure
                        (symbol, corp_name, report_name, rcept_dt, rcept_no, is_important)
                        VALUES ($1,$2,$3,$4,$5,$6)
                        ON CONFLICT (rcept_no) DO NOTHING
                    """,
                        d["symbol"], d["corp_name"], d["report_name"],
                        d["rcept_dt"], d["rcept_no"], d["is_important"]
                    )
        except Exception as e:
            logger.error(f"공시 DB 저장 실패: {e}")

        # 중요 공시 텔레그램 알림
        important = [d for d in all_disclosures if d["is_important"]]
        if important and telegram_func:
            msg = f"📋 중요 공시 알림 [{datetime.now().strftime('%m/%d %H:%M')}]\n\n"
            for d in important[:5]:
                msg += f"🔔 {d['corp_name']}({d['symbol']})\n"
                msg += f"   {d['report_name']}\n\n"
            await telegram_func(msg)

        logger.info(f"✅ 공시 수집 완료: 전체 {len(all_disclosures)}건, 중요 {len(important)}건")

    async def get_financial_summary(self, symbol: str) -> dict:
        """재무 요약 정보 조회 (최신 분기)"""
        if not self.api_key:
            return {}

        try:
            corp_code = await self.get_corp_code(symbol)
            if not corp_code:
                return {}

            year = datetime.now().year
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/fnlttSinglAcnt.json",
                    params={
                        "crtfc_key": self.api_key,
                        "corp_code": corp_code,
                        "bsns_year": str(year),
                        "reprt_code": "11013",  # 1분기보고서
                        "fs_div": "CFS",  # 연결재무제표
                    }
                ) as resp:
                    data = await resp.json()

            if data.get("status") != "000":
                return {}

            result = {}
            for item in data.get("list", []):
                account_nm = item.get("account_nm", "")
                if "매출액" in account_nm:
                    result["revenue"] = item.get("thstrm_amount", 0)
                elif "영업이익" in account_nm:
                    result["operating_profit"] = item.get("thstrm_amount", 0)
                elif "당기순이익" in account_nm:
                    result["net_income"] = item.get("thstrm_amount", 0)

            return result

        except Exception as e:
            logger.debug(f"재무 정보 조회 실패 [{symbol}]: {e}")
            return {}
