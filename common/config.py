import os
from dataclasses import dataclass


@dataclass
class Config:
    # ── PostgreSQL ──
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "autotrader")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASS: str = os.getenv("DB_PASS", "")

    # ── Redis ──
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASS: str = os.getenv("REDIS_PASS", "")

    # ── KIS (한국투자증권) ──
    KIS_APP_KEY: str = os.getenv("KIS_APP_KEY", "")
    KIS_APP_SECRET: str = os.getenv("KIS_APP_SECRET", "")
    KIS_ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "")   # 계좌번호
    KIS_IS_PAPER: bool = os.getenv("KIS_IS_PAPER", "true").lower() == "true"  # 모의투자

    # 모의투자 전용 키 (stock-trader용)
    # 설정 시 KIS_APP_KEY/SECRET 대신 사용
    KIS_APP_KEY_PAPER: str = os.getenv("KIS_APP_KEY_PAPER", "")
    KIS_APP_SECRET_PAPER: str = os.getenv("KIS_APP_SECRET_PAPER", "")

    @property
    def kis_app_key(self) -> str:
        """모의투자 키 우선, 없으면 실전 키"""
        if self.KIS_IS_PAPER and self.KIS_APP_KEY_PAPER:
            return self.KIS_APP_KEY_PAPER
        return self.KIS_APP_KEY

    @property
    def kis_app_secret(self) -> str:
        """모의투자 시크릿 우선, 없으면 실전 시크릿"""
        if self.KIS_IS_PAPER and self.KIS_APP_SECRET_PAPER:
            return self.KIS_APP_SECRET_PAPER
        return self.KIS_APP_SECRET

    # ── 업비트 ──

    # ── 텔레그램 ──
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    DART_API_KEY: str = os.getenv("DART_API_KEY", "")

    # ── 텔레그램 봇별 토큰 ──
    JARVIS_ANALYST_TOKEN:   str = os.getenv("JARVIS_ANALYST_TOKEN", "")
    JARVIS_ANALYST_CHAT_ID: str = os.getenv("JARVIS_ANALYST_CHAT_ID", "")
    STOCK_BOT_TOKEN:        str = os.getenv("STOCK_BOT_TOKEN", "")
    STOCK_CHAT_ID:          str = os.getenv("STOCK_CHAT_ID", "")
    # STARK v2 통합 봇(한강뷰매니저) — bot/telegram_bot.py 전용 토큰. Railway에 등록됨.
    STARK_BOT_TOKEN:        str = os.getenv("STARK_BOT_TOKEN", "")

    # ── 수집 설정 ──
    COLLECT_INTERVAL_SEC: int = int(os.getenv("COLLECT_INTERVAL_SEC", "60"))  # 1분봉
    STOCK_SYMBOLS: list = None   # 아래에서 설정

    def __post_init__(self):
        # 수집할 주식 종목 (환경변수로 override 가능)
        symbols_env = os.getenv("STOCK_SYMBOLS", "")
        self.STOCK_SYMBOLS = symbols_env.split(",") if symbols_env else [
            "005930",  # 삼성전자
            "000660",  # SK하이닉스
            "035420",  # NAVER
            "035720",  # 카카오
            "005380",  # 현대차
            "068270",  # 셀트리온
            "373220",  # LG에너지솔루션
            "323410",  # 카카오뱅크
        ]

    @property
    def db_url(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def redis_url(self) -> str:
        if self.REDIS_PASS:
            return f"redis://:{self.REDIS_PASS}@{self.REDIS_HOST}:{self.REDIS_PORT}/0"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    @property
    def kis_base_url(self) -> str:
        if self.KIS_IS_PAPER:
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"


config = Config()
