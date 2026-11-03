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
    KIS_APP_KEY: str = os.getenv("KIS_APP_KEY", "").strip()
    KIS_APP_SECRET: str = os.getenv("KIS_APP_SECRET", "").strip()
    KIS_ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "").strip()   # 계좌번호
    KIS_IS_PAPER: bool = os.getenv("KIS_IS_PAPER", "true").lower() == "true"  # 모의투자

    # 모의투자 전용 키/계좌 (stock-trader용)
    # 설정 시 KIS_APP_KEY/SECRET/ACCOUNT_NO 대신 사용
    KIS_APP_KEY_PAPER: str = os.getenv("KIS_APP_KEY_PAPER", "").strip()
    KIS_APP_SECRET_PAPER: str = os.getenv("KIS_APP_SECRET_PAPER", "").strip()
    KIS_ACCOUNT_NO_PAPER: str = os.getenv("KIS_ACCOUNT_NO_PAPER", "").strip()

    @property
    def kis_account_no(self) -> str:
        """모의투자 계좌번호 우선, 없으면 실전 계좌번호"""
        if self.KIS_IS_PAPER and self.KIS_ACCOUNT_NO_PAPER:
            return self.KIS_ACCOUNT_NO_PAPER
        return self.KIS_ACCOUNT_NO

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
    # 장 시간 검사 없이 항상 수집(기존 동작). 기본값 false — 평일 장중에만 수집.
    COLLECT_ALWAYS: bool = os.getenv("COLLECT_ALWAYS", "") == "1"
    STOCK_SYMBOLS: list = None   # 아래에서 설정

    # ── 계좌 설정 ──
    # 모의투자 시작 자금(원) — "누적 손익(원금 대비)" 계산 기준. PM 확인 값 1,000만원.
    INITIAL_SEED_KRW: int = int(os.getenv("INITIAL_SEED_KRW", "10000000"))

    # ── 리스크 기반 포지션 사이징 (PM 승인, 2026-09-30) ──
    # 종목당 목표 최대 손실 = 총자산 × RISK_PER_TRADE_PCT(%). 기본 0.75%.
    RISK_PER_TRADE_PCT: float = float(os.getenv("RISK_PER_TRADE_PCT", "0.75"))

    # ── 모멘텀 게이트 (PM 승인, [AT] feat/momentum-gate) ──
    # AI 자동매수 직전 직전 N거래일 수익률(종가 기준)을 확인한다.
    # 수익률 < DROP_THRESHOLD면 자동매수 차단, >= SHADOW_THRESHOLD면 차단 없이 shadow 로그만 남긴다.
    MOMENTUM_GATE_LOOKBACK_DAYS: int = int(os.getenv("MOMENTUM_GATE_LOOKBACK_DAYS", "5"))
    MOMENTUM_GATE_DROP_THRESHOLD_PCT: float = float(os.getenv("MOMENTUM_GATE_DROP_THRESHOLD_PCT", "0"))
    MOMENTUM_GATE_SHADOW_THRESHOLD_PCT: float = float(os.getenv("MOMENTUM_GATE_SHADOW_THRESHOLD_PCT", "8"))

    # ── 종목당 비중 상한 (PM 승인, [AT] feat/single-stock-cap) ──
    # 매수 후 예상 비중 = (해당 종목 현재 평가금액 + 이번 매수금액) ÷ 총자산. 초과 시
    # AI 자동매수는 차단, 수동 채팅매수·제안승인 매수는 경고 후 '강제매수' 키워드로만 통과.
    SINGLE_STOCK_MAX_PCT: float = float(os.getenv("SINGLE_STOCK_MAX_PCT", "20"))

    # ── 보호 규칙 ([AT] feat/protective-rules, PM 승인) — 환경변수로 덮어쓴다 ──
    # 일일 손실 한도: 당일 실현 pnl 합계 <= 총자산 x 이 값(%) 이면 신규 매수 차단('daily_loss_limit').
    DAILY_LOSS_LIMIT_PCT: float = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "-1.0"))
    # 시장 게이트: KOSPI·KOSDAQ 등락률이 둘 다 이 값(%) 이하이면 신규 매수 차단('market_gate').
    MARKET_GATE_PCT: float = float(os.getenv("MARKET_GATE_PCT", "-1.0"))
    # 급등주 진입 차단: 5거래일 수익률 >= SURGE_5D_BLOCK_PCT(%) 또는 당일 등락 >= SURGE_1D_BLOCK_PCT(%)
    # 이면 신규 매수 차단('surge_block'). 데이터가 없으면 차단하지 않는다(fail-open).
    SURGE_5D_BLOCK_PCT: float = float(os.getenv("SURGE_5D_BLOCK_PCT", "10"))
    SURGE_1D_BLOCK_PCT: float = float(os.getenv("SURGE_1D_BLOCK_PCT", "7"))
    # 마감 전 손실 정리: EOD_CUT_TIME_HHMM(KST) 이후, 매수 후 1거래일 이내이고 평가손익 <= EOD_CUT_PCT(%)
    # 인 보유 종목을 전량 매도한다(전략 라벨 '마감전손실정리').
    # EOD_CUT_ENABLED: 기본 false. false면 stock_trader가 _eod_loss_cut을 호출하지 않는다.
    EOD_CUT_ENABLED: bool = os.getenv("EOD_CUT_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
    EOD_CUT_PCT: float = float(os.getenv("EOD_CUT_PCT", "-3.0"))
    EOD_CUT_TIME_HHMM: str = os.getenv("EOD_CUT_TIME_HHMM", "15:20")

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


def compute_total_pnl(total_eval: float, seed: float = None) -> tuple:
    """시작 자금(seed) 대비 누적 손익(원, %) 계산. seed 미지정 시 INITIAL_SEED_KRW 사용."""
    seed = config.INITIAL_SEED_KRW if seed is None else seed
    if not seed:
        return 0, 0.0
    pnl = total_eval - seed
    return pnl, pnl / seed * 100
