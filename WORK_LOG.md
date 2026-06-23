# AutoTrader 작업일지

## 프로젝트 정보
- **Railway 프로젝트**: ideal-elegance (Pro plan)
- **대시보드**: https://dashboard-production-65e3.up.railway.app
- **GitHub**: jjjcho0710-bot/autotrader (Claude 직접 푸시 가능)
- **GitHub Token**: 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

---

## 서비스 구조
```
dashboard      → FastAPI + Jarvis AI + SSE 실시간 이벤트
open-webui     → Gemini 2.5 Flash (Jarvis 모델: autotrader-jarvis)
autotrader     → data-collector (1분봉 수집, KIS 실전 API)
stock-trader   → KIS 모의투자 자동매매
crypto-trader  → 업비트 자동매매 (실전, 45,057원 잔고)
Redis          → 캐시 + 대화히스토리 + 시세 캐시
PostgreSQL     → OHLCV + 매매이력 + ML + watchlist + 수급 + 공시 + 뉴스감성
```

## KIS API 설정
- **autotrader**: 실전 키 + KIS_IS_PAPER=false (실시간 시세)
- **stock-trader**: 실전 키 + KIS_IS_PAPER=true (모의투자 매매)
- **문제**: stock-trader 잔고조회 실패 (실전키로 모의투자 잔고 조회 불일치)

## 업비트 설정
- Static IP 화이트리스트: 152.55.176.240, 162.220.232.252, 152.55.177.181
- crypto-trader 정상 작동, KRW 잔고 45,057원
- USDT 소량 보유 (스테이블코인, 매매 제외 처리됨)

## Railway Static IP
- 152.55.176.240
- 162.220.232.252
- 152.55.177.181

---

## ✅ 완료된 작업 (2026-06-23)

### 인프라
- GitHub 직접 푸시 워크플로우 (Claude → GitHub → Railway 자동 배포)
- KST 시간 전 서비스 통일
- KIS 토큰 Redis 캐싱 (재시작해도 재발급 안 함)
- SSE 실시간 이벤트 (매수/매도 즉각 팝업 알림)

### 데이터 수집
- 실시간 1분봉 수집 (20종목, KIS 실전 API)
- pykrx 과거 일봉 데이터 (2021~현재)
- 외국인/기관 수급 수집 (stock_supply 테이블)
- DART 공시 연동 (stock_disclosure 테이블, API키: 0fa66d7197e1147047df7ff103b4c0e0b1142fd4)
- 뉴스 감성 분석 (Open-WebUI + Gemini → stock_news_sentiment 테이블)
- 뉴스 수집 후 Jarvis Open-WebUI 세션에 기억 저장

### 매매 시스템 (주식)
- 일봉 기준 MA크로스 전략
- 매수 필터 체인: MA크로스 → ML → 수급 → 뉴스감성 → Jarvis 판단
- 전체 종목 골든크로스 스캐너 (08:30 자동, 점수 2 이상)
- max_positions: 10종목

### 매매 시스템 (코인)
- MACD 전략 ON (fast:12, slow:26, signal:9)
- buy_amount: 10,000원, stop_loss: -3%, take_profit: +7%
- Jarvis 최종 판단 후 업비트 실제 매수/매도
- 시세 3초마다 Redis 업데이트
- USDT 등 스테이블코인 매매 제외

### Jarvis AI
- Open-WebUI 연동 (Gemini 2.5 Flash)
- 웹 ↔ 텔레그램 대화 공유 (같은 세션)
- 답변 2-3문장 이내 강제 제한
- 자동 분석 스케줄러 (08:30, 15:40)
- 텔레그램 "뉴스 수집해줘" 명령으로 수동 수집
- TTS 🔊 버튼

### UI 전면 개편 (업비트 스타일 라이트 모드)
- 폰트: Inter + JetBrains Mono
- 색상: #1261c4 (파랑), #059669 (초록), #dc2626 (빨강), #f5f6fa (배경)
- 사이드바: 200px, 섹션 구분 (메뉴/AI)
- 그라데이션/글로우 효과 완전 제거

#### 각 화면
- **대시보드**: 봇상태 + 오늘이벤트 + Jarvis분석 + 매매이력
- **주식**: 포지션/수급공시/Jarvis신호/감시종목 (탭4개)
- **코인**: 포지션/마켓현황/Jarvis신호 (탭3개) + 실시간 갱신
- **전략**: ON/OFF 토글 + 파라미터 설정
- **분석**: 성과/ML예측/학습데이터/백테스트 (탭4개)
- **Jarvis**: 채팅 + 빠른명령 + 포트폴리오 현황

### 데이터 API
- GET /api/data/supply (외국인/기관 수급)
- GET /api/data/disclosure (DART 공시)
- GET /api/data/sentiment (뉴스 감성)
- POST /api/data/collect (수동 수집)
- GET /api/events (SSE 실시간 이벤트)

---

## 스케줄

| 시간 | 작업 |
|------|------|
| 08:30 | 전종목 스캔 + ML분석 + 뉴스수집 + 공시확인 |
| 09:00~15:30 | 1분마다 주식 매매 사이클 |
| 매 60초 | 코인 매매 사이클 |
| 매 3초 | 코인 시세 Redis 업데이트 |
| 15:40 | ML분석 + 뉴스수집 |
| 16:00 | 일봉 + 수급 + 공시 + 뉴스 정규수집 |

---

## ❌ 남은 작업

### 긴급
- [ ] **stock-trader 잔고 조회 수정**
  - 문제: 실전 키 + 모의투자 계좌 불일치 → 화면에 잔고 "-"
  - 해결안 A: KIS Developers에서 모의투자 앱키 별도 발급 → stock-trader Variables 교체
  - 해결안 B: 실전 계좌에 소액 입금 → KIS_IS_PAPER=false 전환

### 코인 고도화
- [ ] **코인 뉴스 감성 분석**
  - BTC/ETH/SOL 관련 뉴스 Jarvis 웹 검색
  - 매수 필터에 뉴스 감성 반영
- [ ] **Jarvis 자율 코인 매매 강화**
  - 현재: MACD 신호 → Jarvis 판단
  - 목표: 급변 감지 (±5%) → 즉시 Jarvis 호출
  - 1시간마다 시장 전체 분석

### 주식 고도화
- [ ] **재무 데이터 수집** (4순위)
  - pykrx로 PER, PBR, ROE 수집
  - 적자 기업 매수 필터 제외
- [ ] **시장 지수 모니터링** (5순위)
  - 코스피/코스닥 지수
  - 하락장 매수 억제

### ML 고도화
- [ ] ML 모델 고도화 (현재 Naive Bayes → XGBoost/LSTM)
- [ ] 백테스트 개선 (인트라데이 데이터 충분히 쌓이면)

### railclaw 서비스
- [ ] 독립 추론/학습 엔진 구축
- [ ] PostgreSQL 대화 히스토리 영구 저장

---

## 주요 파일 위치
```
common/
  config.py      → 환경변수 설정
  database.py    → DB 테이블 정의
dashboard/
  main.py        → FastAPI + Jarvis + SSE + API
  static/
    dashboard-C.html  → 대시보드
    stock.html        → 주식
    crypto.html       → 코인
    strategy.html     → 전략
    analysis.html     → 분석
    jarvis.html       → Jarvis AI 채팅
stock_trader/
  main.py        → KIS 매매 + ML + 수급 + 뉴스 필터
  kis_trader.py  → KIS API + Redis 토큰 캐싱
crypto_trader/
  main.py        → 업비트 매매 + Jarvis 판단 + 시세 루프
  upbit_trader.py → 업비트 API
data_collector/
  main.py        → 수집 스케줄러
  collectors/
    supply_collector.py → 수급 데이터
    dart_collector.py   → DART 공시
    news_collector.py   → 뉴스 감성 분석
```

## 환경변수 (Shared Variables)
- KIS_APP_KEY, KIS_APP_SECRET (실전키)
- KIS_ACCOUNT_NO (모의투자 계좌)
- OPENWEBUI_URL, OPENWEBUI_API_TOKEN, JARVIS_MODEL
- JARVIS_ANALYST_TOKEN, JARVIS_ANALYST_CHAT_ID
- DART_API_KEY: 0fa66d7197e1147047df7ff103b4c0e0b1142fd4
- GEMINI_API_KEY
- UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY
