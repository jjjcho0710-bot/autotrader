# AutoTrader 작업일지

## 프로젝트 정보
- **Railway 프로젝트**: ideal-elegance (Pro plan)
- **대시보드**: https://dashboard-production-65e3.up.railway.app
- **GitHub**: jjjcho0710-bot/autotrader

---

## ✅ 완료된 작업

### 인프라
- Railway 6개 서비스 배포 (dashboard, stock-trader, autotrader, crypto-trader, open-webui, Redis, PostgreSQL)
- KST 시간 전 서비스 통일
- KIS 토큰 Redis 캐싱 (재시작해도 재발급 안 함)
- GitHub 직접 푸시 워크플로우 (Claude → GitHub → Railway 자동 배포)

### 데이터 수집
- 실시간 1분봉 수집 (20종목, KIS 실전 API)
- 0원 재시도 로직
- pykrx 과거 일봉 데이터 (2021~현재)
- 외국인/기관 수급 데이터 수집 (stock_supply 테이블)
- DART 공시 연동 (stock_disclosure 테이블)
- 뉴스 감성 분석 (Open-WebUI + Gemini, stock_news_sentiment 테이블)

### 매매 시스템
- MA크로스 + RSI반등 + 볼린저밴드 전략
- 일봉 기준 자동 매매
- 매수 필터 체인: MA크로스 → ML → 수급 → 뉴스감성 → Jarvis 판단
- 전체 종목 골든크로스 스캐너 (08:30 자동 실행)
- max_positions: 10종목

### Jarvis AI
- Open-WebUI 연동 (Gemini 2.5 Flash)
- 웹 ↔ 텔레그램 대화 공유 (같은 세션)
- 웹 검색 (날씨, 뉴스)
- 감시 종목 추가/삭제 명령
- 자동 분석 스케줄러 (08:30, 15:40)
- 뉴스 수집 후 Jarvis 세션에 기억 저장
- TTS 🔊 버튼

### UI/UX
- 모바일 하단 탭바 6개
- 감시 종목 실시간 시세 + 종목명
- 분석 페이지 탭 UI (성과/ML예측/학습데이터/백테스트)
- 수급/공시/뉴스 확인 API

### 데이터 API
- GET /api/data/supply (외국인/기관 수급)
- GET /api/data/disclosure (DART 공시)
- GET /api/data/sentiment (뉴스 감성)
- POST /api/data/collect (수동 수집)

---

## 🔧 한국 주식 - 남은 작업

### 긴급
- [ ] **stock-trader 잔고 조회 수정**
  - 문제: 실전 키 + 모의투자 계좌 불일치
  - 해결: 모의투자 키 별도 적용 OR 실전 계좌 전환
  - stock-trader Variables: KIS_APP_KEY, KIS_APP_SECRET, KIS_IS_PAPER

### 매매 고도화
- [ ] **재무 데이터 수집** (4순위)
  - pykrx로 PER, PBR, ROE 수집
  - 적자 기업 매수 필터 제외
- [ ] **시장 지수 모니터링** (5순위)
  - 코스피/코스닥 지수
  - 지수 하락장에서 매수 억제
- [ ] **Jarvis 자동 종목 스캐너 개선**
  - 스캐너 조건 튜닝 (현재: 골든크로스 점수 2 이상)
  - 유망 종목 발굴 정확도 향상
- [ ] **ML 모델 고도화**
  - 현재: Naive Bayes (정확도 43~64%)
  - 개선: LSTM, XGBoost 등 고급 모델

### 모니터링
- [ ] **매매 신호 발생 확인**
  - MA크로스 신호 아직 미발생 (횡보장)
  - 수동으로 신호 트리거 테스트 필요
- [ ] **수급/공시/뉴스 정규 수집 검증**
  - 16:00 이후 자동 수집 확인
  - API 결과 품질 검증

---

## 🪙 코인 - 진행 예정
→ 아래 별도 섹션 참조

---

## 📅 스케줄

| 시간 | 작업 |
|------|------|
| 08:30 | 전종목 스캔 + ML분석 + 뉴스수집 + 공시확인 |
| 09:00~15:30 | 1분마다 매매 사이클 |
| 15:40 | ML분석 + 뉴스수집 |
| 16:00 | 일봉 + 수급 + 공시 + 뉴스 정규 수집 |

