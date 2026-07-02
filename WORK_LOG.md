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
crypto-trader  → 업비트 자동매매 (실전, 잔고 35원 + BTC 보유중)
Redis          → 캐시 + 대화히스토리 + 시세 캐시
PostgreSQL     → OHLCV + 매매이력 + ML + watchlist + 수급 + 공시 + 뉴스감성 + jarvis_memory
```

---

## ✅ 2026-06-23 오늘 완료 작업

### 자동매매 파이프라인 완성
- 전체 자동화 파이프라인 구축 완료
  ```
  08:30 전종목 스캔 (RSI+모멘텀+거래량+골든크로스)
    → watchlist 자동 추가 → OHLCV 수집 트리거
  09:00~15:30 MA크로스 + ML → Jarvis 판단 → 자동매매
  15:40 ML 자동학습 (watchlist 전체) → 일일결산 텔레그램
  자정 00:00 코인 일일결산 보고
  ```
- 스캐너 강화: RSI(과매도반등/강세) + 5일모멘텀 + MA정배열 조건 추가
- ML 학습 대상: 고정 심볼 → watchlist 전체 종목으로 변경
- 장 마감 후 일일결산 + 내일 전략 텔레그램 자동 발송

### 코인 자동매매 개선
- USDT/스테이블코인 포지션 완전 제외
- Redis TTL 120초로 안정화 (사이클 60초 대비)
- Gemini API 제거 → ML 모델 로컬 판단으로 교체 (API 비용 절감)
- 매매 시 텔레그램 알림 제거 (조용히 자동 실행)
- 자정 00:00 하루 1번 결산 보고만 텔레그램 전송
- **Jarvis 매수 금액 자율 판단** (ML 확률 기반)
  - 90%+ → 잔고 40% / 80%+ → 25% / 70%+ → 15% / 60%+ → 10%
  - 최대 잔고 50% 초과 금지

### 데이터 적재
- 주식 과거 OHLCV: KIS API로 20종목 16일치 적재 완료
- 코인 과거 OHLCV: 업비트 API로 9종목 60일치 적재 완료
- get_recent_ohlcv 버그 수정: ASC LIMIT → DESC LIMIT + 역정렬 (최신 데이터 정확히 조회)

### Jarvis AI 고도화
- **영구 메모리 구현**: jarvis_memory 테이블 (PostgreSQL 영구 저장)
  - Redis 재시작해도 메모리 유지 (DB에서 자동 복원)
  - 대시보드 ↔ 텔레그램 메모리 공유 확인 완료
- **ML 학습 결과 메모리 저장**: 매일 학습 후 결과를 Jarvis가 기억
  - "오늘 학습 결과 어때?" → Jarvis가 답변 가능
  - 전략 회의 가능
- **매매 결과 메모리 저장**: 매수/매도 후 자동으로 Jarvis 메모리에 기록
- signal 프롬프트에 DB 컨텍스트 추가 (잔고/포지션/시세 기반 판단)
- Open-WebUI system 프롬프트 중복 제거 (두 개 합쳐지던 문제 수정)
- /api/jarvis/memory 엔드포인트 추가 (외부 서비스에서 메모리 저장)

### 실시간 시세 조회
- /api/price/{symbol} API 추가 (KIS API 직접 호출, stock_trader 모듈 불필요)
- Open-WebUI Tools에 get_price 함수 추가
- Gemini fallback에도 실시간 시세 조회 로직 추가
- 삼성전자 시세 310,000원 -12.31% 정상 조회 확인

### 첫 실거래
- **KRW-BTC 매수 완료**: 45,000원 @ 94,978,000원 (MACD 골든크로스)
- 현재 BTC 보유 중 (KRW 잔고 35원)

---

## ❌ 내일 해야 할 작업 및 고도화 방향

### 🔴 긴급 (내일 최우선)

#### 1. stock-trader 잔고 조회 수정
- 문제: 실전 키 + 모의투자 계좌 불일치 → 잔고 "-" 표시
- 해결: KIS Developers에서 모의투자 앱키 별도 발급 후 stock-trader 환경변수 교체
- 또는: 실전 계좌에 소액 입금 후 KIS_IS_PAPER=false 전환

#### 2. 코인 잔고 충전
- 현재 KRW 35원 → BTC 매도 후 재투자 또는 추가 충전 필요
- BTC 익절 조건: +7% (현재 -0.7%)

#### 3. Open-WebUI 503 오류 대응
- Gemini API 과부하로 간헐적 실패 → 재시도 로직 추가
- 또는 모델을 gemini-1.5-flash로 변경 (더 안정적)

---

### 🟡 고도화 (우선순위 순)

#### 4. Jarvis 판단 고도화
- 현재: Open-WebUI(Gemini)가 EXECUTE/SKIP 판단
- 목표: ML 모델 + 과거 매매 메모리 기반 자체 판단
- 구현: railclaw 서비스 구축 (독립 추론 엔진)

#### 5. 코인 뉴스 감성 분석
- BTC/ETH/SOL 관련 뉴스 수집
- 매수 필터에 뉴스 감성 반영 (주식처럼)
- 급변 감지 (±5%) → 즉시 Jarvis 호출

#### 6. 주식 자동매매 실거래 전환
- 현재: 모의투자
- 목표: 실전 계좌 소액 투자
- 조건: 잔고 조회 수정 후 + 1주일 모의투자 검증 후

#### 7. ML 모델 고도화
- 현재: Naive Bayes (단순)
- 목표: XGBoost 또는 LSTM
- 데이터: 일봉 쌓일수록 정확도 향상 (매일 15:40 자동학습)

#### 8. 재무 데이터 수집
- PER, PBR, ROE 수집 (pykrx)
- 적자 기업 매수 필터 제외
- 시장 지수 (코스피/코스닥) 하락장 매수 억제

---

## 방향성 및 비전

```
지금 (2026-06-23)
  ML 모델 → 기술적 신호 판단
  Jarvis(Gemini) → 최종 매매 결정
  메모리 → Redis + PostgreSQL 영구 저장

단기 (1개월)
  ML 모델 고도화 (XGBoost)
  Jarvis 메모리 축적 → 경험 기반 판단
  코인/주식 동시 실거래

중기 (3개월)
  railclaw 서비스 → 독립 추론 엔진
  Jarvis가 뉴스/시황/ML/메모리 종합 판단
  전략 회의: "이번 주 어떤 전략이 좋아?" → Jarvis 자동 제안

장기 (6개월)
  완전 자율 매매 시스템
  Jarvis가 스스로 전략 수정
  수익률 기반 자동 포트폴리오 리밸런싱
```

---

## 스케줄
| 시간 | 작업 |
|------|------|
| 00:00 | 코인 일일결산 텔레그램 보고 |
| 08:30 | 전종목 스캔(RSI+모멘텀) + ML분석 + 뉴스수집 + OHLCV트리거 |
| 09:00~15:30 | 주식 매매 사이클 (1분마다) |
| 매 60초 | 코인 매매 사이클 |
| 매 3초 | 코인 시세 Redis 업데이트 |
| 15:40 | ML 자동학습 + 일일결산 텔레그램 + 뉴스수집 |

---

## 주요 파일
```
common/
  config.py        → 환경변수
  database.py      → DB 테이블 (jarvis_memory 포함)
dashboard/
  main.py          → FastAPI + Jarvis + 메모리 + 시세API
stock_trader/
  main.py          → KIS 매매 + ML + Jarvis 메모리 저장
crypto_trader/
  main.py          → 업비트 매매 + ML판단 + 자율금액 + 결산보고
data_collector/
  bulk_fetch.py    → 과거 데이터 일괄 적재 (KIS API)
```

## 환경변수 (Railway Shared Variables)
- KIS_APP_KEY, KIS_APP_SECRET (실전키)
- KIS_ACCOUNT_NO (모의투자 계좌)
- OPENWEBUI_URL, OPENWEBUI_API_TOKEN, JARVIS_MODEL
- JARVIS_ANALYST_TOKEN, JARVIS_ANALYST_CHAT_ID
- DART_API_KEY: 0fa66d7197e1147047df7ff103b4c0e0b1142fd4
- GEMINI_API_KEY
- UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY

---

## ✅ 2026-06-24 완료 작업

### 주식 화면 정상화
- 모의투자 앱키 별도 발급 후 설정 (KIS_APP_KEY_PAPER, KIS_APP_SECRET_PAPER)
- KIS 토큰 모의투자/실전 Redis 키 분리 (kis:paper_token / kis:real_token / kis:access_token)
- 실전 키(KIS_APP_KEY)와 모의투자 키(KIS_APP_KEY_PAPER) 역할 분리
  - 코스피/코스닥 지수 → 실전 키로 조회
  - 잔고/포지션/매매 → 모의투자 키(kis_app_key 자동선택)로 조회
- 예수금 필드명 수정 (deposit → cash)
- 코스피 8,439 / 예수금 1,000만원 / 총평가 1,000만원 정상 표시 확인

### 코스피/코스닥 지수
- /api/market/index 실전 API로 조회
- SSL 설정 추가
- 토큰 Redis 캐시 (1분 제한 방지)
- 1분마다 자동 갱신

### data-collector FastAPI 서버
- port 8000 FastAPI 추가
- /api/collect/supply 수급+공시 수동 트리거
- /api/collect/ohlcv OHLCV 트리거
- requirements에 fastapi, uvicorn 추가

### 실시간 시세 조회
- /api/price/{symbol} 정상 동작 확인
- 삼성전자 310,000원 -12.31% 조회 확인
- Open-WebUI Tools get_price 함수 등록 완료

---

## 📋 2026-06-25 작업 예정 (민기님이 요청 시)

### 코인 RSI 전략 DB 등록 (crypto-trader Console에서)
```
python3 - << 'PYEOF'
import asyncio, sys, json
sys.path.insert(0, "/app")
from common.database import db

async def main():
    await db.connect()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO strategy_config (bot, name, is_active, params)
            VALUES ('crypto_trader', 'RSI반등', true, $1)
            ON CONFLICT (bot, name) DO UPDATE SET is_active=true
        """, json.dumps({"period": 14, "entry": 35, "exit": 65,
                         "stop_loss": -0.04, "take_profit": 0.06, "buy_amount": 10000}))
        print("완료!")
    await db.disconnect()

asyncio.run(main())
PYEOF
```

---

## ✅ 2026-06-24 추가 작업

### 자동매매 완전 자율화
- Jarvis API 제거 → ML 직접 판단 매수 (주식/코인 동일)
- ML 확률 기반 매수 금액 자율 결정 (90%→잔고30%, 80%→20%, 70%→15%, 60%→10%)
- RSI반등 + 볼린저밴드 전략 구현 (주식)
- 실시간 급락/급등 감지 (3초 모니터링)
  - 주식: -3% / +7% → Jarvis 즉시 판단
  - 코인: -4% / +8% → 즉시 자동 매도
- KIS 토큰 만료 자동 재발급 (수동 삭제 불필요)
- 보유 종목 중복 매수 방지

### 코인
- 업비트 거래량 TOP 20 자동 스캔 (매일 08:30)
- 코인 한글명 표시 (비트코인, 이더리움 등)
- RSI반등 전략 추가 (MACD 신호 부족 보완)

### 대시보드 UI
- 주식/코인 색상 통일: 상승=빨강, 하락=파랑 (한국 증시 기준)
- 모바일 반응형 전체 화면
- 하단 네비게이션 바 (대시보드/주식/코인/전략/Jarvis)
- 전략 카드 모바일 1열
- 코스피/코스닥 실시간 표시
- 예수금/포지션 정상 표시

### PENDING
- [ ] 코인 RSI 전략 DB 등록
- [ ] 코인 단타 전략 구현 (1분봉 기반)
- [ ] 수급/공시 데이터 수집 (16:00 이후)
- [ ] railclaw 서비스 구축
- [ ] jarvis-analyst 서비스 개발

---

## 📋 주식 자동화 계획 (코인 안정화 후 진행)

### 목표
Jarvis 기반 완전 자동 주식 매매 시스템

### 구현 계획
1. **08:30 전체 종목 스캔**
   - Jarvis가 코스피/코스닥 전체 스캔
   - 거래량, RSI, 모멘텀 조건 체크
   - 유망 종목 watchlist 자동 추가
   - data-collector 수집 시작

2. **장중 (09:00~15:30)**
   - 실시간 시세 수집
   - MA크로스 + ML 신호
   - Jarvis 최종 판단
   - 자동 매수/매도
   - 텔레그램 알림

3. **15:40 장 마감 후**
   - 전체 종목 ML 자동 학습
   - 내일 전략 업데이트

### 신호 강도에 따른 투자 금액
- 신호 강함 (ML 90%+) → 잔고 40%
- 신호 보통 (ML 75%+) → 잔고 20%
- 신호 약함 (ML 60%+) → 잔고 10%
- 확신 없음 → SKIP


---

## 📋 코인 자동매매 작업일지 (2026-06-23 ~ 2026-07-03)

### 현재 코인 시스템 상태
- 거래소: 업비트 실전계정
- 잔고: 130만원
- 대상: 메이저 20개 코인 고정

### 메이저 20개 코인 목록
BTC, ETH, XRP, SOL, ADA, DOGE, AVAX, LINK, DOT, SUI,
TRX, NEAR, ARB, SHIB, APT, SAND, ATOM, FIL, AXS, XLM

### 완료된 작업

#### 전략
- RSI 반등 전략 (entry=35, exit=65)
- MACD 골든크로스 전략
- ML 제거 → RSI 수치 기반 매수금액 결정
- 중복 매수 완전 차단
- 매수 후 즉시 포지션 등록

#### 매수 금액 (RSI 강도별)
- RSI 20이하 → 잔고 40% (강한 신호)
- RSI 25이하 → 잔고 25% (보통)
- RSI 30이하 → 잔고 15% (약함)
- RSI 35이하 → 최소 15만원
- 최소 매수금액: 15만원 (순수익 최소 1,200원 보장)

#### 시간대별 전략
- 낮 (04:00~22:00): RSI 35이하, 익절 +1%, 손절 -5%
- 야간 (22:00~04:00): RSI 20이하만, 익절 +3%, 손절 -5%

#### 익절/손절
- 낮 익절: +1%
- 야간 익절: +3% (아침 반등 기다림)
- 손절: -5% (웬만하면 기다림)
- +3% 이상: Jarvis 판단 (HOLD/SELL)

#### 텔레그램
- 개별 알림 제거
- 6시간마다 요약 리포트 (00:00, 06:00, 12:00, 18:00)

#### 대시보드
- 단타/스윙 모드 스위치 (전략 화면)
- 코인 초기화+수집 버튼
- 시스템 헬스체크 (/api/health/full)

### PENDING 작업
- [ ] 야간 RSI 20이하 전략 세부 적용 (오늘 예정)
- [ ] 시간대별 매수금액 다르게 적용
- [ ] 주식 자동화 (코인 안정화 후)

### 핵심 교훈
- SLX, RE 같은 잡코인 → 손절 반복 (메이저만 사용)
- 중복 매수 → 같은 코인 계속 매수 버그 수정
- 밤 22시 이후 신호 약해짐 → 시간대별 전략 필요
- ML은 코인 단타에 불필요 → RSI 수치로 대체
- 익절 너무 빠르면 수수료 손해 → 최소 15만원 매수

