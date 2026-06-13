# AutoTrader — 배포 & 테스트 체크리스트

> Railway GitHub 연동 배포 기준

---

## 1단계 — GitHub 준비

- [ ] 레포지토리 생성 (`autotrader`)
- [ ] 폴더 구조 그대로 push
  ```
  autotrader/
  ├── common/
  ├── data_collector/
  ├── stock_trader/
  ├── crypto_trader/
  └── .env.example
  ```
- [ ] `.gitignore` 에 `.env` 추가 (API키 노출 방지)

---

## 2단계 — Railway 인프라 세팅

- [ ] Railway 프로젝트 `zonal-nurturing` 접속
- [ ] **PostgreSQL** 플러그인 추가
  - Variables 탭에서 `DATABASE_URL` 자동 생성 확인
- [ ] **Redis** 플러그인 추가
  - Variables 탭에서 `REDIS_URL` 자동 생성 확인

---

## 3단계 — 서비스 배포 (3개)

### data-collector
- [ ] New Service → GitHub repo 연결
- [ ] Root Directory: `data_collector`
- [ ] Dockerfile 경로: `data_collector/Dockerfile`
- [ ] 환경변수 설정 (아래 목록 참고)
- [ ] Deploy 확인

### stock-trader
- [ ] New Service → GitHub repo 연결
- [ ] Root Directory: `stock_trader`
- [ ] Dockerfile 경로: `stock_trader/Dockerfile`
- [ ] 환경변수 설정
- [ ] Deploy 확인

### crypto-trader
- [ ] New Service → GitHub repo 연결
- [ ] Root Directory: `crypto_trader`
- [ ] Dockerfile 경로: `crypto_trader/Dockerfile`
- [ ] 환경변수 설정
- [ ] Deploy 확인

---

## 4단계 — 환경변수 세팅 (각 서비스 공통)

| 변수명 | 설명 | 필수 |
|--------|------|------|
| `DB_HOST` | PostgreSQL 호스트 | ✅ |
| `DB_PORT` | 5432 | ✅ |
| `DB_NAME` | railway | ✅ |
| `DB_USER` | postgres | ✅ |
| `DB_PASS` | Railway 자동생성 | ✅ |
| `REDIS_HOST` | Redis 호스트 | ✅ |
| `REDIS_PORT` | 6379 | ✅ |
| `REDIS_PASS` | Railway 자동생성 | ✅ |
| `KIS_APP_KEY` | KIS 앱키 | ✅ |
| `KIS_APP_SECRET` | KIS 시크릿 | ✅ |
| `KIS_ACCOUNT_NO` | 계좌번호 (00000000-01) | ✅ |
| `KIS_IS_PAPER` | true (모의투자 먼저!) | ✅ |
| `UPBIT_ACCESS_KEY` | 업비트 Access Key | ✅ |
| `UPBIT_SECRET_KEY` | 업비트 Secret Key | ✅ |
| `TELEGRAM_TOKEN` | 텔레그램 봇 토큰 | ✅ |
| `TELEGRAM_CHAT_ID` | 텔레그램 채팅 ID | ✅ |
| `COLLECT_INTERVAL_SEC` | 60 | 선택 |

---

## 5단계 — 기능 테스트 항목

### ✅ data-collector 테스트
- [ ] 서비스 로그에 `✅ PostgreSQL 연결 완료` 출력
- [ ] 서비스 로그에 `✅ Redis 연결 완료` 출력
- [ ] 서비스 로그에 `✅ 주식 수집 완료 — N/N종목` 출력
- [ ] 서비스 로그에 `✅ 코인 수집 완료 — N/N페어` 출력
- [ ] PostgreSQL 에서 `stock_ohlcv` 테이블 데이터 확인
  ```sql
  SELECT * FROM stock_ohlcv ORDER BY created_at DESC LIMIT 10;
  ```
- [ ] PostgreSQL 에서 `crypto_ohlcv` 테이블 데이터 확인
  ```sql
  SELECT * FROM crypto_ohlcv ORDER BY created_at DESC LIMIT 10;
  ```
- [ ] Redis 에 `stock:price:005930` 키 존재 확인
- [ ] Redis 에 `bot:status` → `data_collector` 상태 확인
- [ ] 60초 주기 반복 수집 확인 (로그에서 사이클 반복)

### ✅ stock-trader 테스트 (모의투자 KIS_IS_PAPER=true)
- [ ] 서비스 로그에 `✅ KISTrader 시작` 출력
- [ ] 장 시간(09:00~15:30) 외에는 대기 로그 확인
- [ ] 장 시간 중 매매 사이클 로그 확인
- [ ] MA크로스 신호 감지 로그 확인 (`골든크로스 감지`)
- [ ] 모의투자 매수 주문 체결 확인 (KIS 모의투자 HTS에서)
- [ ] `trade_history` 테이블에 매수 기록 확인
  ```sql
  SELECT * FROM trade_history WHERE bot='stock_trader' ORDER BY ts DESC;
  ```
- [ ] 텔레그램으로 매수 알림 수신 확인
- [ ] 손절/익절 조건 충족 시 매도 실행 확인

### ✅ crypto-trader 테스트 (업비트 소액 실거래)
- [ ] 서비스 로그에 `✅ UpbitTrader 시작` 출력
- [ ] 24시간 매매 사이클 로그 확인
- [ ] MACD 신호 감지 로그 확인
- [ ] 업비트 소액 매수 체결 확인 (업비트 앱에서)
- [ ] `trade_history` 테이블에 코인 매수 기록 확인
  ```sql
  SELECT * FROM trade_history WHERE bot='crypto_trader' ORDER BY ts DESC;
  ```
- [ ] 텔레그램으로 코인 매수 알림 수신 확인
- [ ] 잔고 부족 시 매수 스킵 로그 확인

### ✅ 텔레그램 알림 테스트
- [ ] 매수 체결 알림 수신
- [ ] 매도 체결 알림 수신
- [ ] 손절 알림 수신
- [ ] 에러 발생 시 알림 수신

### ✅ 통합 안정성 테스트
- [ ] 3개 서비스 동시 운영 시 DB 충돌 없음
- [ ] Redis 키 TTL 정상 만료 확인
- [ ] 서비스 재시작 후 정상 복구 확인
- [ ] 24시간 이상 무중단 운영 확인

---

## 6단계 — 실거래 전환 (모의투자 충분히 테스트 후)

- [ ] `KIS_IS_PAPER=false` 로 변경
- [ ] 소액으로 첫 실거래 테스트 (1회 매수금액 최소화)
- [ ] 실거래 체결 확인 (증권사 앱)
- [ ] 이상 없으면 정상 운영

---

## ⚠️ 주의사항

- 반드시 **모의투자 먼저** 충분히 테스트 후 실거래 전환
- 업비트는 모의투자 없으므로 **소액(5,000원)으로 먼저** 테스트
- KIS API 요청 횟수 제한 주의 (1초당 20회)
- 텔레그램 알림 확인 후 실거래 진행
