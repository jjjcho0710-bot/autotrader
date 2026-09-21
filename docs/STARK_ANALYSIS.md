# AutoTrader 기존 시스템 코드 분석 보고서

분석일: 2026-09-21 / 대상: github.com/jjjcho0710-bot/autotrader (main, 75파일, Python 14,252줄 + HTML 5,325줄)
분석 방식: 저장소 전체 clone 후 코드 직접 읽음. **DB 실데이터·Railway 로그·Open WebUI 설정·환경변수 값은 접근 불가** → 이 부분은 OpenClaw 팀(FRIDAY) 후속 조사 항목으로 분리 (마지막 절 참고).

---

## 1. 시스템 구성 (실제 코드 기준)

### 1-1. 서비스 5개 (Railway ideal-elegance)
| 서비스 | 진입점 | 줄수 | 역할 |
|---|---|---|---|
| dashboard | dashboard/main.py | **7,651** | FastAPI. 웹 화면 + 97개 API + 자비스 채팅 + 텔레그램 웹훅 + 스케줄러 + 학습 + 판단 API — **사실상 모든 것이 이 한 파일에 있음** |
| stock-trader | stock_trader/main.py | 1,054 | 장중 루프. 룰 전략(RSI/MA/볼린저)으로 후보 감지 → 필터 → dashboard `/api/jarvis/signal`에 판단 요청 → 결과대로 주문 |
| crypto-trader | crypto_trader/main.py | 1,448 | 코인 루프 (폐기 대상) |
| data-collector | data_collector/main.py | 244 | OHLCV·수급·공시·뉴스 수집 |
| open-webui | (외부 Railway 서비스) | — | LLM 프록시. 모든 자비스 AI 호출이 여기를 경유 |

### 1-2. 자비스 "입구" — 코드에서 확인된 것
| 입구 | 코드 위치 | 세션 ID |
|---|---|---|
| 텔레그램 웹훅 | main.py:5751 `/api/telegram/webhook` | JARVIS_ANALYST_CHAT_ID |
| 웹 채팅 (모바일 jarvis.html, PC pages/jarvis.html) | main.py:4917 `/api/jarvis/chat` | JARVIS_ANALYST_CHAT_ID (텔레그램과 **동일 세션 공유**, :4936) |
| 코인 채팅 | main.py:4902 `/api/crypto/chat` | 별도 |
| Open WebUI 직접 접속 | 외부 서비스 | Open WebUI 자체 세션 |

텔레그램 봇 토큰은 config.py에 **4종** 정의: TELEGRAM_TOKEN(기본), JARVIS_ANALYST_TOKEN(자비스), STOCK_BOT_TOKEN(주식 알림), CRYPTO_BOT_TOKEN(코인). common/telegram.py는 **발송 전용**이고 수신은 dashboard 웹훅 하나가 전부 받음.

### 1-3. LLM 호출 체인 (모든 AI 판단이 이 경로)
```
_jarvis_chat_impl / jarvis_signal / jarvis_exit_decision / _learn_from_url
        │
        ▼
_ask_openwebui()  (main.py:5616)
   ├─ OPENWEBUI_API_TOKEN 있음 → Open WebUI /api/chat/completions, model="autotrader-jarvis"
   │     (Open WebUI 안에 정의된 모델·시스템프롬프트를 씀 — 저장소에 없음, 확인 불가)
   └─ 토큰 없거나 실패 → _ask_gemini_direct() (main.py:5697)
         gemini-2.5-flash + JARVIS_SYSTEM_PROMPT (main.py:3861, 1,378자)
         종목 시세는 **하드코딩된 8개 종목만** 조회 (:5705)
```
→ 실제로 어느 경로가 타는지는 Railway 변수 `OPENWEBUI_API_TOKEN` 설정 여부에 달림. **두 경로의 시스템 프롬프트가 다르므로 응답 성격이 갈릴 수 있음.**

### 1-4. DB 테이블
- common/database.py에 정식 정의: stock_ohlcv, crypto_ohlcv, trade_history, balance_snapshot, stock_daily_ohlcv, stock_indicators, ml_predictions, strategy_config, watchlist, stock_supply, stock_disclosure, jarvis_memory, stock_news_sentiment
- dashboard/main.py 안에서 **즉석 생성**: stock_master(:182), jarvis_notes(여러 곳에서 CREATE TABLE IF NOT EXISTS 반복), trade_journal, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 10회
→ 스키마 정본이 없음. 어떤 컬럼이 실제 존재하는지는 DB를 직접 봐야 앎.

### 1-5. 스케줄러 (main.py:2295 `_jarvis_scheduler`, 1분 폴링)
08:30 전종목 스캔·ML·작전수립 / 09:01 예약주문 큐 / 09:30·10:30·13:00 장중 스캔 / 11:00·14:00 능동 제안 / 15분마다 우선감시 / 15:40 마감 분석·리포트 / 21:00 일일 통합 리포트 / 토 10:00 주간복기+지식정리 / 일 20:00 주간예습

---

## 2. 증상별 근본 원인 (코드 근거)

### 증상 A. 감시종목 아닌 종목을 물으면 "알 수 없음"
**원인 확정 — 설계상 제약**
- `_resolve_stock_symbol()` (main.py:4514): 종목 식별 순서가 ① 6자리 코드 → ② **watchlist 테이블** 이름 매칭 → ③ `_stock_name_cache`(pykrx 전종목) → ④ stock_master DB LIKE
- ③④가 있어서 이론상 전종목 인식 가능하지만, `_load_stock_cache()` (:176)가 **pykrx 로드 실패 시 조용히 넘어감** (:213 `logger.warning` 후 진행). Railway 컨테이너에서 pykrx가 KRX 접속 차단당하면 캐시가 비고, KIS 마스터파일 폴백도 SSL 무시·바이트 슬라이싱으로 깨지기 쉬움
- 종목 못 찾으면 `stock_ctx=""`로 AI에게 넘어가고, AI는 `[[NEED_SEARCH]]` 태그를 붙일 수도 안 붙일 수도 있음 (:5147 프롬프트 지시에만 의존)
- Gemini 직접 경로(폴백)에서는 아예 **8개 종목 하드코딩** (:5705)
→ "모른다" 응답은 AI 판단이 아니라 **종목 캐시가 비었거나 폴백 경로를 탄 것**.

### 증상 B. 거짓 보고 (실제와 다른 매매 내역)
**원인: 두 가지가 겹침**
1. **대화 히스토리 오염** — `_ask_openwebui()` (:5616~)에서 `_save_chat_history(session_id, "user", message)`의 `message`는 사용자 원문이 아니라 **포트폴리오 컨텍스트 + 2,000자 규칙문 + 출력 프로토콜이 전부 붙은 full_msg** (:5153 조립 → :5197 호출 → :5691 저장). 다음 대화 때 최근 8턴(:5537 `max_turns=8`)을 다시 넣으니, **과거 시점의 계좌 상태 8개**가 현재 상태와 함께 프롬프트에 들어감 → 모델이 옛 데이터를 현재로 착각.
2. **프롬프트 텍스트로만 막고 있음** — :5163 "[사실 기반 답변 필수] … 지어내는 것은 심각한 오류다"라고 써놨지만 코드 검증 없음. 리포트류(`_jarvis_unified_daily_report` :1572, `_jarvis_closing_report` :972)는 DB 조회값을 넣긴 하나 최종 문장은 LLM이 생성.
- 반면 stock_trader의 `_six_hour_report` (:970)와 `notify_buy/sell`은 **템플릿 + DB값**이라 거짓이 끼어들 여지 없음 → 이 방식이 정답.

### 증상 C. 영상 학습했다는데 기억 못함
**원인 확정 — 원문을 저장하지 않는 설계**
- `_learn_from_url()` (:1159): 자막 최대 18,000자 → LLM에 "원칙 3~5개, 각 40자 이내로 뽑아라"(:1149 `_LEARN_PROMPT_BASE`) → **그 3~5줄만** jarvis_notes(category='knowledge')에 저장. 영상 원문·요약·출처 URL 본문은 **버림**.
- 이후 판단 시 주입되는 지식은 `_get_jarvis_knowledge(5~6)` — 최근 **5~6줄**뿐.
- 자막 실패 시 Gemini 영상 직접 시청 폴백(:1133) → 이것도 원칙 몇 줄만 추출.
- `_jarvis_knowledge_curate()` (:1984, 토요일)가 지식을 재정리·삭제까지 함.
→ "저 영상에서 뭐라고 했지?"에 답할 데이터 자체가 DB에 없음. 학습이 아니라 **한 줄 요약 추출**.

### 증상 D. 하루 매수매도 0건
**원인: 정상 보류와 오류를 구분할 로그가 없음 + 게이트가 매우 많음**
- stock_trader 매수 루프 (:730~) 한 종목이 자비스에 도달하기까지 `continue` 게이트: 보유중 / 데이터<21봉 / 룰 신호 BUY 아님 / 현재가 0 / 쿨다운 / 잔고조회 실패 / 잔고<10만 / 수급 -2 / 뉴스 -2 / 자비스 30분 SKIP 쿨다운 / 익절 후 재매수 금지 → **10개**
- 자비스 도달 후에도 :7020 당일 손절 2회면 차단, 최대 포지션 수 도달 시 break
- 대부분 `logger.debug`(:857, :866 등)라 Railway 기본 로그 레벨에서 **안 보임**. 결과적으로 "왜 0건인지"를 사후에 알 방법이 없음.
- `_ask_openwebui` 실패 → `_ask_gemini_direct` 폴백 → 그것도 실패 시 `"❌ AI 오류"` 문자열 반환 → `jarvis_signal`에서 정규식으로 EXECUTE/SKIP 찾다 못 찾으면 SKIP 처리 → **AI 장애가 SKIP으로 둔갑**.

### 증상 E. "판단"이 고정 룰이었다는 느낌
**실제: 절반은 맞고 절반은 틀림**
- 매수 **후보 감지**는 100% 룰 (stock_trader/strategy/rsi.py·ma_cross.py·bollinger.py — RSI 30 반등, MA 교차 등). 룰이 BUY를 안 내면 AI는 호출조차 안 됨.
- 최종 **매수 판단**(`jarvis_signal` :6982)과 **익절 판단**(`jarvis_exit_decision` :6936 HOLD/HALF/ALL)은 실제 LLM 호출. 다만 LLM 응답에서 정규식으로 키워드만 뽑고(:6975 `\b(HOLD|HALF|ALL)\b`), 없으면 HOLD 기본값.
- 손절 -7%·급등 절반매도·당일 손절 2회 차단은 룰 (의도된 안전장치).
→ 문제는 "AI가 없다"가 아니라 **AI가 룰이 걸러준 극소수 후보에 대해서만, 정보도 부족한 상태(지식 5줄·차트 텍스트 한 덩어리)로 판단**한다는 것. 그리고 A~D 때문에 그 판단 품질도 낮음.

### 증상 F. 같은 실수 반복 / 했던 말 반복
- 위 B의 히스토리 오염이 직접 원인. 규칙문 2,000자가 8턴 반복 주입 → 모델이 규칙을 되뇌거나 이전 답을 복창.
- 장기기억 검색 `_search_past_chats()` (:4830)은 **ILIKE 키워드 4개 OR 매칭** → 의미 기반이 아니라 단어가 우연히 겹치는 옛 대화를 끌어옴.
- `_jarvis_evening_review`(:1394)가 교훈(lesson)을 만들지만 판단 시 3줄(:7050 `_get_jarvis_lessons(3)`)만 주입.

### 증상 G. 코인 손실 (폐기 결정 — 참고만)
- crypto_trader는 WORK_LOG 기준 ML 제거 후 순수 RSI 수치 매매(+1%/+3% 익절, -5% 손절). AI 판단은 +3% 이상일 때만. 낮 익절 +1%는 수수료·슬리피지 고려 시 구조적으로 불리.

---

## 3. 구조적 문제 (증상 밖에서 발견)

1. **7,651줄 단일 파일** — 채팅·판단·학습·스케줄·웹·API가 한 곳. 어느 한 기능 수정이 다른 기능을 깨기 쉬움. 커밋 로그의 `# force-redeploy` 주석이 그 증거.
2. **규칙 40% 정규식 선처리** — `_jarvis_chat_impl`에서 AI 도달 전 정규식/키워드 분기 9개(지시·설정·감시·학습목록·지식삭제·URL학습·차트·승인·매매). 사용자가 원한 "AI가 이해하고 판단"과 정반대 구조이고, 정규식에 안 걸리면 AI로, 걸리면 AI 안 거침 → 응답 일관성 없음.
3. **LLM 프록시 이중화** — Open WebUI(외부 시스템프롬프트) ↔ Gemini 직접(코드 내 시스템프롬프트) 두 경로. 어느 게 도는지 코드만 봐선 모름.
4. **스키마 정본 부재** — CREATE/ALTER가 코드 곳곳에 흩어짐.
5. **에러 삼킴** — `except: pass` / `except Exception: pass` 다수, 실패가 SKIP·HOLD·빈문자열로 변환됨.
6. **세션 공유** — 웹·텔레그램이 같은 session_id를 쓰고 웹 대화가 텔레그램으로 미러링(:4923)되어 한쪽 대화가 다른 쪽 컨텍스트를 오염.

---

## 4. 살릴 것 / 버릴 것

### 살릴 것 (그대로 또는 소폭 수정)
- data_collector 전체 (수집 로직은 독립적이고 문제 보고 없음)
- stock_trader의 룰 전략 3종 + 손절/급등매도/재매수금지 안전장치 (실행 레이어로 그대로)
- KIS 주문 래퍼 `kis_trader.py`, `_kis_stock_order`
- 템플릿 기반 알림 (`common/telegram.py`, `_six_hour_report`)
- DB 테이블 데이터 (OHLCV·지표·수급·공시·뉴스·거래내역) — 스키마 정본만 새로 잡고 데이터는 보존
- ml/ (백테스트·피처·모델) — 별도 검증 후

### 버릴 것 (재작성)
- dashboard/main.py의 자비스 관련 전부: `_jarvis_chat_impl`, `_ask_openwebui`, `_ask_gemini_direct`, `jarvis_signal`, `jarvis_exit_decision`, `_learn_from_url`, 스케줄러 내 자비스 작업들, 정규식 명령 핸들러 9종
- Open WebUI 경유 구조 (프로바이더는 코드에서 직접 호출)
- jarvis_memory / jarvis_notes 저장 방식 (원문 미보존, 규칙문 오염)
- crypto_trader 전체 + Dockerfile.crypto + crypto 관련 API·화면
- 웹 채팅 ↔ 텔레그램 세션 공유·미러링

### 새로 만들 것
- 질문 라우터 (정규식 분기 대신 LLM 의도분류 1회 → 도구 호출)
- 종목 조회기 (watchlist 무관, KIS API 직접, 전종목 마스터는 별도 배치로 관리)
- 판단 로그 테이블 (매 사이클 결과+사유 기록, SKIP과 오류 구분)
- 학습 저장소 (원문·요약·원칙·출처를 분리 저장, 의미 검색 가능하게)
- 대화 메모리 (사용자 원문만 저장, 컨텍스트는 호출 시점에 조립)
- 스키마 정본 (마이그레이션 파일)

---

## 5. 제가 확인 못 한 것 → OpenClaw FRIDAY 조사 항목

| 항목 | 확인 방법 | 왜 필요한가 |
|---|---|---|
| `OPENWEBUI_API_TOKEN` 설정 여부 | Railway dashboard 서비스 Variables | Open WebUI 경로 vs Gemini 직접 경로 중 실제로 뭐가 도는지 |
| Open WebUI "autotrader-jarvis" 모델 정의 | Open WebUI 관리 화면 | 거기 시스템 프롬프트가 코드 프롬프트와 어떻게 다른지 |
| stock_master 행 수 | `SELECT COUNT(*) FROM stock_master` | 2,000 미만이면 증상 A 원인 확정 |
| jarvis_memory 샘플 5건 | `SELECT content FROM jarvis_memory ORDER BY created_at DESC LIMIT 5` | 규칙문 오염 실증 |
| jarvis_notes 카테고리별 건수 | `SELECT category, COUNT(*) FROM jarvis_notes GROUP BY 1` | 학습 지식이 실제로 몇 줄 남아있는지 |
| trade_journal 최근 30일 SKIP 사유 분포 | `SELECT jarvis_decision, COUNT(*) ... ` | 0건 날의 원인 분포 |
| stock-trader 로그 레벨 | Railway 로그 설정 | debug 로그가 실제로 안 보이는지 |
| 현재 살아있는 봇 토큰 4종 각각의 실제 봇 이름 | Railway Variables + 텔레그램 | 이름 정리(STARK/JARVIS) 매핑용 |

---

## 6. 결론
- 증상 A·C·F는 **설계 결함**이라 패치로 안 됨 (재작성 대상 확정)
- 증상 B·D는 설계 결함 + 로그 부재 → 재작성 시 "실측값 템플릿 + 판단 로그" 원칙으로 자연 해결
- 증상 E는 오해가 섞여 있음: AI 판단은 존재하지만 룰이 앞에서 다 걸러버려 거의 안 불림 + 불려도 정보 부족
- 재설계 시 **실행 레이어(룰·주문·수집)는 대부분 재사용 가능**, 갈아엎을 건 "자비스 두뇌·기억·학습·대화" 부분 = dashboard/main.py의 약 60%
