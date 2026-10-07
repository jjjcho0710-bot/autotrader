-- V012__widen_jarvis_decision_column_40.sql
-- [AT] feat/scoring-improvement
--
-- 배경: V011이 trade_journal.jarvis_decision을 VARCHAR(10) → VARCHAR(20)으로
-- 넓혔지만, 이 브랜치에서 추가한 판정 라벨이 20자를 다시 넘는다:
--   MANUAL_UNCERTAIN_FILLED            (23자)
--   ADVICE_APPROVED_UNCERTAIN_FILLED   (32자)
--   PROPOSE_APPROVED_UNCERTAIN_FILLED  (33자, router/handlers/order_handler.py
--                                        handle_proposal_response에 기존부터 있던 값)
-- Postgres는 VARCHAR(n) 초과 INSERT를 자르지 않고 에러를 내므로, 이 값들은
-- dashboard/main.py._log_journal의 try/except에 걸려 조용히 버려진다 — 매매일지
-- 기록 누락, 채점(_score_journal) 과소집계로 이어진다.
--
-- V011은 이미 배포됐으므로 수정하지 않고, 가장 긴 라벨(33자)보다 여유를 둔
-- VARCHAR(40)으로 새로 확장한다. V011과 동일하게 여러 번 실행해도 안전하도록
-- (이미 40자 이상이면 건너뜀) DO 블록으로 현재 길이를 먼저 확인한다.

-- Up

DO $$
BEGIN
    IF (
        SELECT character_maximum_length FROM information_schema.columns
        WHERE table_name = 'trade_journal' AND column_name = 'jarvis_decision'
    ) < 40 THEN
        ALTER TABLE trade_journal ALTER COLUMN jarvis_decision TYPE VARCHAR(40);
    END IF;
END $$;

-- Down
-- 주의: ADVICE_APPROVED_UNCERTAIN_FILLED(32자)·PROPOSE_APPROVED_UNCERTAIN_FILLED
-- (33자)처럼 20자를 넘는 값이 이미 저장돼 있으면 아래 축소는
-- "value too long for type character varying(20)" 에러로 실패한다(의도된 동작 —
-- 데이터 유실 없이 실패해야 롤백이 안전하다). 축소하려면 해당 행을 먼저 정리할 것.

ALTER TABLE trade_journal ALTER COLUMN jarvis_decision TYPE VARCHAR(20);
