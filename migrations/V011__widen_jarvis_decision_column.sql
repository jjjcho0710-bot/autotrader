-- V011__widen_jarvis_decision_column.sql
-- [AT] fix/journal-decision-length
--
-- 배경(EDITH 조회): trade_journal.jarvis_decision이 VARCHAR(10)인데
-- stark/execution_guard.py가 소액매수 판단을 "EXECUTE_SMALL"(13자)로 기록한다.
-- Postgres는 VARCHAR(n) 초과 INSERT를 조용히 자르지 않고 에러를 내므로, 이
-- INSERT는 매번 예외가 나서 dashboard/main.py._log_journal의 try/except에
-- 걸려 조용히 버려졌다 — 최근 30일 stark_decisions에는 EXECUTE_SMALL 48건이
-- 있는데 trade_journal에는 0건. 채점(_score_journal)·주간복습·주말학습보고의
-- "실행" 건수가 전부 이만큼 과소집계되고 있었다.
--
-- "EXECUTE_SMALL"(13자)보다 여유를 두고 VARCHAR(20)으로 확장한다.
--
-- 여러 번 실행해도 안전하도록(이미 20자 이상으로 넓어져 있으면 건너뜀) DO 블록으로
-- 현재 길이를 먼저 확인한다 — 단순 ALTER COLUMN TYPE도 같은 타입 재실행 자체는
-- 에러가 나지 않지만, 누군가 이미 더 넓게(예: 30자) 바꿔둔 상태에서 이 마이그레이션이
-- 재실행되어 20자로 되돌리며 그 사이 쌓인 긴 값이 잘리는 사고를 막기 위함이다.

-- Up

DO $$
BEGIN
    IF (
        SELECT character_maximum_length FROM information_schema.columns
        WHERE table_name = 'trade_journal' AND column_name = 'jarvis_decision'
    ) < 20 THEN
        ALTER TABLE trade_journal ALTER COLUMN jarvis_decision TYPE VARCHAR(20);
    END IF;
END $$;

-- Down
-- 주의: EXECUTE_SMALL(13자)처럼 10자를 넘는 값이 이미 저장돼 있으면 아래 축소는
-- "value too long for type character varying(10)" 에러로 실패한다(의도된 동작 —
-- 데이터 유실 없이 실패해야 롤백이 안전하다). 축소하려면 해당 행을 먼저 정리할 것.

ALTER TABLE trade_journal ALTER COLUMN jarvis_decision TYPE VARCHAR(10);
