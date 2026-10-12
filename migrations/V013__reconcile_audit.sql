-- V013__reconcile_audit.sql
-- [AT] feat/reconcile-apply (PM 승인)
--
-- KIS 체결내역 기준 trade_history 보정 도구(dashboard/kis_history_reconcile.py
-- apply_reconcile_plan)의 감사 로그. 보정으로 생기는 모든 INSERT/UPDATE는 반드시
-- 이 테이블에 같은 트랜잭션으로 한 행을 남겨, 언제·무엇이·왜 바뀌었는지 추적한다.
-- trade_history_id는 INSERT/UPDATE 대상이 된 trade_history 행의 id.

-- Up

CREATE TABLE IF NOT EXISTS reconcile_audit (
    id BIGSERIAL PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    action VARCHAR(10) NOT NULL,            -- INSERT / UPDATE
    trade_history_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(5) NOT NULL,
    trade_date DATE NOT NULL,
    old_quantity NUMERIC(20,8),
    new_quantity NUMERIC(20,8),
    old_amount NUMERIC(20,2),
    new_amount NUMERIC(20,2),
    old_pnl NUMERIC(20,2),
    new_pnl NUMERIC(20,2),
    note TEXT
);

-- Down

DROP TABLE IF EXISTS reconcile_audit;
