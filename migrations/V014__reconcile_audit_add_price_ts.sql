-- V014__reconcile_audit_add_price_ts.sql
-- [AT] feat/reconcile-manual (PM 승인)
--
-- reconcile_trades_period의 지정 보정(fix_delete/fix_update)에서 DELETE는 삭제되는
-- trade_history 행을 완전히 복원 가능해야 한다(symbol/side/old_quantity/old_amount는
-- V013의 기존 컬럼에 이미 있음 — price와 ts만 추가로 필요). UPDATE 감사 기록에도 같은
-- 컬럼을 채워 일관되게 기록한다.

-- Up

ALTER TABLE reconcile_audit ADD COLUMN IF NOT EXISTS old_price NUMERIC(20,2);
ALTER TABLE reconcile_audit ADD COLUMN IF NOT EXISTS old_ts TIMESTAMPTZ;

-- Down

ALTER TABLE reconcile_audit DROP COLUMN IF EXISTS old_ts;
ALTER TABLE reconcile_audit DROP COLUMN IF EXISTS old_price;
