-- V008__stark_decisions_features_and_symbol_index.sql
-- STARK v2 3단계: 판단 엔진(stark/decision_engine.py)이 매 판단마다 stark_decisions에
-- 대량 INSERT를 하게 되면서 발견된 두 가지 개선 사항.
--
-- 1) features_json: 판단 시점에 모델에 입력된 피처(스냅샷)를 남겨 학습·복기에
--    쓸 수 있도록 JSONB 컬럼 추가. NULL 허용 — 피처를 넘기지 않는 호출부(rule 기반
--    판단 등)도 기존처럼 계속 동작해야 하므로 NOT NULL을 걸지 않는다.
-- 2) idx_stark_decisions_symbol_decided_at: get_decisions_by_symbol()이 실행하는
--    "WHERE symbol=$1 ORDER BY decided_at DESC" 조회는 V004의
--    idx_stark_decisions_decided_at_symbol(decided_at, symbol) 인덱스로는 symbol이
--    선행 컬럼이 아니라서 효율적으로 탈 수 없다. symbol을 선행 컬럼으로 하는 인덱스를
--    추가한다 (decided_at DESC로 걸어 정렬 방향까지 인덱스로 커버).
--    기존 인덱스는 symbol 없이 decided_at만으로 최근 이력을 훑는
--    get_recent_decisions()에 계속 필요하므로 그대로 둔다.

-- Up

ALTER TABLE stark_decisions ADD COLUMN IF NOT EXISTS features_json JSONB;
CREATE INDEX IF NOT EXISTS idx_stark_decisions_symbol_decided_at
    ON stark_decisions (symbol, decided_at DESC);

-- Down

DROP INDEX IF EXISTS idx_stark_decisions_symbol_decided_at;
ALTER TABLE stark_decisions DROP COLUMN IF EXISTS features_json;
