-- V004__add_indexes.sql
-- 조회 성능 인덱스 추가: 종목명/코드 검색(stocks), 판단 이력 조회(stark_decisions).

-- Up

CREATE INDEX IF NOT EXISTS idx_stocks_symbol_name ON stocks (symbol, name);
CREATE INDEX IF NOT EXISTS idx_stark_decisions_decided_at_symbol ON stark_decisions (decided_at, symbol);

-- Down

DROP INDEX IF EXISTS idx_stark_decisions_decided_at_symbol;
DROP INDEX IF EXISTS idx_stocks_symbol_name;
