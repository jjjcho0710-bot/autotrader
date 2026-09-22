-- V002__create_stocks_and_migrate.sql
-- stock_master → stocks 완전 교체.
-- stocks는 stock_master보다 확장 가능하도록 market/is_active/created_at을 추가하되
-- symbol(PK)/name/updated_at은 그대로 유지해 애플리케이션 조회 호환성을 지킨다.

-- Up

CREATE TABLE IF NOT EXISTS stocks (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(80),
    market VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO stocks (symbol, name, updated_at)
SELECT symbol, name, updated_at FROM stock_master
ON CONFLICT (symbol) DO UPDATE
SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at;

DROP TABLE stock_master;

-- Down

CREATE TABLE IF NOT EXISTS stock_master (
    symbol VARCHAR(10) PRIMARY KEY, name VARCHAR(80), updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO stock_master (symbol, name, updated_at)
SELECT symbol, name, updated_at FROM stocks
ON CONFLICT (symbol) DO UPDATE
SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at;

DROP TABLE stocks;
