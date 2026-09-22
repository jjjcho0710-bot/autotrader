-- V001__baseline_tables.sql
-- STARK v2 DB 마이그레이션 정본화 — 베이스라인 스키마
--
-- 이 파일은 이전까지 common/database.py(Database._create_tables)와
-- dashboard/main.py 곳곳에 흩어져 있던 암묵적 `CREATE TABLE IF NOT EXISTS` /
-- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` DDL을 한 곳에 정본화한 것이다.
--
-- 중복 정의 판단 근거(자세한 내용은 PR 설명 참고):
--   - ml_predictions / stock_daily_ohlcv / stock_indicators / strategy_config:
--     database.py와 dashboard/main.py 양쪽에 정의되어 있었으나 컬럼 구성은 동일.
--     인덱스 방식만 달라(database.py는 별도 CREATE UNIQUE INDEX, main.py는 인라인 UNIQUE)
--     database.py 쪽의 "이름 있는 별도 인덱스" 스타일을 정본으로 채택(추후 인덱스만
--     독립적으로 관리 가능하도록).
--   - strategy_config seed 데이터: main.py 쪽이 crypto_trader 전략까지 포함한 상위집합이라
--     데이터 연속성을 위해 그대로 채택(운영 판단이 아니라 기존 동작 보존 목적).
--   - jarvis_notes: 3곳(1213/4522/5074) 모두 동일 스키마 + is_active 보강용
--     ALTER TABLE이 반복 실행되고 있었음 → is_active를 처음부터 포함한 단일 정의로 통합.
--   - ml_models: main.py에만 정의돼 있었으나 실제 사용처(ml/model.py `_save_model`,
--     `get_all_predictions`)가 `samples` 컬럼을 필요로 함(원래 DDL엔 없었음 — 운영 DB에서
--     별도로 컬럼이 추가됐거나 잠재적 버그였을 가능성). 정본에는 `samples`를 포함시킴.
--   - trade_history.created_at / trade_journal.principles / watchlist.priority /
--     notifications.meta: 모두 런타임에 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`로
--     보강되던 컬럼들 → 베이스라인 정의에 처음부터 포함시켜 ALTER 문 자체를 제거.
--   - stock_master: 이번 마이그레이션에서 실제 그대로 재현(정본화)한 뒤, V002에서
--     `stocks` 테이블로 완전 교체한다. (V001은 "과거에 실제 존재했던 스키마"를
--     정확히 기록하는 것이 목적이고, 교체는 V002가 담당)

-- Up

CREATE TABLE IF NOT EXISTS stock_ohlcv (
    id BIGSERIAL PRIMARY KEY, symbol VARCHAR(10) NOT NULL,
    ts TIMESTAMPTZ NOT NULL, open BIGINT, high BIGINT,
    low BIGINT, close BIGINT, volume BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ohlcv_symbol_ts ON stock_ohlcv (symbol, ts);

CREATE TABLE IF NOT EXISTS trade_history (
    id BIGSERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
    asset_type VARCHAR(10) NOT NULL, symbol VARCHAR(20) NOT NULL,
    side VARCHAR(5) NOT NULL, price NUMERIC(20,2), quantity NUMERIC(20,8),
    amount NUMERIC(20,2), strategy VARCHAR(50), pnl NUMERIC(20,2),
    ts TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS balance_snapshot (
    id BIGSERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
    total_krw NUMERIC(20,2), cash_krw NUMERIC(20,2),
    eval_krw NUMERIC(20,2), pnl_today NUMERIC(20,2),
    ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_daily_ohlcv (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts DATE NOT NULL,
    open BIGINT, high BIGINT, low BIGINT, close BIGINT,
    volume BIGINT, change_rate NUMERIC(8,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_daily_symbol_ts
    ON stock_daily_ohlcv (symbol, ts);

CREATE TABLE IF NOT EXISTS stock_indicators (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts DATE NOT NULL,
    rsi14 NUMERIC(8,2),
    macd NUMERIC(12,2), macd_signal NUMERIC(12,2), macd_hist NUMERIC(12,2),
    bb_upper NUMERIC(12,2), bb_middle NUMERIC(12,2), bb_lower NUMERIC(12,2),
    bb_pct NUMERIC(8,4),
    atr14 NUMERIC(12,2),
    stoch_k NUMERIC(8,2), stoch_d NUMERIC(8,2),
    sma5 NUMERIC(12,2), sma20 NUMERIC(12,2), sma60 NUMERIC(12,2),
    ema12 NUMERIC(12,2), ema26 NUMERIC(12,2),
    golden_cross BOOLEAN, dead_cross BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_indicators_symbol_ts
    ON stock_indicators (symbol, ts);

CREATE TABLE IF NOT EXISTS ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    model_name VARCHAR(50),
    buy_prob NUMERIC(6,4),
    sell_prob NUMERIC(6,4),
    signal VARCHAR(10),
    features JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_config (
    id SERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
    name VARCHAR(50) NOT NULL, is_active BOOLEAN DEFAULT FALSE,
    params JSONB DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bot, name)
);
INSERT INTO strategy_config (bot, name, is_active, params) VALUES
('stock_trader','MA크로스',true,'{"short":5,"long":20,"stop_loss":-2,"take_profit":5,"buy_amount":500000,"max_positions":5}'),
('stock_trader','RSI반등',false,'{"period":14,"entry":30,"exit":60,"stop_loss":-2,"buy_amount":500000}'),
('stock_trader','볼린저밴드',false,'{"period":20,"std":2,"stop_loss":-2,"buy_amount":500000}'),
('crypto_trader','MACD',true,'{"fast":12,"slow":26,"signal":9,"candle_min":60,"stop_loss":-3,"take_profit":7,"buy_amount":500000}'),
('crypto_trader','변동성돌파',false,'{"k":0.5,"candle_min":1440,"close_time":"09:00","stop_loss":-3}'),
('crypto_trader','RSI과매도',false,'{"period":14,"entry":25,"exit":65,"stop_loss":-3,"buy_amount":500000}')
ON CONFLICT (bot, name) DO NOTHING;

CREATE TABLE IF NOT EXISTS watchlist (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(50),
    added_by VARCHAR(20) DEFAULT 'manual',
    reason TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    priority BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_supply (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    foreign_net BIGINT DEFAULT 0,
    institution_net BIGINT DEFAULT 0,
    individual_net BIGINT DEFAULT 0,
    foreign_hold_ratio NUMERIC(6,2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date)
);

CREATE TABLE IF NOT EXISTS stock_disclosure (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    corp_name VARCHAR(100),
    report_name VARCHAR(200),
    rcept_dt VARCHAR(20),
    rcept_no VARCHAR(20) UNIQUE,
    is_important BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jarvis_memory (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_jarvis_memory_session ON jarvis_memory (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS stock_news_sentiment (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    sentiment_score INTEGER DEFAULT 0,
    signal VARCHAR(10) DEFAULT 'NEUTRAL',
    summary TEXT,
    news_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date)
);

-- dashboard/main.py 인라인 정의(중복분 제외 신규 6개)

CREATE TABLE IF NOT EXISTS stock_master (
    symbol VARCHAR(10) PRIMARY KEY, name VARCHAR(80), updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ml_models (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    model_data TEXT NOT NULL,
    accuracy NUMERIC(6,2),
    samples INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, model_name)
);

CREATE TABLE IF NOT EXISTS jarvis_notes (
    id SERIAL PRIMARY KEY,
    category VARCHAR(30) DEFAULT 'note',
    content TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS principle_stats (
    principle_id INT PRIMARY KEY,
    applied INT DEFAULT 0,
    hits INT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    title VARCHAR(120), body TEXT,
    is_read BOOLEAN DEFAULT FALSE,
    meta TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trade_journal (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT NOW(),
    bot VARCHAR(20), symbol VARCHAR(15), name VARCHAR(50),
    action VARCHAR(10), strategy VARCHAR(50),
    signal_reason TEXT,
    jarvis_decision VARCHAR(10), jarvis_reason TEXT,
    executed BOOLEAN, order_success BOOLEAN,
    price NUMERIC, qty NUMERIC,
    source VARCHAR(10) DEFAULT 'auto',
    eval_price NUMERIC, eval_pnl_rate NUMERIC, eval_at TIMESTAMPTZ,
    principles TEXT
);

-- Down

DROP INDEX IF EXISTS idx_jarvis_memory_session;
DROP INDEX IF EXISTS idx_stock_indicators_symbol_ts;
DROP INDEX IF EXISTS idx_stock_daily_symbol_ts;
DROP INDEX IF EXISTS idx_stock_ohlcv_symbol_ts;

DROP TABLE IF EXISTS trade_journal;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS principle_stats;
DROP TABLE IF EXISTS jarvis_notes;
DROP TABLE IF EXISTS ml_models;
DROP TABLE IF EXISTS stock_master;
DROP TABLE IF EXISTS stock_news_sentiment;
DROP TABLE IF EXISTS jarvis_memory;
DROP TABLE IF EXISTS stock_disclosure;
DROP TABLE IF EXISTS stock_supply;
DROP TABLE IF EXISTS watchlist;
DROP TABLE IF EXISTS strategy_config;
DROP TABLE IF EXISTS ml_predictions;
DROP TABLE IF EXISTS stock_indicators;
DROP TABLE IF EXISTS stock_daily_ohlcv;
DROP TABLE IF EXISTS balance_snapshot;
DROP TABLE IF EXISTS trade_history;
DROP TABLE IF EXISTS stock_ohlcv;
