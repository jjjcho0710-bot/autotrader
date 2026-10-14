-- V015__entry_risk_shadow_and_market_index.sql
-- [AT] feat/entry-risk-shadow (PM 승인)
--
-- EDITH 41건 매수 분석: 저가주(<5,000원) 8/8 손실, 5일 +20% 급등 후 매수 7/9 손실.
-- 차단은 하지 않고(하드룰 변경 없음) 관찰 데이터만 쌓는다.
-- trade_journal에 매수 판단 시점의 저가주/단기급등 플래그를 기록하고(기존 momentum
-- shadow와 동일한 관찰 전용 원칙), 시장지수(코스피/코스닥) 일별 이력을 별도 테이블에
-- 저장해 추후 시장 약세 영향 분석을 가능하게 한다.

-- Up

ALTER TABLE trade_journal ADD COLUMN IF NOT EXISTS low_price BOOLEAN DEFAULT FALSE;
ALTER TABLE trade_journal ADD COLUMN IF NOT EXISTS surge_5d BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS market_index_daily (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    index_code VARCHAR(10) NOT NULL,   -- KOSPI=0001, KOSDAQ=1001
    close NUMERIC(14,2),
    change_rate NUMERIC(8,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, index_code)
);

-- Down

DROP TABLE IF EXISTS market_index_daily;
ALTER TABLE trade_journal DROP COLUMN IF EXISTS surge_5d;
ALTER TABLE trade_journal DROP COLUMN IF EXISTS low_price;
