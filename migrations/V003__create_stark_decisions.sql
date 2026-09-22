-- V003__create_stark_decisions.sql
-- STARK v2 판단 레이어(AI HOLD/HALF/ALL, 매수/매도/보류 신호) 로그 테이블.
-- docs/STARK_PLAN.md 2번·6번 원칙: "판단(AI)과 실행(룰)을 코드 레벨로 분리"하고
-- "모든 사이클은 무언가를 기록한다 — 0건인 날도 왜 0건인지 조회 가능해야 함"을
-- 만족시키기 위해, 판단이 SKIP/HOLD로 끝나거나 모델 호출 자체가 실패한 경우도
-- 반드시 한 행으로 남긴다(decision='SKIP' 등 + reason에 사유 기록).
--
-- 판단시각 컬럼명은 반드시 decided_at (다른 테이블의 ts/created_at과 구분해
-- "판단이 내려진 시각"임을 명확히 하기 위함, 실제 체결시각과는 다를 수 있음).

-- Up

CREATE TABLE IF NOT EXISTS stark_decisions (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(50),
    decision VARCHAR(20) NOT NULL,          -- BUY/SELL/HOLD/HALF/ALL/SKIP 등
    confidence NUMERIC(6,4),                -- 0~1 판단 확신도 (모델이 제공한 경우)
    reason TEXT,                            -- 판단 근거 요약 (사람이 읽는 짧은 사유)
    rationale TEXT,                         -- 모델 응답 원문/상세 근거 (학습·복기용, 없을 수 있음)
    strategy VARCHAR(50),                   -- 후보를 만든 룰 전략명 (있는 경우)
    source VARCHAR(20) DEFAULT 'ai',        -- 판단 주체: ai / rule / manual
    model_name VARCHAR(50),                 -- 판단에 사용된 모델명 (모델 호출 실패 시 NULL 가능)
    executed BOOLEAN DEFAULT FALSE,         -- 실행 레이어가 실제로 주문을 실행했는지
    order_success BOOLEAN,                  -- executed=TRUE인 경우 주문 성공 여부
    price NUMERIC(20,2),
    quantity NUMERIC(20,8),
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Down

DROP TABLE IF EXISTS stark_decisions;
