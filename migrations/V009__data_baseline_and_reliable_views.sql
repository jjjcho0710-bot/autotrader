-- V009__data_baseline_and_reliable_views.sql
-- 데이터 기준선(data_baseline) 테이블과 기준선 이후 데이터만 보여주는 뷰 추가.
--
-- 배경: 2026-09-28 이전 거래·판단 기록은 옛 모의계좌와 버그 구간의 데이터라서
-- 손절/익절 룰 분석에 섞이면 안 된다. 다만 기존 데이터는 삭제하지 않고 그대로 둔다
-- (원본 테이블 구조·행 모두 변경 없음). 분석 쿼리는 아래 *_reliable 뷰를 사용한다.
--
-- 1) data_baseline: 키-값 형태의 기준선 테이블. 'reliable_trading_data_from' 행의
--    effective_at(2026-09-28 11:00 KST) 이후 시각의 행만 "신뢰 가능"으로 본다.
--    계좌번호 등 식별 정보는 넣지 않는다.
-- 2) trade_history_reliable: trade_history 중 ts >= 기준선인 행.
-- 3) stark_decisions_reliable: stark_decisions 중 decided_at >= 기준선인 행.
--    두 뷰 모두 JOIN으로 기준선 행을 붙이므로 (a) 기준선 행이 없으면 결과가 비고,
--    (b) 시각이 NULL인 행은 WHERE에서 제외된다. 경계 시각(==effective_at)은 포함한다.
--
-- 영향 서비스: 기존 테이블을 변경하지 않으므로 기존 서비스(stock_trader, dashboard,
-- data_collector 등)에는 영향이 없다. 뷰는 SELECT t.* 로 만들어져 생성 시점의 컬럼
-- 목록으로 고정된다 (이후 원본 테이블에 컬럼을 추가해도 뷰에는 자동 반영되지 않음).

-- Up

CREATE TABLE IF NOT EXISTS data_baseline (
    key TEXT PRIMARY KEY,
    effective_at TIMESTAMPTZ NOT NULL,
    note TEXT
);

INSERT INTO data_baseline (key, effective_at, note)
VALUES (
    'reliable_trading_data_from',
    '2026-09-28 11:00:00+09',
    '새 모의계좌 적용 이후. 이전은 옛 계좌와 버그 구간'
)
ON CONFLICT (key) DO NOTHING;

CREATE OR REPLACE VIEW trade_history_reliable AS
SELECT t.*
FROM trade_history t
JOIN data_baseline b ON b.key = 'reliable_trading_data_from'
WHERE t.ts IS NOT NULL
  AND t.ts >= b.effective_at;

CREATE OR REPLACE VIEW stark_decisions_reliable AS
SELECT d.*
FROM stark_decisions d
JOIN data_baseline b ON b.key = 'reliable_trading_data_from'
WHERE d.decided_at IS NOT NULL
  AND d.decided_at >= b.effective_at;

-- Down

DROP VIEW IF EXISTS stark_decisions_reliable;
DROP VIEW IF EXISTS trade_history_reliable;
DROP TABLE IF EXISTS data_baseline;
