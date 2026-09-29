-- V010__position_management_policy.sql
-- 보유중 종목 추가매수(물타기) 원칙을 jarvis_notes에 별도 category로 등록.
--
-- 배경: 2026-09-29 쓰리빌리언(394800) 사례 — Jarvis가 하락 중인 보유 종목에
-- 3연속 추가매수(물타기)를 하면서, 신규 진입 판단용 원칙(K79,K80,K82,K83 —
-- "매수세 강한 양봉", "5일선 위" 등)을 근거로 잘못 인용했다. jarvis_notes의
-- category='knowledge_core' 27건을 확인한 결과 물타기/추가매수/급락/변동성
-- 관련 원칙이 전혀 없었던 게 원인. 신규 진입 원칙과 섞여 프롬프트에 노출되면
-- 다시 오인용될 수 있으므로, 별도 category('position_management')로 등록해
-- stark/context_collector.py가 별도 섹션으로 구분해서 주입한다
-- ([보유중 종목 추가매수 원칙] — [학습한 매매 원칙]과 분리).
--
-- 실제 강제는 이 원칙 텍스트(AI 프롬프트)만으로 하지 않고
-- stark/execution_guard.py의 코드 레벨 검사로도 이중 집행한다
-- (AI가 EXECUTE로 판단해도 조건 미충족이면 SKIP 처리).

-- Up

INSERT INTO jarvis_notes (category, content, is_active)
VALUES (
    'position_management',
    '이미 보유 중인 종목에 대한 추가매수(물타기)는 원칙적으로 금지한다. '
    '예외: 최초 매수가 대비 현재가가 -3% 이내인 경우에 한해, 종목당 평생 1회만 허용한다. '
    '투자주의/경고/투자위험 종목, 오늘 VI가 발동한 종목은 추가매수뿐 아니라 신규매수도 이미 차단되어 있다. '
    '이 원칙은 신규 진입 판단 원칙(예: K79~K83 "매수세 강한 양봉", "5일선 위" 등)과는 별개이니 '
    '이미 보유 중인 종목의 추가매수 판단 근거로 신규 진입 원칙을 인용하지 말 것.',
    TRUE
);

-- Down

DELETE FROM jarvis_notes WHERE category = 'position_management';
