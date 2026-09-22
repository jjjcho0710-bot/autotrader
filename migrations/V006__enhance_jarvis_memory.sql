-- V006__enhance_jarvis_memory.sql
-- STARK v2 2단계: jarvis_memory에 채널 구분(channel)과 순수 사용자 발화 여부(is_pure_user) 컬럼 추가.
-- 목적: 웹/텔레그램 등 채널별 세션 분리 및 조립된 컨텍스트(계좌 정보+규칙문)와
-- 순수 사용자 발화를 구분 저장하기 위함(오염 방지).
-- 영향받는 서비스: dashboard(_jarvis_chat_impl, chat/memory.py), STARK 대화 메모리 조회 경로 전반.
-- 기존 행은 모두 channel='web', is_pure_user=TRUE로 채워짐(과거 데이터는 대부분 웹 채널의
-- 순수 사용자 발화였다는 보수적 가정 — 운영 데이터 재분류가 필요하면 별도 백필 작업 요망).

-- Up

ALTER TABLE jarvis_memory ADD COLUMN IF NOT EXISTS channel VARCHAR(20) NOT NULL DEFAULT 'web';
ALTER TABLE jarvis_memory ADD COLUMN IF NOT EXISTS is_pure_user BOOLEAN NOT NULL DEFAULT TRUE;

-- Down

ALTER TABLE jarvis_memory DROP COLUMN IF EXISTS is_pure_user;
ALTER TABLE jarvis_memory DROP COLUMN IF EXISTS channel;
