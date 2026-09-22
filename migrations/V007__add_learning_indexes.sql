-- V007__add_learning_indexes.sql
-- STARK v2 2단계: 학습 테이블 조회 성능 인덱스 및 jarvis_memory 세션별 조회 복합 인덱스 추가.

-- Up

CREATE INDEX IF NOT EXISTS idx_learning_sources_url ON learning_sources (url);
CREATE INDEX IF NOT EXISTS idx_learning_rules_source_id ON learning_rules (source_id);
CREATE INDEX IF NOT EXISTS idx_jarvis_memory_session_created ON jarvis_memory (session_id, created_at);

-- Down

DROP INDEX IF EXISTS idx_jarvis_memory_session_created;
DROP INDEX IF EXISTS idx_learning_rules_source_id;
DROP INDEX IF EXISTS idx_learning_sources_url;
