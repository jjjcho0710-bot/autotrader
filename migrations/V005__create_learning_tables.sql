-- V005__create_learning_tables.sql
-- STARK v2 2단계: 외부 학습 소스(learning_sources) 및 파생 규칙(learning_rules) 테이블 신설.
-- 영향받는 서비스: dashboard (신규 chat/ 패키지에서 참조 예정, 이번 마이그레이션에는 DDL만 포함)

-- Up

CREATE TABLE IF NOT EXISTS learning_sources (
    source_id BIGSERIAL PRIMARY KEY,
    source_type VARCHAR(20) NOT NULL,
    url VARCHAR(1000) NOT NULL,
    title VARCHAR(255),
    author VARCHAR(100),
    content_raw TEXT NOT NULL,
    hint VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS learning_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES learning_sources(source_id) ON DELETE CASCADE,
    rule_text TEXT NOT NULL,
    importance_score INT NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Down

DROP TABLE IF EXISTS learning_rules;
DROP TABLE IF EXISTS learning_sources;
