-- queue_failed_jobs — table contract for gofailedjobs.
-- failed_at is stored as a local-time string in the format "2006-01-02 15:04:05"
-- (no timezone); normalize TZ on the application side if needed.

-- PostgreSQL
CREATE TABLE IF NOT EXISTS queue_failed_jobs (
    id                   BIGSERIAL PRIMARY KEY,
    connection           TEXT NOT NULL DEFAULT '',
    queue                TEXT NOT NULL,
    payload              TEXT NOT NULL,
    exception            TEXT NOT NULL DEFAULT '',
    failed_at            TEXT NOT NULL,
    maintenance_comment  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_queue_failed_jobs_queue ON queue_failed_jobs (queue);

-- MySQL / MariaDB
-- CREATE TABLE IF NOT EXISTS queue_failed_jobs (
--     id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
--     connection           VARCHAR(255) NOT NULL DEFAULT '',
--     queue                VARCHAR(255) NOT NULL,
--     payload              LONGTEXT NOT NULL,
--     exception            LONGTEXT NOT NULL,
--     failed_at            VARCHAR(32) NOT NULL,
--     maintenance_comment  TEXT NOT NULL
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
--
-- CREATE INDEX idx_queue_failed_jobs_queue ON queue_failed_jobs (queue);
