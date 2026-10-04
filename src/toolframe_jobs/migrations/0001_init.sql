CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    application_id TEXT NOT NULL,
    source TEXT NOT NULL,
    logical_key TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    handler TEXT NOT NULL,
    payload TEXT NOT NULL,
    result TEXT,
    progress TEXT,
    checkpoint TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_retries INTEGER NOT NULL CHECK (max_retries >= 0 AND max_retries <= 10),
    retry_policy TEXT NOT NULL CHECK (retry_policy IN ('never', 'safe')),
    retry_delay REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'retry_wait', 'succeeded', 'failed', 'cancelled', 'interrupted')),
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    lease_owner TEXT,
    attempt_token TEXT,
    lease_expires_at REAL,
    attempt_deadline REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    eligible_at REAL NOT NULL,
    error TEXT,
    UNIQUE (application_id, source, logical_key)
);
CREATE INDEX jobs_due ON jobs (application_id, source, status, eligible_at);
CREATE INDEX jobs_expired ON jobs (application_id, source, status, lease_expires_at);
