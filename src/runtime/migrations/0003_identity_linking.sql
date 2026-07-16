CREATE TABLE identity_link_codes (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    code_hash BLOB NOT NULL UNIQUE CHECK(length(code_hash) = 32),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL CHECK(expires_at > created_at)
) STRICT;

CREATE TABLE identity_link_attempts (
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    window_started_at INTEGER NOT NULL,
    failure_count INTEGER NOT NULL CHECK(failure_count BETWEEN 1 AND 5),
    blocked_until INTEGER,
    PRIMARY KEY(provider, subject)
) STRICT;
