CREATE TABLE IF NOT EXISTS requests (
    id         SERIAL PRIMARY KEY,
    source_id  INT,
    method     TEXT NOT NULL,
    path       TEXT NOT NULL,
    headers    JSONB,
    body       JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
