CREATE TABLE IF NOT EXISTS requests (
    id         SERIAL PRIMARY KEY,
    method     TEXT NOT NULL,
    path       TEXT NOT NULL,
    headers    JSONB,
    body       JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Trigger: notify on every INSERT/UPDATE
CREATE OR REPLACE FUNCTION notify_request_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('hr_employee', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS request_notify ON requests;
CREATE TRIGGER request_notify
    AFTER INSERT OR UPDATE ON requests
    FOR EACH ROW EXECUTE FUNCTION notify_request_change();
