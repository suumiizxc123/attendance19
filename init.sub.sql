CREATE TABLE IF NOT EXISTS hr_employee (
    id          SERIAL PRIMARY KEY,
    employee_id INT,
    name        TEXT,
    work_email  TEXT,
    barcode     TEXT,
    last_check_in  TIMESTAMPTZ,
    last_check_out TIMESTAMPTZ,
    raw_data    JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);
