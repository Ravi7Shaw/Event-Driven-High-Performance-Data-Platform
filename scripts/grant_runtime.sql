-- Run as migration owner on the target database, after `eventvault migrate`.
-- Create the actual LOGIN role/password outside source control, then:
-- GRANT eventvault_runtime TO your_runtime_login;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventvault_runtime') THEN
        CREATE ROLE eventvault_runtime NOLOGIN;
    END IF;
END $$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO eventvault_runtime;
GRANT SELECT, INSERT, UPDATE ON items, event_clock, event_jobs, inventory_activity TO eventvault_runtime;
GRANT SELECT, INSERT ON events, idempotency TO eventvault_runtime;
GRANT SELECT ON schema_migrations TO eventvault_runtime;
GRANT USAGE, SELECT ON SEQUENCE items_id_seq TO eventvault_runtime;
