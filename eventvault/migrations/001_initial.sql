CREATE TABLE items (
    id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0 AND reserved_quantity <= quantity),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- Serial IDs are allocation order, not commit order. The feed uses a separate
-- transactionally allocated cursor under a late, short-lived global row lock.
CREATE TABLE event_clock (singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton), value BIGINT NOT NULL);
INSERT INTO event_clock VALUES (true, 0);
CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL CHECK (event_type IN ('ITEM_CREATED','ITEM_UPDATED','INVENTORY_RESERVED','INVENTORY_RELEASED')),
    aggregate_type VARCHAR(100) NOT NULL DEFAULT 'item' CHECK (aggregate_type = 'item'),
    aggregate_id BIGINT NOT NULL REFERENCES items(id),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version >= 1),
    idempotency_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (aggregate_id, aggregate_version)
);
CREATE INDEX events_type_id ON events(event_type, id);
CREATE INDEX events_created_id ON events(created_at, id);
CREATE INDEX events_idempotency ON events(idempotency_key);
CREATE TABLE idempotency (
    key VARCHAR(255) PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    response JSONB NOT NULL,
    status INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- Delivery metadata is mutable; business events are not.
CREATE TABLE event_jobs (
    event_id BIGINT PRIMARY KEY REFERENCES events(id),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ,
    dead BOOLEAN NOT NULL DEFAULT false,
    last_error TEXT
);
CREATE INDEX event_jobs_pending ON event_jobs(available_at, event_id) WHERE processed_at IS NULL AND NOT dead;
CREATE TABLE inventory_activity (
    item_id BIGINT PRIMARY KEY REFERENCES items(id),
    total_created BIGINT NOT NULL DEFAULT 0,
    total_updated BIGINT NOT NULL DEFAULT 0,
    total_reserved BIGINT NOT NULL DEFAULT 0,
    total_released BIGINT NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ NOT NULL
);
CREATE FUNCTION reject_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events are immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON events
FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation();
