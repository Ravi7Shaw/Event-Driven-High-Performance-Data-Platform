-- Keep cumulative committed attempts when retry_dead resets the retry budget.
ALTER TABLE event_jobs ADD COLUMN total_attempts BIGINT NOT NULL DEFAULT 0
    CHECK (total_attempts >= 0);
-- History erased by requeues before this migration cannot be reconstructed.
UPDATE event_jobs SET total_attempts = attempts;
