-- Applied once by the Postgres image; loader also checks the contract on reruns.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.incoming_tickets (
    ticket_id INTEGER PRIMARY KEY,
    created_date DATE NOT NULL,
    ticket_text TEXT NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    source_batch TEXT NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw.ticket_outcomes (
    ticket_id INTEGER PRIMARY KEY REFERENCES raw.incoming_tickets(ticket_id),
    final_category TEXT,
    sentiment TEXT,
    summary TEXT,
    classification_source TEXT NOT NULL CHECK (classification_source IN ('DETERMINISTIC', 'AI', 'HUMAN_REVIEW')),
    ai_used BOOLEAN NOT NULL,
    final_handling TEXT NOT NULL CHECK (final_handling IN ('DETERMINISTIC', 'AI_ACCEPTED', 'HUMAN_REVIEW')),
    content_sha256 CHAR(64) NOT NULL,
    source_batch TEXT NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ticket_created_date ON raw.incoming_tickets(created_date);
CREATE INDEX IF NOT EXISTS idx_ticket_outcome_handling ON raw.ticket_outcomes(final_handling);
