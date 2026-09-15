PRAGMA foreign_keys = ON;

-- ============================================================
-- PIPELINE EXECUTION / OBSERVABILITY
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL
        CHECK (status IN (
            'RUNNING',
            'SUCCESS',
            'FAILED',
            'COMPLETED_WITH_EXCEPTIONS'
        )),
    source_row_count INTEGER DEFAULT 0,
    clean_row_count INTEGER DEFAULT 0,
    quarantined_row_count INTEGER DEFAULT 0,
    deterministic_count INTEGER DEFAULT 0,
    ai_candidate_count INTEGER DEFAULT 0,
    ai_success_count INTEGER DEFAULT 0,
    ai_failure_count INTEGER DEFAULT 0,
    human_review_count INTEGER DEFAULT 0
);


CREATE TABLE IF NOT EXISTS task_run_metrics (
    run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL,
    recorded_at TEXT NOT NULL,

    PRIMARY KEY (
        run_id,
        task_name,
        metric_name
    ),

    FOREIGN KEY (run_id)
        REFERENCES pipeline_run(run_id)
);


-- ============================================================
-- RAW / STAGING
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_raw_tickets (
    ticket_id INTEGER PRIMARY KEY,
    created_date TEXT NOT NULL,
    ticket_text TEXT NOT NULL,

    source_file TEXT NOT NULL,
    source_row_number INTEGER,
    source_hash TEXT NOT NULL,

    loaded_at TEXT NOT NULL
);


-- ============================================================
-- DATA QUALITY
-- ============================================================

CREATE TABLE IF NOT EXISTS ticket_quality_issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,

    run_id TEXT NOT NULL,
    ticket_id INTEGER,

    issue_code TEXT NOT NULL,
    issue_detail TEXT NOT NULL,

    source_file TEXT,
    source_row_number INTEGER,

    detected_at TEXT NOT NULL,

    FOREIGN KEY (run_id)
        REFERENCES pipeline_run(run_id)
);


-- ============================================================
-- CLEAN / VALIDATED DATA
-- ============================================================

CREATE TABLE IF NOT EXISTS ticket_clean (
    ticket_id INTEGER PRIMARY KEY,

    created_date TEXT NOT NULL,
    ticket_text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,

    source_hash TEXT NOT NULL,
    cleaned_at TEXT NOT NULL
);


-- ============================================================
-- DETERMINISTIC CLASSIFICATION
-- ============================================================

CREATE TABLE IF NOT EXISTS deterministic_classification (
    ticket_id INTEGER PRIMARY KEY,

    category TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    evidence TEXT NOT NULL,

    classified_at TEXT NOT NULL,

    FOREIGN KEY (ticket_id)
        REFERENCES ticket_clean(ticket_id)
);


-- ============================================================
-- AI CANDIDATE ROUTING
-- ============================================================

CREATE TABLE IF NOT EXISTS ai_candidate (
    ticket_id INTEGER PRIMARY KEY,

    evidence_text TEXT NOT NULL,
    candidate_reason TEXT NOT NULL,
    input_hash TEXT NOT NULL,

    created_at TEXT NOT NULL,

    FOREIGN KEY (ticket_id)
        REFERENCES ticket_clean(ticket_id)
);


-- ============================================================
-- AI ENRICHMENT RESULTS
-- ============================================================

CREATE TABLE IF NOT EXISTS ai_classification (
    ai_result_id INTEGER PRIMARY KEY AUTOINCREMENT,

    ticket_id INTEGER NOT NULL,

    evidence_text TEXT NOT NULL,
    input_hash TEXT NOT NULL,

    prompt_version TEXT NOT NULL,
    model TEXT NOT NULL,

    category TEXT,
    sentiment TEXT,
    summary TEXT,

    confidence REAL
        CHECK (
            confidence IS NULL
            OR confidence BETWEEN 0.0 AND 1.0
        ),

    ai_used INTEGER NOT NULL DEFAULT 1
        CHECK (ai_used IN (0, 1)),

    result_status TEXT NOT NULL
        CHECK (result_status IN (
            'SUCCESS',
            'FAILED',
            'INVALID_OUTPUT'
        )),

    error_type TEXT,
    error_message TEXT,

    processed_at TEXT NOT NULL,

    UNIQUE (
        ticket_id,
        input_hash,
        prompt_version,
        model
    ),

    FOREIGN KEY (ticket_id)
        REFERENCES ticket_clean(ticket_id)
);


-- ============================================================
-- HUMAN REVIEW
-- ============================================================

CREATE TABLE IF NOT EXISTS human_review_queue (
    ticket_id INTEGER PRIMARY KEY,

    review_reason TEXT NOT NULL,
    proposed_category TEXT,
    confidence REAL,

    review_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (review_status IN (
            'PENDING',
            'REVIEWED',
            'RESOLVED'
        )),

    created_at TEXT NOT NULL,

    FOREIGN KEY (ticket_id)
        REFERENCES ticket_clean(ticket_id)
);


-- ============================================================
-- PIPELINE / AI ERRORS
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_errors (
    error_id INTEGER PRIMARY KEY AUTOINCREMENT,

    run_id TEXT,
    task_name TEXT NOT NULL,

    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,

    record_id TEXT,

    occurred_at TEXT NOT NULL,

    FOREIGN KEY (run_id)
        REFERENCES pipeline_run(run_id)
);


-- ============================================================
-- FINAL BUSINESS OUTPUT
-- ============================================================

CREATE TABLE IF NOT EXISTS final_ticket_classification (
    ticket_id INTEGER PRIMARY KEY,

    final_category TEXT,
    sentiment TEXT,
    summary TEXT,

    classification_source TEXT NOT NULL
        CHECK (classification_source IN (
            'DETERMINISTIC',
            'AI',
            'FALLBACK',
            'HUMAN_REVIEW'
        )),

    ai_used INTEGER NOT NULL
        CHECK (ai_used IN (0, 1)),

    final_handling TEXT NOT NULL
        CHECK (final_handling IN (
            'DETERMINISTIC',
            'AI_ACCEPTED',
            'AI_FAILURE_FALLBACK',
            'HUMAN_REVIEW'
        )),

    updated_at TEXT NOT NULL,

    FOREIGN KEY (ticket_id)
        REFERENCES ticket_clean(ticket_id)
);


-- ============================================================
-- ANALYTICAL SUMMARY
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_ticket_summary (
    summary_date TEXT NOT NULL,
    category TEXT NOT NULL,

    ticket_count INTEGER NOT NULL,
    deterministic_count INTEGER NOT NULL DEFAULT 0,
    ai_enriched_count INTEGER NOT NULL DEFAULT 0,
    human_review_count INTEGER NOT NULL DEFAULT 0,
    ai_failure_fallback_count INTEGER NOT NULL DEFAULT 0,

    refreshed_at TEXT NOT NULL,

    PRIMARY KEY (
        summary_date,
        category
    )
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_ticket_clean_created_date
    ON ticket_clean(created_date);

CREATE INDEX IF NOT EXISTS idx_ai_classification_ticket
    ON ai_classification(ticket_id);

CREATE INDEX IF NOT EXISTS idx_ai_classification_status
    ON ai_classification(result_status);

CREATE INDEX IF NOT EXISTS idx_quality_issue_ticket
    ON ticket_quality_issues(ticket_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_errors_run
    ON pipeline_errors(run_id);

CREATE INDEX IF NOT EXISTS idx_final_category
    ON final_ticket_classification(final_category);

