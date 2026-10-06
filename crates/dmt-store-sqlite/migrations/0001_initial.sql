CREATE TABLE dmt_schema_meta (
    key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL
) STRICT;
INSERT INTO dmt_schema_meta VALUES ('schema_version', '1');
CREATE TABLE dmt_graphs (
    id TEXT NOT NULL, version INTEGER NOT NULL, definition_hash TEXT NOT NULL,
    definition_json TEXT NOT NULL, PRIMARY KEY (id, version)
) STRICT;
CREATE TABLE dmt_runs (
    id TEXT PRIMARY KEY NOT NULL, graph_id TEXT NOT NULL, graph_version INTEGER NOT NULL,
    definition_hash TEXT NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
    machine_json TEXT NOT NULL, occurrences_json TEXT NOT NULL, input_json TEXT NOT NULL,
    output_json TEXT, next_seq INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
    FOREIGN KEY (graph_id, graph_version) REFERENCES dmt_graphs (id, version)
) STRICT;
CREATE TABLE dmt_events (
    run_id TEXT NOT NULL REFERENCES dmt_runs (id), seq INTEGER NOT NULL, kind TEXT NOT NULL,
    payload_json TEXT NOT NULL, recorded_at INTEGER NOT NULL, PRIMARY KEY (run_id, seq)
) STRICT;
CREATE TABLE dmt_tasks (
    id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES dmt_runs (id),
    graph_id TEXT NOT NULL, node_id TEXT NOT NULL, step_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL, attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
    run_at INTEGER NOT NULL, lease_owner TEXT, lease_until INTEGER,
    input_json TEXT NOT NULL, outcome_json TEXT, join_id TEXT, branch_index INTEGER,
    planned_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE dmt_joins (
    id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES dmt_runs (id),
    node_id TEXT NOT NULL, step_key TEXT NOT NULL, policy_json TEXT NOT NULL,
    expected INTEGER NOT NULL, received INTEGER NOT NULL, failed INTEGER NOT NULL,
    results_json TEXT NOT NULL, satisfied_at INTEGER
) STRICT;
CREATE TABLE dmt_signals (
    id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES dmt_runs (id),
    task_id TEXT NOT NULL, key TEXT NOT NULL, name TEXT NOT NULL,
    deadline_at INTEGER, resolved_at INTEGER, label TEXT, payload_json TEXT,
    UNIQUE (run_id, key)
) STRICT;
CREATE INDEX dmt_tasks_ready ON dmt_tasks (status, run_at, id);
CREATE INDEX dmt_tasks_run ON dmt_tasks (run_id, status);
CREATE INDEX dmt_tasks_graph ON dmt_tasks (graph_id, status);
CREATE INDEX dmt_runs_status ON dmt_runs (status);
CREATE INDEX dmt_signals_open ON dmt_signals (run_id, name) WHERE resolved_at IS NULL;
CREATE INDEX dmt_signals_due ON dmt_signals (deadline_at, id)
    WHERE resolved_at IS NULL AND deadline_at IS NOT NULL;
