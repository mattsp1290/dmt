pub(crate) const SELECT_SCHEMA_VERSION: &str =
    "SELECT value FROM dmt_schema_meta WHERE key = 'schema_version'";
pub(crate) const PRAGMA_JOURNAL_MODE: &str = "PRAGMA journal_mode";

pub(crate) const BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";
pub(crate) const SELECT_GRAPH_HASH: &str =
    "SELECT definition_hash FROM dmt_graphs WHERE id=?1 AND version=?2";
pub(crate) const INSERT_GRAPH: &str =
    "INSERT INTO dmt_graphs (id,version,definition_hash,definition_json) VALUES (?1,?2,?3,?4)";
pub(crate) const SELECT_GRAPH_JSON: &str =
    "SELECT definition_json FROM dmt_graphs WHERE id=?1 AND version=?2";
pub(crate) const SELECT_RUN_HEAD: &str =
    "SELECT status,version,next_seq,graph_id FROM dmt_runs WHERE id=?1";
pub(crate) const INSERT_RUN: &str = "INSERT INTO dmt_runs (id,graph_id,graph_version,definition_hash,status,version,machine_json,occurrences_json,input_json,created_at,updated_at) VALUES (?1,?2,?3,?4,'created',0,'null','{}',?5,?6,?6)";
pub(crate) const UPDATE_RUN: &str = "UPDATE dmt_runs SET version=?2,next_seq=?3,occurrences_json=?4,updated_at=?5 WHERE id=?1 AND version=?6";
pub(crate) const UPDATE_RUN_WITH_STATE: &str = "UPDATE dmt_runs SET version=?2,next_seq=?3,occurrences_json=?4,updated_at=?5,status=?7,machine_json=?8,output_json=?9 WHERE id=?1 AND version=?6";
pub(crate) const SELECT_RUN: &str = "SELECT * FROM dmt_runs WHERE id=?1";
pub(crate) const LIST_RUNS: &str = "SELECT * FROM dmt_runs WHERE (?1 IS NULL OR status=?1) AND (?2 IS NULL OR graph_id=?2) ORDER BY created_at,id LIMIT ?3";
pub(crate) const ALLOC_SEQ: &str =
    "UPDATE dmt_runs SET next_seq=next_seq+1 WHERE id=?1 RETURNING next_seq-1";
pub(crate) const SELECT_TASK_LEASE: &str =
    "SELECT run_id,status,lease_owner,attempt FROM dmt_tasks WHERE id=?1";
pub(crate) const SELECT_TASK_RUN: &str = "SELECT run_id FROM dmt_tasks WHERE id=?1";
pub(crate) const UPDATE_TASK: &str = "UPDATE dmt_tasks SET status=?2,attempt=?3,run_at=?4,outcome_json=?5,planned_at=?6,lease_owner=CASE WHEN ?2='running' THEN lease_owner ELSE NULL END,lease_until=CASE WHEN ?2='running' THEN lease_until ELSE NULL END,updated_at=?7 WHERE id=?1 AND (?2<>'cancelled' OR status IN ('ready','awaiting'))";
pub(crate) const INSERT_TASK: &str = "INSERT INTO dmt_tasks (id,run_id,graph_id,node_id,step_key,status,attempt,max_attempts,run_at,input_json,join_id,branch_index,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13) ON CONFLICT DO NOTHING";
pub(crate) const SELECT_TASK: &str = "SELECT * FROM dmt_tasks WHERE id=?1";
pub(crate) const SELECT_RUN_TASKS: &str = "SELECT * FROM dmt_tasks WHERE run_id=?1 AND status IN ('ready','running','awaiting','exhausted') ORDER BY id";
pub(crate) const EXHAUSTED_TASKS: &str = "SELECT t.* FROM dmt_tasks t JOIN dmt_runs r ON r.id=t.run_id WHERE t.status='exhausted' AND t.planned_at IS NULL AND r.status NOT IN ('completed','failed','cancelled') ORDER BY t.run_at,t.id LIMIT ?1";
macro_rules! claim_predicate {
    () => { "t.graph_id IN (SELECT value FROM json_each(?1)) AND ((t.status='ready' AND t.run_at<=?2) OR (t.status='running' AND t.lease_until<?2))" };
}
macro_rules! claim_probe {
    () => {
        concat!(
            "SELECT 1 FROM dmt_tasks t WHERE ",
            claim_predicate!(),
            " LIMIT 1"
        )
    };
}
macro_rules! claim_page {
    () => { concat!("SELECT t.*,r.status AS run_status,r.graph_version FROM dmt_tasks t JOIN dmt_runs r ON r.id=t.run_id WHERE ",claim_predicate!()," AND (?3=1 OR (t.run_at,t.id)>(?4,?5)) ORDER BY t.run_at,t.id LIMIT ?6") };
}
pub(crate) const CLAIM_PROBE: &str = claim_probe!();
pub(crate) const CLAIM_PAGE: &str = claim_page!();
#[cfg(test)]
pub(crate) const EXPLAIN_PROBE: &str = concat!("EXPLAIN QUERY PLAN ", claim_probe!());
#[cfg(test)]
pub(crate) const EXPLAIN_PAGE: &str = concat!("EXPLAIN QUERY PLAN ", claim_page!());
pub(crate) const TASK_CLEAR_LEASE: &str =
    "UPDATE dmt_tasks SET status=?2,lease_owner=NULL,lease_until=NULL,updated_at=?3 WHERE id=?1";
pub(crate) const TASK_CLAIM: &str = "UPDATE dmt_tasks SET status=?2,attempt=?3,lease_owner=?4,lease_until=?5,updated_at=?6 WHERE id=?1";
pub(crate) const HEARTBEAT: &str = "UPDATE dmt_tasks SET lease_until=?2,updated_at=?3 WHERE id=?1 AND status='running' AND lease_owner=?4 AND attempt=?5";
pub(crate) const TASK_EXISTS: &str = "SELECT 1 FROM dmt_tasks WHERE id=?1";
pub(crate) const SELECT_JOIN_GUARD: &str = "SELECT * FROM dmt_joins WHERE id=?1";
pub(crate) const JOIN_EXISTS: &str = "SELECT 1 FROM dmt_joins WHERE id=?1";
pub(crate) const INSERT_JOIN: &str = "INSERT INTO dmt_joins (id,run_id,node_id,step_key,policy_json,expected,received,failed,results_json,satisfied_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)";
pub(crate) const UPDATE_JOIN_CONTRIBUTION: &str = "UPDATE dmt_joins SET received=?2,failed=?3,results_json=?4,satisfied_at=?5 WHERE id=?1 AND satisfied_at IS NULL";
pub(crate) const SELECT_RUN_JOINS: &str = "SELECT * FROM dmt_joins WHERE run_id=?1 ORDER BY id";
pub(crate) const SELECT_SIGNAL_STATE: &str =
    "SELECT run_id,resolved_at FROM dmt_signals WHERE id=?1";
pub(crate) const SIGNAL_CONFLICT: &str =
    "SELECT 1 FROM dmt_signals WHERE id=?1 OR (run_id=?2 AND key=?3) LIMIT 1";
pub(crate) const INSERT_SIGNAL: &str =
    "INSERT INTO dmt_signals (id,run_id,task_id,key,name,deadline_at) VALUES (?1,?2,?3,?4,?5,?6)";
pub(crate) const RESOLVE_SIGNAL: &str = "UPDATE dmt_signals SET resolved_at=?2,label=?3,payload_json=?4 WHERE id=?1 AND resolved_at IS NULL";
pub(crate) const SELECT_OPEN_SIGNALS: &str =
    "SELECT * FROM dmt_signals WHERE run_id=?1 AND resolved_at IS NULL ORDER BY id";
pub(crate) const FIND_OPEN_SIGNAL: &str = "SELECT * FROM dmt_signals WHERE run_id=?1 AND name=?2 AND resolved_at IS NULL ORDER BY id LIMIT 1";
pub(crate) const DUE_SIGNALS: &str = "SELECT s.* FROM dmt_signals s JOIN dmt_runs r ON r.id=s.run_id WHERE s.resolved_at IS NULL AND s.deadline_at IS NOT NULL AND s.deadline_at<=?1 AND r.status NOT IN ('completed','failed','cancelled') ORDER BY s.deadline_at,s.id LIMIT ?2";
pub(crate) const INSERT_EVENT: &str =
    "INSERT INTO dmt_events (run_id,seq,kind,payload_json,recorded_at) VALUES (?1,?2,?3,?4,?5)";
pub(crate) const SELECT_EVENTS: &str = "SELECT seq,recorded_at,payload_json FROM dmt_events WHERE run_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3";
