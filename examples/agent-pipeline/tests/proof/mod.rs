use agent_pipeline::ledger::{read_entries, read_invocations};
use dmt::{EventRecord, NodeOutcome, RunEvent};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
const SUFFIXES: [&str; 11] = [
    "plan/0",
    "iterate-plan/0",
    "iterate-plan/1",
    "implement/0",
    "review-fanout/0",
    "review/0/0",
    "review/0/1",
    "review/0/2",
    "collect-feedback/0",
    "fix/0",
    "open-pr/0",
];
pub fn assert_ledger(path: &Path, run: &str) {
    let entries = read_entries(path).unwrap();
    let keys: BTreeSet<_> = entries.iter().map(|e| e.step_key.clone()).collect();
    assert_eq!(entries.len(), 11);
    assert_eq!(
        keys,
        SUFFIXES.map(|s| format!("{run}/{s}")).into_iter().collect()
    );
    let fan_out = &entries
        .iter()
        .find(|e| e.step_key == format!("{run}/review-fanout/0"))
        .unwrap()
        .outcome;
    assert!(matches!(fan_out, NodeOutcome::FanOut(values) if values.len() == 3));
    let collect = &entries
        .iter()
        .find(|e| e.step_key == format!("{run}/collect-feedback/0"))
        .unwrap()
        .outcome;
    assert!(matches!(collect, NodeOutcome::Done(outcome) if outcome.label == "needs_fixes"));
}
pub fn assert_invocations(path: &Path, run: &str) {
    let invocations = read_invocations(path).unwrap();
    assert_eq!(
        invocations
            .iter()
            .map(|i| i.step_key.clone())
            .collect::<BTreeSet<_>>(),
        SUFFIXES.map(|s| format!("{run}/{s}")).into_iter().collect()
    );
    for suffix in SUFFIXES {
        let records: Vec<_> = invocations
            .iter()
            .filter(|i| i.step_key == format!("{run}/{suffix}"))
            .collect();
        let attempts: BTreeSet<_> = records.iter().map(|i| i.attempt).collect();
        assert!(attempts.contains(&1), "{suffix}: {attempts:?}");
        if ["review/0/1", "open-pr/0"].contains(&suffix) {
            assert!(attempts.contains(&2));
            assert!(records.len() >= 2);
        }
        let node = suffix.split('/').next().unwrap();
        assert!(records.iter().all(|i| i.node_id == node));
    }
}
pub fn assert_events(events: &[EventRecord], run: &str) {
    assert_eq!(
        events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert!(matches!(
        events.last().unwrap().event,
        RunEvent::RunCompleted { .. }
    ));
    for (kind, count) in [
        ("TaskCompleted", 14),
        ("RunParked", 3),
        ("RunResumed", 3),
        ("SignalReceived", 3),
        ("JoinSatisfied", 1),
        ("RunStarted", 1),
        ("RunCompleted", 1),
    ] {
        assert_eq!(
            events.iter().filter(|e| e.event.kind() == kind).count(),
            count,
            "{kind}"
        );
    }
    let mut last_claims = BTreeMap::new();
    let mut attempts: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    let mut completions = BTreeMap::new();
    let mut completed_seq = None;
    for record in events {
        match &record.event {
            RunEvent::TaskClaimed {
                task_id, attempt, ..
            } => {
                assert!(completed_seq.is_none(), "claim after RunCompleted");
                last_claims.insert(task_id.to_string(), *attempt);
                attempts
                    .entry(task_id.to_string())
                    .or_default()
                    .insert(*attempt);
            }
            RunEvent::TaskCompleted {
                task_id, attempt, ..
            } => {
                assert!(completions.insert(task_id.to_string(), *attempt).is_none());
            }
            RunEvent::RunCompleted { .. } => completed_seq = Some(record.seq),
            RunEvent::RunStarted {
                graph_id,
                graph_version,
                ..
            } => {
                assert_eq!(graph_id.as_str(), "agent-pipeline");
                assert_eq!(*graph_version, 1);
            }
            _ => {}
        }
    }
    let expected: BTreeSet<_> = SUFFIXES
        .into_iter()
        .chain(["plan-signoff/0", "plan-signoff/1", "present/0"])
        .map(|s| format!("{run}/{s}"))
        .collect();
    assert_eq!(
        completions.keys().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    for (task, attempt) in last_claims {
        assert_eq!(completions[&task], attempt, "{task}");
    }
    for suffix in ["review/0/1", "open-pr/0"] {
        assert!(attempts[&format!("{run}/{suffix}")].is_superset(&BTreeSet::from([1, 2])));
    }
    let labels: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            RunEvent::SignalReceived { label, .. } => Some(label.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(labels, ["changes_requested", "approved", "acknowledged"]);
    let keys: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            RunEvent::WaitOpened { signal_key, .. } => Some(signal_key.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(keys, ["signoff/0", "signoff/1", "ack/0"]);
}
