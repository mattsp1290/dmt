use crate::{crash::CrashAt, graph, ledger::Ledger};
use dmt::{
    BranchResult, EngineBuilder, HandlerError, NodeContext, NodeHandler, NodeInput, NodeOutcome,
    Outcome,
};
use serde_json::{Value, json};
use std::sync::Arc;
pub struct Effects {
    pub ledger: Ledger,
    pub crash: Option<CrashAt>,
}
type Perform = fn(&NodeContext, &NodeInput) -> NodeOutcome;
struct Handler {
    effects: Arc<Effects>,
    perform: Perform,
}
#[dmt::async_trait]
impl NodeHandler for Handler {
    async fn run(&self, ctx: NodeContext, input: NodeInput) -> Result<NodeOutcome, HandlerError> {
        let effects = self.effects.clone();
        let perform = self.perform;
        tokio::task::spawn_blocking(move || step(&effects, &ctx, &input, perform))
            .await
            .map_err(|e| HandlerError::Retryable(e.to_string()))?
    }
}
fn step(
    effects: &Effects,
    ctx: &NodeContext,
    input: &NodeInput,
    perform: Perform,
) -> Result<NodeOutcome, HandlerError> {
    effects
        .ledger
        .record_invocation(ctx)
        .map_err(|e| HandlerError::Retryable(e.to_string()))?;
    let (outcome, performed) = effects
        .ledger
        .perform_once(ctx.step_key.as_str(), || perform(ctx, input))
        .map_err(|e| HandlerError::Retryable(e.to_string()))?;
    if performed
        && effects
            .crash
            .as_ref()
            .is_some_and(|c| c.matches(ctx, input))
    {
        std::process::abort();
    }
    Ok(outcome)
}
fn done(label: &str, payload: Value) -> NodeOutcome {
    NodeOutcome::Done(Outcome::with_payload(label, payload))
}
fn invalid() -> NodeOutcome {
    NodeOutcome::Fail {
        message: "unexpected handler input".into(),
        retryable: false,
    }
}
fn plan(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    if !matches!(input, NodeInput::Task(_)) {
        return invalid();
    }
    done("planned", json!({"plan": "milestone plan draft"}))
}
fn iterate(ctx: &NodeContext, input: &NodeInput) -> NodeOutcome {
    if !matches!(input, NodeInput::Task(_)) {
        return invalid();
    }
    done(
        "iterated",
        json!({"plan": "milestone plan revised", "step_key": ctx.step_key}),
    )
}
fn implement(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    if !matches!(input, NodeInput::Task(_)) {
        return invalid();
    }
    done("implemented", json!({"diff": "3 files changed"}))
}
fn fan_out(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    if !matches!(input, NodeInput::Task(_)) {
        return invalid();
    }
    NodeOutcome::FanOut(
        ["reviewer-a", "reviewer-b", "reviewer-c"]
            .into_iter()
            .map(|v| json!(v))
            .collect(),
    )
}
fn review(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    match input {
        NodeInput::Branch { index, value } => {
            done("reviewed", json!({"reviewer": value, "findings": index}))
        }
        _ => invalid(),
    }
}
fn collect(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    let NodeInput::Join(join) = input else {
        return invalid();
    };
    let total: Option<u64> = join
        .results
        .iter()
        .try_fold(0_u64, |total, result| match result {
            BranchResult::Done { outcome, .. } => {
                total.checked_add(outcome.payload.get("findings")?.as_u64()?)
            }
            BranchResult::Failed { .. } => None,
        });
    match total {
        Some(total) => done(
            if total > 0 {
                graph::NEEDS_FIXES
            } else {
                graph::CLEAN
            },
            json!({"findings": total}),
        ),
        None => invalid(),
    }
}
fn fix(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    match input {
        NodeInput::Task(value) if value.get("findings").and_then(Value::as_u64).is_some() => {
            done("fixed", json!({"fixed": value["findings"]}))
        }
        _ => invalid(),
    }
}
fn open_pr(_: &NodeContext, input: &NodeInput) -> NodeOutcome {
    if !matches!(input, NodeInput::Task(_)) {
        return invalid();
    }
    done("opened", json!({"pr_url": "https://example.invalid/pr/1"}))
}
#[must_use]
pub fn register(mut builder: EngineBuilder, effects: &Arc<Effects>) -> EngineBuilder {
    let handlers: [(&str, Perform); 8] = [
        (graph::PLAN, plan),
        (graph::ITERATE_PLAN, iterate),
        (graph::IMPLEMENT, implement),
        (graph::REVIEW_FANOUT, fan_out),
        (graph::REVIEW, review),
        (graph::COLLECT_FEEDBACK, collect),
        (graph::FIX, fix),
        (graph::OPEN_PR, open_pr),
    ];
    for (id, perform) in handlers {
        builder = builder.handler(
            id,
            Arc::new(Handler {
                effects: effects.clone(),
                perform,
            }),
        );
    }
    builder
}
#[cfg(test)]
mod tests;
