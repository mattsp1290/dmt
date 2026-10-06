mod lifecycle_round_trip {
    use dmt_core::{
        Effects, LifecycleError, LifecycleEvent, RejectedTransition, RunEvent, RunStatus,
        new_machine, restore,
    };
    use serde_json::json;
    fn start() -> LifecycleEvent {
        LifecycleEvent::Start {
            graph_id: "graph".into(),
            graph_version: 1,
            definition_hash: "hash".into(),
            input: json!(null),
        }
    }
    #[test]
    fn parked_round_trip_and_next_event() {
        let mut effects = Effects::default();
        let mut machine = new_machine(&mut effects);
        machine.handle_with_context(&start(), &mut effects);
        machine.handle_with_context(&LifecycleEvent::Park, &mut effects);
        assert_eq!(RunStatus::from(machine.state()), RunStatus::Parked);
        assert_eq!(
            effects.emitted,
            vec![
                RunEvent::RunStarted {
                    graph_id: "graph".into(),
                    graph_version: 1,
                    definition_hash: "hash".into(),
                    input: json!(null)
                },
                RunEvent::RunParked
            ]
        );
        let json = serde_json::to_value(&machine).unwrap();
        assert_eq!(json["state"], json!({"Parked": {}}));
        assert!(json["shared_storage"].is_null());
        let mut fresh = Effects::default();
        let mut restored = restore(RunStatus::Parked, &json, &mut fresh).unwrap();
        assert_eq!(restored.state(), machine.state());
        assert_eq!(fresh.emitted, [] as [RunEvent; 0]);
        assert!(fresh.rejected.is_none());
        restored.handle_with_context(&LifecycleEvent::Resume, &mut fresh);
        assert_eq!(RunStatus::from(restored.state()), RunStatus::Active);
        assert_eq!(fresh.emitted, vec![RunEvent::RunResumed]);
        restored.handle_with_context(
            &LifecycleEvent::Cancel {
                reason: "stop".into(),
            },
            &mut fresh,
        );
        assert_eq!(RunStatus::from(restored.state()), RunStatus::Cancelled);
        assert_eq!(
            fresh.emitted.last(),
            Some(&RunEvent::RunCancelled {
                reason: "stop".into()
            })
        );
        let count = fresh.emitted.len();
        restored.handle_with_context(&start(), &mut fresh);
        assert_eq!(
            fresh.rejected,
            Some(RejectedTransition {
                status: RunStatus::Cancelled,
                event: "Start"
            })
        );
        assert_eq!(fresh.emitted.len(), count);
        assert!(matches!(
            restore(RunStatus::Active, &json, &mut Effects::default()),
            Err(LifecycleError::CorruptRun {
                stored: RunStatus::Active,
                machine: RunStatus::Parked
            })
        ));
        assert!(matches!(
            restore(
                RunStatus::Parked,
                &json!({"state": "garbage"}),
                &mut Effects::default()
            ),
            Err(LifecycleError::Deserialize(_))
        ));
    }
    #[test]
    fn restore_emits_nothing_from_every_status() {
        for status in [
            RunStatus::Created,
            RunStatus::Active,
            RunStatus::Parked,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            let mut effects = Effects::default();
            let mut machine = new_machine(&mut effects);
            if status != RunStatus::Created {
                machine.handle_with_context(&start(), &mut effects);
            }
            let event = match status {
                RunStatus::Parked => Some(LifecycleEvent::Park),
                RunStatus::Completed => Some(LifecycleEvent::Complete { output: json!(42) }),
                RunStatus::Failed => Some(LifecycleEvent::Fail {
                    message: "error".into(),
                }),
                RunStatus::Cancelled => Some(LifecycleEvent::Cancel {
                    reason: "stop".into(),
                }),
                _ => None,
            };
            if let Some(event) = event {
                machine.handle_with_context(&event, &mut effects);
            }
            let mut fresh = Effects::default();
            let restored =
                restore(status, &serde_json::to_value(&machine).unwrap(), &mut fresh).unwrap();
            assert_eq!(machine.state(), restored.state());
            assert_eq!(fresh.emitted, [] as [RunEvent; 0]);
            assert!(fresh.rejected.is_none());
        }
    }
    #[test]
    fn lifecycle_source_has_no_actions() {
        let source = include_str!("../src/lifecycle.rs");
        for needle in [
            concat!("#[", "action]"),
            concat!("entry_", "action"),
            concat!("exit_", "action"),
        ] {
            assert!(!source.contains(needle));
        }
    }
}
mod entry_action_probe {
    use serde::{Deserialize, Serialize};
    use statig::blocking::{IntoStateMachineExt, Outcome, UninitializedStateMachine};
    use statig::state_machine;
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Probe;
    struct ProbeEvent;
    #[derive(Default)]
    struct ProbeEffects {
        entered: u32,
    }
    #[state_machine(
        initial = "State::armed()",
        state(derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize))
    )]
    impl Probe {
        #[state(entry_action = "enter_armed")]
        fn armed(context: &mut ProbeEffects, event: &ProbeEvent) -> Outcome<State> {
            let _ = (context, event);
            Outcome::Handled
        }
        #[action]
        fn enter_armed(context: &mut ProbeEffects) {
            context.entered += 1;
        }
    }
    #[test]
    fn entry_actions_repeat_on_restore() {
        let mut effects = ProbeEffects::default();
        let machine = Probe
            .uninitialized_state_machine()
            .init_with_context(&mut effects);
        assert_eq!(effects.entered, 1);
        let restored: UninitializedStateMachine<Probe> =
            serde_json::from_value(serde_json::to_value(&machine).unwrap()).unwrap();
        let mut fresh = ProbeEffects::default();
        let _ = restored.init_with_context(&mut fresh);
        assert_eq!(
            fresh.entered, 1,
            "entry actions re-run on restore; RunLifecycle must stay action-free"
        );
    }
}
