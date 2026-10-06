use dmt_core::{InvalidTaskTransition, TaskEvent::*, TaskStatus::*, TransitionOwner};
#[test]
fn exhaustive_sixty_pairs() {
    let statuses = [Ready, Running, Awaiting, Completed, Exhausted, Cancelled];
    let events = [
        Claim,
        Reclaim,
        LeaseExhausted,
        RunTerminal,
        Done,
        FailRetry,
        FailFinal,
        Signal,
        Timeout,
        Cancel,
    ];
    let expected = [
        [
            Some(Running),
            None,
            None,
            Some(Cancelled),
            None,
            None,
            None,
            None,
            None,
            Some(Cancelled),
        ],
        [
            None,
            Some(Running),
            Some(Exhausted),
            Some(Cancelled),
            Some(Completed),
            Some(Ready),
            Some(Exhausted),
            None,
            None,
            None,
        ],
        [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Completed),
            Some(Completed),
            Some(Cancelled),
        ],
        [None; 10],
        [None; 10],
        [None; 10],
    ];
    for (status, row) in statuses.into_iter().zip(expected) {
        for (event, target) in events.into_iter().zip(row) {
            assert_eq!(
                status.next(event),
                target.ok_or(InvalidTaskTransition {
                    from: status,
                    event
                }),
                "{status:?} {event:?}"
            );
        }
    }
}
#[test]
fn owner_split() {
    for event in [Claim, Reclaim, LeaseExhausted, RunTerminal] {
        assert_eq!(event.owner(), TransitionOwner::Store);
    }
    for event in [Done, FailRetry, FailFinal, Signal, Timeout, Cancel] {
        assert_eq!(event.owner(), TransitionOwner::Planner);
    }
}
