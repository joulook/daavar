use std::collections::HashSet;

use prusti_contracts::*;
use prusti_refinement::prelude::*;
use prusti_refinement::abs_types::{AbsSet, AbsMap};
use prusti_refinement::state_handle::{gl_initial_state, gl_old_state};
use prusti_refinement::make_map;
use prusti_refinement::thread_spawn;

struct ConcreteSet {
    set: HashSet<u32>,
}

impl ConcreteSet {
    #[trusted]
    #[ensures(forall(|value: u32| !result.contains(value)))]
    fn new() -> Self {
        ConcreteSet { set: HashSet::new() }
    }

    #[trusted]
    #[pure]
    fn contains(&self, value: u32) -> bool {
        self.set.contains(&value)
    }

    #[trusted]
    #[ensures(forall(|v: u32| self.contains(v) == (old(self.contains(v)) || v === value)))]
    #[ensures(result == old(!self.contains(value)))]
    fn insert(&mut self, value: u32) -> bool {
        self.set.insert(value)
    }
}

type Value = u8;
type Acceptor = u32;
type Leader = u32;
type Ballot = (u32, Leader);

#[trusted]
#[pure]
fn quorum() -> AbsSet<AbsSet<Acceptor>> {
  unimplemented!()
}

#[derive(Clone, Copy)]
enum Message {
    Msg1a(Ballot),
    Msg1b(Acceptor, Ballot, Option<(Ballot, Value)>),
    Msg2a(Ballot, Value),
    Msg2b(Acceptor, Ballot, Value),
}

#[derive(Clone, Copy)]
struct AbsState {
    msgs: AbsSet<Message>,
    max_bal: AbsMap<Acceptor, Option<Ballot>>,
    max_vote: AbsMap<Acceptor, Option<(Ballot, Value)>>,
}

#[allow(unused)]
#[derive(Clone, Copy)]
enum Action {
    Phase1a(Ballot),
    Phase1b(Acceptor, Ballot),
    Phase2a(Ballot, Value, AbsSet<Acceptor>, Option<(Acceptor, Ballot)>),
    Phase2b(Acceptor, Ballot, Value),
}

#[derive(Clone, Copy)]
enum GuardKind {
    Acceptor(Acceptor),
    Leader(Leader),
}

#[pure]
fn ballot_greater(b1: Ballot, b2: Ballot) -> bool {
    b1.0 > b2.0 || (b1.0 == b2.0 && b1.1 > b2.1)
}

#[pure]
fn ballot_greater_equal(b1: Ballot, b2: Ballot) -> bool {
    b1.0 > b2.0 || (b1.0 == b2.0 && b1.1 >= b2.1)
}

#[derive(Clone, Copy)]
#[allow(unused)]
/// This is the part of the AbsState which models the environment and which can
/// only be changed by calling the functions that interact with the environment.
struct EnvState {
    msgs: AbsSet<Message>,
}

struct PaxosModel;
type PxStateHandle = StateHandle<PaxosModel>;
type PxGuard = Guard<PaxosModel>;

#[refine_trait_spec]
impl Model for PaxosModel {
    type AbsState = AbsState;
    type Action = Action;
    type GuardKind = GuardKind;
    type EnvState = EnvState;

    predicate! { fn init(s: AbsState) -> bool {
        s.msgs === AbsSet::empty() &&
        s.max_bal === AbsMap::new(None) &&
        s.max_vote === AbsMap::new(None)
    } }

    predicate! { fn next(s: AbsState, n: AbsState, a: Action) -> bool {
        match a {
            Action::Phase1a(b) =>
                n === AbsState { msgs: s.msgs.add(Message::Msg1a(b)), ..s },
            Action::Phase1b(a, b) =>
                s.msgs.contains(Message::Msg1a(b)) &&
                match s.max_bal.get(a) {
                    Some(mb) => ballot_greater(b, mb),
                    None => true,
                } &&
                n === AbsState {
                    msgs: s.msgs.add(Message::Msg1b(a, b, s.max_vote.get(a))),
                    max_bal: s.max_bal.set(a, Some(b)),
                    ..s
                },
            Action::Phase2a(b, v, q, max_b) =>
                !(exists(|v2: Value| s.msgs.contains(Message::Msg2a(b, v2)))) &&
                quorum().contains(q) &&
                match max_b {
                    None => forall(|a: Acceptor| q.contains(a) ==> s.msgs.contains(Message::Msg1b(a, b, None))),
                    Some((a, mb)) =>
                        q.contains(a) &&
                        s.msgs.contains(Message::Msg1b(a, b, Some((mb, v)))) &&
                        forall(|a2: Acceptor| q.contains(a2) ==>
                            exists(|vote: Option<(Ballot, Value)>|
                                s.msgs.contains(Message::Msg1b(a2, b, vote)) &&
                                match vote {
                                    Some((mb2, _)) => ballot_greater_equal(mb, mb2),
                                    None => true
                                }
                            )
                        )
                } &&
                n === AbsState { msgs: s.msgs.add(Message::Msg2a(b, v)), ..s },
            Action::Phase2b(a, b, v) =>
                s.msgs.contains(Message::Msg2a(b, v)) &&
                match s.max_bal.get(a) {
                    Some(mb) => ballot_greater_equal(b, mb),
                    None => true,
                } &&
                n === AbsState {
                    msgs: s.msgs.add(Message::Msg2b(a, b, v)),
                    max_bal: s.max_bal.set(a, Some(b)),
                    max_vote: s.max_vote.set(a, Some((b, v))),
                    ..s
                },
        }
    } }

    predicate! { fn action_requires_guard(a: Action, g: GuardKind) -> bool {
        match a {
            Action::Phase1a((_, l)) => g === GuardKind::Leader(l),
            Action::Phase1b(a, _) => g === GuardKind::Acceptor(a),
            Action::Phase2a((_, l), _, _, _) => g === GuardKind::Leader(l),
            Action::Phase2b(a, _, _) => g === GuardKind::Acceptor(a),
        }
    } }

    #[pure]
    fn get_env_state(s: AbsState) -> EnvState {
        EnvState {
            msgs: s.msgs,
        }
    }
}

#[trusted]
#[requires(g.locked())]
#[requires(!g.did_act())]
#[requires(g.env_state() === PaxosModel::get_env_state(g.state))]
#[ensures(g.state.msgs.contains(result))]
fn receive(g: &PxStateHandle) -> Message {
    #![allow(unused)]
    unimplemented!()
}

#[trusted]
#[requires(g.locked())]
#[requires(g.env_state() === PaxosModel::get_env_state(g.state))]
#[ensures(g.locked())]
#[ensures(g.env_state() === PaxosModel::get_env_state(g.state))]
#[ensures(g.id() === old(g.id()))]
#[ensures(g.state === AbsState { msgs: old(g.state.msgs).add(msg), ..old(g.state) })]
fn send(msg: Message, g: &mut PxStateHandle) {
    #![allow(unused)]
    unimplemented!()
}

// #[trusted] // save verification time
#[requires(g.locked())]
#[requires(!g.did_act())]
#[requires(gl_old_state::<PaxosModel>(old_state))]
#[ensures(old(old_state.msgs.is_subset(gl_initial_state::<PaxosModel>(g.id()).msgs)))]
fn messages_still_there(old_state: AbsState, g: &PxStateHandle) {
    #![allow(unused)]
    let predicate = make_map!(|s: AbsState| (old_state.msgs.is_subset(s.msgs)));
    g.invariant_preserved(old_state, predicate);
}

// #[trusted] // save verification time
#[requires(!g.locked())]
#[requires(guard.invariant_any(GuardKind::Leader(my_id)))]
#[requires(guard.last_state().msgs === AbsSet::empty())]
#[ensures(!g.locked())]
fn run_leader(my_id: Leader, g: &mut PxStateHandle, guard: &mut PxGuard) {
    let mut ballot_counter = 0;
    loop {
        body_invariant!(!g.locked());
        body_invariant!(guard.invariant_any(GuardKind::Leader(my_id)));
        body_invariant!(forall(|c: u32, v: Value|
            c >= ballot_counter ==>
            !guard.last_state().msgs.contains(Message::Msg2a((c, my_id), v))
        ));

        g.acquire();

        // Work around prusti limitation. ("Inhale might fail. There might be insufficient permission to access _5.val_int")
        let ballot_counter2 = ballot_counter;

        guard.open(g, make_map!(|s: AbsState| (forall(|c: u32, v: Value|
            c >= ballot_counter2 ==>
            !s.msgs.contains(Message::Msg2a((c, my_id), v))
        ))));

        let b = (ballot_counter, my_id);
        send(Message::Msg1a(b), g);

        g.release(Action::Phase1a(b));

        run_leader_ballot(my_id, ballot_counter, g, guard);

        if ballot_counter == u32::MAX {
            break;
        }
        ballot_counter += 1;
    }
}

#[trusted]
/// Make a decision, which is not relevant for correctness
fn decide() -> bool {
    unimplemented!()
}

#[trusted]
fn choose_value() -> Value {
    unimplemented!()
}

// #[trusted] // save verification time
#[requires(!g.locked())]
#[requires(guard.invariant_any(GuardKind::Leader(my_id)))]
#[requires(forall(|c: u32, v: Value|
    c >= counter ==>
    !guard.last_state().msgs.contains(Message::Msg2a((c, my_id), v))
))]
#[ensures(!g.locked())]
#[ensures(guard.invariant_any(GuardKind::Leader(my_id)))]
#[ensures(forall(|c: u32, v: Value|
    c > counter ==>
    !guard.last_state().msgs.contains(Message::Msg2a((c, my_id), v))
))]
fn run_leader_ballot(my_id: Leader, counter: u32, g: &mut PxStateHandle, guard: &mut PxGuard) {
    let acceptor_count = 5;
    let majority = acceptor_count / 2 + 1;
    prusti_assume!(forall(|q: AbsSet<Acceptor>| q.len() >= majority ==> quorum().contains(q)));

    let b = (counter, my_id);
    let mut q_concrete = ConcreteSet::new();
    let mut q = AbsSet::empty();
    let mut vote_count = 0;
    let mut max_vote: Option<(Acceptor, Ballot, Value)> = None;
    g.acquire();
    let mut msgs_state = g.state;
    g.release_stutter();

    while vote_count < majority {
        body_invariant!(!g.locked());
        body_invariant!(gl_old_state::<PaxosModel>(msgs_state));
        body_invariant!(q.len() == vote_count);
        body_invariant!(forall(|a: Acceptor| q.contains(a) == q_concrete.contains(a)));
        body_invariant!(match max_vote {
            Some((a, mb, v)) =>
                q.contains(a) &&
                msgs_state.msgs.contains(Message::Msg1b(a, b, Some((mb, v)))) &&
                forall(|a2: Acceptor| q.contains(a2) ==>
                    exists(|vote: Option<(Ballot, Value)>|
                        msgs_state.msgs.contains(Message::Msg1b(a2, b, vote)) &&
                        match vote {
                            Some((mb2, _)) => ballot_greater_equal(mb, mb2),
                            None => true
                        }
                    )
                ),
            None => forall(|a: Acceptor| q.contains(a) ==> msgs_state.msgs.contains(Message::Msg1b(a, b, None)))
        });

        if decide() { // We can cancel the ballot (e.g. timeout)
            return;
        }

        g.acquire();
        let msg = receive(g);
        messages_still_there(msgs_state, g);
        msgs_state = g.state;
        g.release_stutter();

        match msg {
            Message::Msg1b(a, msg_b, msg_v) => {
                if msg_b == b && !q_concrete.contains(a) {
                    vote_count += 1;
                    q_concrete.insert(a);
                    q = q.add(a);
                    match msg_v {
                        Some((mb, v)) => {
                            match max_vote {
                                Some((_, mb2, _)) => {
                                    if ballot_greater(mb, mb2) {
                                        max_vote = Some((a, mb, v));
                                    }
                                }
                                None => {
                                    max_vote = Some((a, mb, v));
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
            _ => {}
        }
    }

    let v = match max_vote {
        Some((_, _, v)) => v,
        None => choose_value(),
    };

    g.acquire();
    messages_still_there(msgs_state, g);

    guard.open(g, make_map!(|s: AbsState| (forall(|c: u32, v2: Value|
        c >= counter ==>
        !s.msgs.contains(Message::Msg2a((c, my_id), v2))
    ))));

    send(Message::Msg2a(b, v), g);

    let max_b = match max_vote {
        None => None,
        Some((a, mb, _)) => Some((a, mb))
    };
    g.release(Action::Phase2a(b, v, q, max_b));
}

// #[trusted] // save verification time
#[requires(!g.locked())]
#[requires(guard.invariant_any(GuardKind::Acceptor(my_id)))]
#[requires(guard.last_state().max_bal.get(my_id) === None)]
#[requires(guard.last_state().max_vote.get(my_id) === None)]
#[ensures(!g.locked())]
fn run_acceptor(my_id: Acceptor, g: &mut PxStateHandle, guard: &mut PxGuard) {
    let mut max_bal: Option<Ballot> = None;
    let mut max_vote: Option<(Ballot, Value)> = None;

    loop {
        body_invariant!(!g.locked());
        body_invariant!(guard.invariant_any(GuardKind::Acceptor(my_id)));
        body_invariant!(guard.last_state().max_bal.get(my_id) === max_bal);
        body_invariant!(guard.last_state().max_vote.get(my_id) === max_vote);

        g.acquire();
        let msg = receive(g);
        let old_state = g.state;
        g.release_stutter();

        match msg {
            Message::Msg1a(b) => {
                let greater = match max_bal {
                    Some(mb) => ballot_greater(b, mb),
                    None => true
                };
                if greater {
                    g.acquire();
                    messages_still_there(old_state, g);

                    guard.open(g, make_map!(|s: AbsState| (
                        s.max_bal.get(my_id) === max_bal &&
                        s.max_vote.get(my_id) === max_vote
                    )));

                    max_bal = Some(b);
                    g.state.max_bal = g.state.max_bal.set(my_id, max_bal);

                    send(Message::Msg1b(my_id, b, max_vote), g);

                    g.release(Action::Phase1b(my_id, b));
                }
            },
            Message::Msg2a(b, v) => {
                let greater_equal = match max_bal {
                    Some(mb) => ballot_greater_equal(b, mb),
                    None => true
                };
                if greater_equal {
                    g.acquire();
                    messages_still_there(old_state, g);

                    guard.open(g, make_map!(|s: AbsState| (
                        s.max_bal.get(my_id) === max_bal
                    )));

                    max_bal = Some(b);
                    g.state.max_bal = g.state.max_bal.set(my_id, max_bal);

                    max_vote = Some((b, v));
                    g.state.max_vote = g.state.max_vote.set(my_id, max_vote);

                    // Work around Prusti bug. ("Folding struct$m_AbsState(_49.val_ref.f$state) might fail. There might be insufficient permission to access self.f$msgs")
                    g.state.msgs = g.state.msgs;

                    send(Message::Msg2b(my_id, b, v), g);

                    g.release(Action::Phase2b(my_id, b, v));
                }
            },
            _ => {}
        }
    }
}

#[trusted]
fn parse_command_line() -> GuardKind {
    unimplemented!();
}

// #[trusted] // save verification time
fn main() {
    let myself = parse_command_line();
    let init_state = AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) };
    let (mut g, mut dispenser) = PaxosModel::new(init_state);
    let mut guard = dispenser.take(myself);
    match myself {
        GuardKind::Leader(l) => run_leader(l, &mut g, &mut guard),
        GuardKind::Acceptor(a) => run_acceptor(a, &mut g, &mut guard),
    }
}

// #[trusted] // save verification time
#[allow(unused)]
/// This function models how the entire distributed system is started.
fn initialize() {
    let init_state = AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) };
    let (mut g, mut dispenser) = PaxosModel::new(init_state);

    let mut l = 0;
    while l < 5 {
        body_invariant!(forall(|leader: Leader| leader >= l ==>
            !dispenser.created().contains(GuardKind::Leader(leader))
        ));
        body_invariant!(forall(|acceptor: Acceptor|
            !dispenser.created().contains(GuardKind::Acceptor(acceptor))
        ));
        let mut guard = dispenser.take(GuardKind::Leader(l));

        thread_spawn!(
            g: PxStateHandle,
            requires(guard.invariant_any(GuardKind::Leader(l)))
            requires(guard.last_state() === AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) })
            param(mut guard: PxGuard) param(l: Leader)
            |g| {
                // Work around Prusti bug ("Consistency error: No matching identifier")
                let _ = AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) };
                let _ = GuardKind::Leader(l);

                run_leader(l, g, &mut guard);
            }
        );
        l += 1;
    }

    let mut a = 0;
    while a < 5 {
        body_invariant!(forall(|acceptor: Acceptor| acceptor >= a ==>
            !dispenser.created().contains(GuardKind::Acceptor(acceptor))
        ));
        let mut guard = dispenser.take(GuardKind::Acceptor(a));

        thread_spawn!(
            g: PxStateHandle,
            requires(guard.invariant_any(GuardKind::Acceptor(a)))
            requires(guard.last_state() === AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) })
            param(mut guard: PxGuard) param(a: Acceptor)
            |g| {
                // Work around Prusti bug ("Consistency error: No matching identifier")
                let _ = AbsState { msgs: AbsSet::empty(), max_bal: AbsMap::new(None), max_vote: AbsMap::new(None) };
                let _ = GuardKind::Acceptor(a);

                run_acceptor(a, g, &mut guard);
            }
        );
        a += 1;
    }
}
