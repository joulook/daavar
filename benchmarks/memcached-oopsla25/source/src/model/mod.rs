use std::time::Instant;

use prusti_contracts::*;
use prusti_refinement::prelude::*;
use prusti_refinement::abs_types::{AbsMap, AbsSeq};

pub mod action;
pub mod env_state;
pub mod guard;
pub mod message;
pub mod state;
pub mod time;

use action::Action;
use env_state::EnvState;
use guard::GuardKind;
#[cfg(prusti)] use message::{AbsCommand, cache_process};
use state::{AbsState, ConState, NetworkConState};
use time::instant_lte;

#[trusted]
#[pure]
pub fn hash_abs(key: AbsSeq<u8>) -> usize {
    let _ = key;
    unimplemented!();
}

#[derive(Clone, Copy)]
#[allow(unused)]
pub struct AbsItem {
    pub value: AbsSeq<u8>,
    pub expiry: Option<Instant>,
}

pub type AbsCache = AbsMap<AbsSeq<u8>, Option<AbsItem>>;
pub type ConId = u32;
pub type McStateHandle = StateHandle<MemcacheModel>;
pub type McGuard = Guard<MemcacheModel>;

pub struct MemcacheModel;

#[refine_trait_spec]
impl Model for MemcacheModel {
    type AbsState = AbsState;
    type Action = Action;
    type GuardKind = GuardKind;
    type EnvState = EnvState;

    predicate! { fn init(s: AbsState) -> bool {
        s === AbsState {
            now: s.now,
            unix_now: s.unix_now,
            network_id: 0,
            network_state: AbsMap::new(NetworkConState::Pending),
            network_cmd: AbsMap::new(AbsSeq::empty()),
            network_res: AbsMap::new(AbsSeq::empty()),
            con_state: AbsMap::new(ConState::Idle),
            cache: AbsMap::new(None),
        }
    } }

    predicate! { fn next(s: AbsState, n: AbsState, a: Action) -> bool {
        match a {
            Action::AdvanceTime =>
                instant_lte(s.now, n.now) &&
                n === AbsState { now: n.now, ..s },
            Action::UpdateUnixTime =>
                n === AbsState { unix_now: n.unix_now, ..s },

            Action::AcceptConnection =>
                s.network_state.get(s.network_id) === NetworkConState::Pending &&
                n === AbsState {
                    network_id: s.network_id + 1,
                    network_state: s.network_state.set(s.network_id, NetworkConState::Established),
                    ..s
                },
            Action::CloseConnection(con) =>
                s.network_state.get(con) === NetworkConState::Established &&
                n === AbsState { network_state: s.network_state.set(con, NetworkConState::Closed), ..s },

            Action::SendCommand(con, cmd) => {
                let queue_cmd = s.network_cmd.get(con);
                n === AbsState {
                    network_cmd: s.network_cmd.set(con, queue_cmd.append(cmd)),
                    ..s
                }
            },
            Action::ReceiveCommand(con, cmd) => {
                match s.network_cmd.get(con).split_head() {
                    Some((head, tail)) =>
                        head === cmd &&
                        s.con_state.get(con) === ConState::Idle &&
                        n === AbsState {
                            network_cmd: s.network_cmd.set(con, tail),
                            con_state: s.con_state.set(con, ConState::HaveCommand(cmd)),
                            ..s
                        },
                    None => false,
                }
            },
            Action::SendResponse(con, res) => {
                let queue_res = s.network_res.get(con);
                s.con_state.get(con) === ConState::HaveResponse(res) &&
                n === AbsState {
                    network_res: s.network_res.set(con, queue_res.append(res)),
                    con_state: s.con_state.set(con, ConState::Idle),
                    ..s
                }
            },
            Action::ReceiveResponse(con, res) => {
                match s.network_res.get(con).split_head() {
                    Some((head, tail)) =>
                        head === res &&
                        n === AbsState {
                            network_res: s.network_res.set(con, tail),
                            ..s
                        },
                    None => false,
                }
            },

            Action::ProcessCommand(con, cmd, res, now, unix_now) =>
                s.con_state.get(con) === ConState::HaveCommand(cmd) &&
                cache_process(s.cache, n.cache, cmd, res, now, unix_now) &&
                n === AbsState {
                    con_state: s.con_state.set(con, ConState::HaveResponse(res)),
                    cache: n.cache,
                    ..s
                },
            Action::Evict(key) =>
                n === AbsState { cache: s.cache.set(key, None), ..s },
        }
    } }

    predicate! { fn action_requires_guard(a: Action, g: GuardKind) -> bool {
        match a {
            Action::AdvanceTime => false,
            Action::UpdateUnixTime => false,

            Action::AcceptConnection => g === GuardKind::Server,
            Action::CloseConnection(con) => false,

            Action::SendCommand(con, _) => false,
            Action::ReceiveCommand(con, _) => g === GuardKind::Connection(con),
            Action::SendResponse(con, _) => g === GuardKind::Connection(con),
            Action::ReceiveResponse(con, _) => false,

            Action::ProcessCommand(con, cmd, _, _, _) =>
                g === GuardKind::Connection(con) ||
                match cmd {
                    AbsCommand::Set { key, .. } |
                    AbsCommand::Get { key, .. } |
                    AbsCommand::Delete { key, .. } => g === GuardKind::StorageBucket(hash_abs(key)),
                },
            Action::Evict(key) => g === GuardKind::StorageBucket(hash_abs(key)),
        }
    } }

    #[pure]
    fn get_env_state(s: AbsState) -> EnvState {
        EnvState {
            now: s.now,
            unix_now: s.unix_now,
            network_id: s.network_id,
            network_state: s.network_state,
            network_cmd: s.network_cmd,
            network_res: s.network_res,
        }
    }

    #[trusted]
    fn some_abs_state() -> AbsState {
        AbsState {
            now: Instant::now(),
            unix_now: 0,
            network_id: 0,
            network_state: AbsMap::havoc(),
            network_cmd: AbsMap::havoc(),
            network_res: AbsMap::havoc(),
            con_state: AbsMap::havoc(),
            cache: AbsMap::havoc(),
        }
    }
}
