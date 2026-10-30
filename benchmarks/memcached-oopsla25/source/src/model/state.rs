use std::time::Instant;

use prusti_contracts::*;
use prusti_refinement::abs_types::{AbsMap, AbsSeq, AnyValue};

use crate::model::{AbsCache, ConId};
use crate::model::message::{AbsCommand, AbsResponse};

#[derive(Clone, Copy)]
pub enum ConState {
    Idle,
    HaveCommand(AbsCommand),
    HaveResponse(AbsResponse),
}

// Only needed to work around missing ghost code support.
impl AnyValue for ConState {
    #[trusted]
    fn value() -> Self {
        ConState::Idle
    }
}

#[derive(Clone, Copy)]
pub enum NetworkConState {
    Pending,
    Established,
    Closed,
}

// Only needed to work around missing ghost code support.
impl AnyValue for NetworkConState {
    #[trusted]
    fn value() -> Self {
        NetworkConState::Pending
    }
}

#[derive(Clone, Copy)]
pub struct AbsState {
    pub now: Instant,
    pub unix_now: u64,
    pub network_id: ConId,
    pub network_state: AbsMap<ConId, NetworkConState>,
    pub network_cmd: AbsMap<ConId, AbsSeq<AbsCommand>>,
    pub network_res: AbsMap<ConId, AbsSeq<AbsResponse>>,
    pub con_state: AbsMap<ConId, ConState>,
    pub cache: AbsCache,
}
