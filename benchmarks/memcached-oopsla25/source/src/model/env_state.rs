use std::time::Instant;

use prusti_refinement::abs_types::{AbsMap, AbsSeq};

use crate::model::ConId;
use crate::model::state::NetworkConState;
use crate::model::message::{AbsCommand, AbsResponse};

#[derive(Clone, Copy)]
#[allow(unused)]
pub struct EnvState {
    pub now: Instant,
    pub unix_now: u64,
    pub network_id: ConId,
    pub network_state: AbsMap<ConId, NetworkConState>,
    pub network_cmd: AbsMap<ConId, AbsSeq<AbsCommand>>,
    pub network_res: AbsMap<ConId, AbsSeq<AbsResponse>>,
}
