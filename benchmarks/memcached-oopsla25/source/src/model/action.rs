use prusti_contracts::*;
use std::time::Instant;

use prusti_refinement::abs_types::AbsSeq;
use prusti_refinement::abs_types::AnyValue;

use crate::model::ConId;
use crate::model::message::{AbsCommand, AbsResponse};

#[allow(unused)]
#[derive(Clone, Copy)]
pub enum Action {
    AdvanceTime,
    UpdateUnixTime,

    AcceptConnection,
    CloseConnection(ConId),

    SendCommand(ConId, AbsCommand),
    ReceiveCommand(ConId, AbsCommand),
    SendResponse(ConId, AbsResponse),
    ReceiveResponse(ConId, AbsResponse),

    ProcessCommand(ConId, AbsCommand, AbsResponse, Instant, u64),
    Evict(AbsSeq<u8>),
}

impl AnyValue for Action {
    #[trusted]
    fn value() -> Self { Action::AdvanceTime }
}
