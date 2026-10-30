use crate::model::ConId;

#[derive(Clone, Copy)]
pub enum GuardKind {
    Connection(ConId),
    Server,
    StorageBucket(usize),
}
