use std::time::Instant;

use prusti_contracts::*;
use prusti_refinement::abs_types::AbsSeq;

use crate::model::{AbsCache, AbsItem};
use crate::model::time::{is_expired, interpret_expiry};

#[derive(Clone, Copy)]
pub enum AbsCommand {
    Set { key: AbsSeq<u8>, value: AbsSeq<u8>, expiry: i64 },
    Get { key: AbsSeq<u8> },
    Delete { key: AbsSeq<u8> }
}

#[derive(Clone, Copy)]
pub enum AbsResponse {
    Set,
    Get { key: AbsSeq<u8>, value: Option<AbsSeq<u8>> },
    Delete { found: bool },
}

predicate! { pub fn cache_process(
    s: AbsCache,
    n: AbsCache,
    cmd: AbsCommand,
    res: AbsResponse,
    now: Instant,
    unix_now: u64,
) -> bool {
    match cmd {
        AbsCommand::Set { key, value, expiry } =>
            res === AbsResponse::Set &&
            n === s.set(key, Some(AbsItem {
                value,
                expiry: interpret_expiry(expiry, now, unix_now),
            })),
        AbsCommand::Get { key } =>
            res === AbsResponse::Get {
                key,
                value: match n.get(key) {
                    Some(item) => Some(item.value),
                    None => None,
                },
            } &&
            n === match s.get(key) {
                Some(AbsItem { expiry, .. })
                    if is_expired(expiry, now) => s.set(key, None),
                _ => s,
            },
        AbsCommand::Delete { key } =>
            // res === AbsResponse::Delete {
            //     found: match s.get(key) {
            //         Some(AbsItem { expiry, .. }) => !is_expired(expiry, now),
            //         None => false,
            //     }
            // } &&
            // Work around Prusti bug: "Consistency error: Local variable not found"
            match res {
                AbsResponse::Delete { found } => found == match s.get(key) {
                    Some(AbsItem { expiry, .. }) => !is_expired(expiry, now),
                    None => false,
                },
                _ => false,
            } &&
            n === s.set(key, None),
    }
} }
