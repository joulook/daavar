use std::time::{Duration, Instant, SystemTime};

use prusti_contracts::*;
use prusti_refinement::prelude::*;

use crate::model::{MemcacheModel, McStateHandle};

// This does not need to be trusted, but if it is not, verification time increases by 3 minutes.
#[trusted]
#[pure]
pub fn is_expired(expiry: Option<Instant>, now: Instant) -> bool {
    match expiry {
        Some(expiry) => instant_lte(expiry, now),
        None => false,
    }
}

#[trusted]
#[pure]
/// Calculate the expiration time from the integer as it appears in the
/// memcached protocol. None means never expire.
pub fn interpret_expiry(expiry: i64, now: Instant, unix_now: u64) -> Option<Instant> {
    if expiry == 0 { // Don't expire
        return None;
    }
    let mut expiry = match u64::try_from(expiry) {
        Ok(expiry) => expiry,
        Err(_) => 0, // expiry is negative, expire immediately
    };

    if expiry > 60*60*24*30 { // Unix time
        // If expiry is in the past, this saturates to 0
        expiry = expiry.saturating_sub(unix_now);
    }
    // If this addition overflows, don't expire
    // (we assume that the monotonic clock does not overflow)
    now.checked_add(Duration::from_secs(expiry))
}

/// This is needed because the less than function is not marked pure.
#[trusted]
#[pure]
pub fn instant_lte(l: Instant, r: Instant) -> bool {
    l <= r
}

/// Get the current time of the monotonic clock.
/// This is a non-mover.
#[trusted]
#[requires(g.locked())]
#[requires(g.env_state() === MemcacheModel::get_env_state(g.state))]
#[ensures(g.locked())]
#[ensures(g.env_state() === MemcacheModel::get_env_state(g.state))]
#[ensures(g.id() === old(g.id()))]
#[ensures(g.state === old(g.state))]
#[ensures(result === g.state.now)]
pub fn get_monotonic_now(g: &mut McStateHandle) -> Instant {
    let _ = g;
    Instant::now()
}

/// Get the current unix time according to the system clock.
/// This is a non-mover.
#[trusted]
#[requires(g.locked())]
#[requires(g.env_state() === MemcacheModel::get_env_state(g.state))]
#[ensures(g.locked())]
#[ensures(g.env_state() === MemcacheModel::get_env_state(g.state))]
#[ensures(g.id() === old(g.id()))]
#[ensures(g.state === old(g.state))]
#[ensures(result === g.state.unix_now)]
pub fn get_unix_now(g: &mut McStateHandle) -> u64 {
    let _ = g;
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs()
}
