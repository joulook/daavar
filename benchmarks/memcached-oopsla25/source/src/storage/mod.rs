#![allow(unused_mut)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use prusti_contracts::*;
use prusti_refinement::abs_types::{AbsMap, AbsSeq};
use prusti_refinement::dispenser::GuardDispenser;
use prusti_refinement::state_handle::{GL_INIT_ID, gl_released, gl_final_state};
use prusti_refinement::std_wrappers::{MutexWrap, VecWrap};
use prusti_refinement::make_map;

use crate::model::{AbsCache, AbsItem, ConId, MemcacheModel, McStateHandle, McGuard, hash_abs};
use crate::model::action::Action;
use crate::model::guard::GuardKind;
use crate::model::state::{AbsState, ConState};
use crate::model::time::{get_monotonic_now, get_unix_now, interpret_expiry, is_expired};
use crate::protocol::message::{Command, Response};
use crate::proof_utils::{axiom_slice_to_seq_injective, clone_slice, slice_to_seq};

pub const KEY_MAX_LEN: usize = 250;
pub const VALUE_MAX_LEN: usize = 1024 * 1024;
pub const TABLE_SIZE: usize = 1 << 16;

#[trusted]
#[ensures(result == hash_abs(slice_to_seq(key)))]
#[ensures(result < TABLE_SIZE)]
pub fn hash(key: &[u8]) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    hash & (TABLE_SIZE - 1)
}

#[derive(Clone)]
pub struct Item {
    pub next: Option<Box<Item>>,
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
    pub expiry: Option<Instant>,
}

impl Item {
    #[pure]
    pub fn as_abs(&self) -> AbsItem {
        AbsItem {
            value: slice_to_seq(&self.value),
            expiry: self.expiry,
        }
    }
}

pub struct Bucket {
    head: Option<Box<Item>>,
    guard: McGuard,
}

impl Bucket {
    predicate! { pub fn type_invariant(&self, hash: usize) -> bool {
        self.guard.invariant_any(GuardKind::StorageBucket(hash)) &&
        self.guard_invariant(hash, self.guard.last_state().cache)
    } }

    predicate! { fn guard_invariant(&self, hash: usize, state: AbsCache) -> bool {
        Self::list_valid(&self.head, hash, state)
    } }

    predicate! { fn list_valid(list: &Option<Box<Item>>, hash: usize, state: AbsCache) -> bool {
        forall(|key: AbsSeq<u8>|
            hash_abs(key) == hash ==>
            Self::item_valid(list, key, state.get(key))
        )
    } }

    predicate! { fn item_valid(list: &Option<Box<Item>>, key: AbsSeq<u8>, abs_item: Option<AbsItem>) -> bool {
        match list {
            Some(item) =>
                if (slice_to_seq(&item.key) === key) {
                    abs_item === Some(item.as_abs()) &&
                    Self::item_valid(&item.next, key, None)
                } else {
                    Self::item_valid(&item.next, key, abs_item)
                },
            None => abs_item === None,
        }
    } }

    predicate! { fn lookup_item(list: &Option<Box<Item>>, key: AbsSeq<u8>) -> &Option<Box<Item>> {
        match list {
            Some(item) =>
                if (slice_to_seq(&item.key) === key) {
                    list
                } else {
                    Self::lookup_item(&item.next, key)
                },
            None => &None,
        }
    } }

    #[requires(Self::list_valid(&list, hash_abs(slice_to_seq(key)), state))]
    #[ensures(state.get(slice_to_seq(&key)) === match result { Some(item) => Some(item.as_abs()), None => None })]
    #[ensures(*result === *old(Self::lookup_item(list, slice_to_seq(key))))]
    #[after_expiry(
        match old(Self::lookup_item(list, slice_to_seq(key))) {
            // key not found, list unchanged
            None => *before_expiry(result) === None ==> *list === *old(&list),
            Some(item) =>
                // key found, item possibly updated
                match before_expiry(&*result) {
                    Some(new_item) if (new_item.key === item.key && new_item.next === item.next) =>
                        Self::list_valid(&list, hash_abs(slice_to_seq(old(key))),
                            old(state).set(slice_to_seq(old(key)), Some(new_item.as_abs()))),
                    _ => true,
                }
                &&
                // key found and removed
                (*before_expiry(&*result) === item.next ==>
                Self::list_valid(&list, old(hash_abs(slice_to_seq(key))), old(state.set(slice_to_seq(key), None)))),
        }
    )]
    fn find_item<'a>(list: &'a mut Option<Box<Item>>, key: &[u8], state: AbsCache) -> &'a mut Option<Box<Item>> {
        // It would be better if this method was iterative instead of recursive.
        // However, that would require putting after_expiry into the loop invariant,
        // which Prusti does not support.
        let item_key;
        match &list {
            Some(item) => {
                if &*item.key == key {
                    return list;
                }
                item_key = slice_to_seq(&item.key);
            }
            None => {
                return list;
            }
        }
        axiom_slice_to_seq_injective();
        // Work around Prusti limitations by moving operations into
        // the separate methods list_unwrap and item_next.
        Self::find_item(Self::item_next(Self::list_unwrap(list)), key, state.set(item_key, None))
    }

    #[requires(match list { Some(_) => true, None => false })]
    #[ensures(match old(list) { Some(item) => *result === *item, None => false })]
    #[after_expiry(match list { Some(item) => *before_expiry(result) === *item, None => false })]
    fn list_unwrap(list: &mut Option<Box<Item>>) -> &mut Item {
        match list {
            Some(item) => item,
            None => unreachable!(),
        }
    }

    #[ensures(*result === *old(&item.next))]
    #[after_expiry(
        item.next === *before_expiry(result) &&
        item.key === *old(&item.key) &&
        item.value === *old(&item.value) &&
        item.expiry === *old(&item.expiry)
    )]
    fn item_next(item: &mut Item) -> &mut Option<Box<Item>> {
        &mut item.next
    }

    #[requires(self.guard_invariant(hash_abs(slice_to_seq(&key)), *state))]
    #[ensures(self.guard_invariant(hash_abs(old(slice_to_seq(&key))), *state))]
    #[ensures(self.guard === *old(&self.guard))]
    #[ensures(*state === old(state.set(slice_to_seq(&key), Some(AbsItem {
        value: slice_to_seq(&value),
        expiry: interpret_expiry(expiry, now, unix_now),
    }))))]
    fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>, expiry: i64, now: Instant, unix_now: u64, state: &mut AbsCache) {
        // Force creation of discriminant axioms
        let _: Option<Item> = None;

        axiom_slice_to_seq_injective();

        let expiry = interpret_expiry(expiry, now, unix_now);

        // If an item with this key already exists, delete it
        let p = Self::find_item(&mut self.head, &key, *state);
        if let Some(item) = p {
            *p = item.next.take();
        }

        // Insert the item
        *state = state.set(slice_to_seq(&key), Some(AbsItem { value: slice_to_seq(&value), expiry }));
        self.head = Some(Box::new(Item { next: self.head.take(), key, value, expiry }));
    }

    #[requires(self.guard_invariant(hash_abs(slice_to_seq(&key)), *state))]
    #[ensures(self.guard_invariant(hash_abs(old(slice_to_seq(&key))), *state))]
    #[ensures(self.guard === *old(&self.guard))]
    #[ensures(
        match state.get(slice_to_seq(key)) {
            Some(item) => Some(item.value),
            None => None,
        } ===
        match &result {
            Some(value) => Some(slice_to_seq(value)),
            None => None,
        }
    )]
    #[ensures(*state === match old(*state).get(slice_to_seq(key)) {
        Some(AbsItem { expiry, .. }) if is_expired(expiry, now) => old(*state).set(slice_to_seq(key), None),
        _ => old(*state),
    })]
    fn get(&mut self, key: &[u8], now: Instant, state: &mut AbsCache) -> Option<Box<[u8]>> {
        // Force creation of discriminant axioms
        let _: Option<AbsItem> = None;

        let p = Self::find_item(&mut self.head, key, *state);
        let item = match p {
            Some(item) => item,
            None => {
                return None;
            }
        };
        if is_expired(item.expiry, now) {
            // Remove the item
            *p = item.next.take();

            axiom_slice_to_seq_injective();
            *state = state.set(slice_to_seq(key), None);

            return None;
        }
        Some(clone_slice(&item.value))
    }

    /// Returns true if the key was found.
    #[requires(self.guard_invariant(hash_abs(slice_to_seq(&key)), *state))]
    #[ensures(self.guard_invariant(hash_abs(old(slice_to_seq(&key))), *state))]
    #[ensures(self.guard === *old(&self.guard))]
    #[ensures(*state === old(state.set(slice_to_seq(key), None)))]
    #[ensures(result == match old(state).get(slice_to_seq(key)) {
        Some(AbsItem { expiry, .. }) => !is_expired(expiry, now),
        None => false,
    })]
    fn delete(&mut self, key: &[u8], now: Instant, state: &mut AbsCache) -> bool {
        axiom_slice_to_seq_injective();

        let p = Self::find_item(&mut self.head, key, *state);
        *state = state.set(slice_to_seq(key), None);
        let item = match p.take() {
            Some(item) => item,
            None => {
                return false;
            }
        };
        *p = item.next;
        return !is_expired(item.expiry, now);
    }
}

pub struct Storage {
    table: VecWrap<MutexWrap<Bucket>>,
}

impl Storage {
    predicate! { pub fn type_invariant(&self) -> bool {
        self.table.len() == TABLE_SIZE &&
        forall(|hash: usize, bucket: &Bucket| hash < TABLE_SIZE ==>
            self.table.index_pure(hash).invariant().get(bucket) ==
            bucket.type_invariant(hash)
        )
    } }


    #[requires(forall(|hash: usize| !guards.created().contains(GuardKind::StorageBucket(hash))))]
    #[requires(gl_released(GL_INIT_ID))]
    #[requires(gl_final_state::<MemcacheModel>(GL_INIT_ID).cache === AbsMap::new(None))]
    #[ensures(result.type_invariant())]
    pub fn new(mut guards: GuardDispenser<MemcacheModel>) -> Self {
        let mut table = VecWrap::<MutexWrap<Bucket>>::with_capacity(TABLE_SIZE);
        let mut hash = 0;
        while hash < TABLE_SIZE {
            body_invariant!(table.len() == hash);
            body_invariant!(forall(|h: usize, bucket: &Bucket| h < hash ==>
                table.index_pure(h).invariant().get(bucket) ==
                bucket.type_invariant(h)
            ));
            body_invariant!(forall(|h: usize| h >= hash ==>
                !guards.created().contains(GuardKind::StorageBucket(h))
            ));

            let guard = guards.take(GuardKind::StorageBucket(hash));
            let predicate = make_map!(|b: &Bucket| (b.type_invariant(hash)));
            table.push(MutexWrap::new(Bucket {
                head: None,
                guard,
            }, predicate));
            hash += 1;
        }
        Storage {
            table,
        }
    }

    #[requires(self.type_invariant())]
    #[requires(!g.locked())]
    #[ensures(!g.locked())]
    #[requires(client_guard.invariant_any(GuardKind::Connection(client_id)))]
    #[ensures(client_guard.invariant_any(GuardKind::Connection(client_id)))]
    #[requires(client_guard.last_state().con_state.get(client_id) === ConState::HaveCommand(cmd.as_abs()))]
    #[ensures(client_guard.last_state().con_state.get(client_id) === ConState::HaveResponse(result.as_abs()))]
    pub fn process(&self, cmd: Command, client_id: ConId, client_guard: &mut McGuard, g: &mut McStateHandle) -> Response {
        g.acquire();
        let now = get_monotonic_now(g);
        g.release_stutter();

        // Measure monotonic time before system time, since
        // expiry should rather be too early than too late.
        g.acquire();
        let unix_now = get_unix_now(g);
        g.release_stutter();

        let hash = match &cmd {
            Command::Set(cmd) => hash(&cmd.key),
            Command::Get(cmd) => hash(&cmd.key),
            Command::Delete(cmd) => hash(&cmd.key),
        };

        let mut bucket_guard = self.table.index(hash).lock();
        let bucket_locked = bucket_guard.deref_mut();

        g.acquire();
        let cmd_abs = cmd.as_abs();
        client_guard.open(g, make_map!(|s: AbsState| (
            s.con_state.get(client_id) === ConState::HaveCommand(cmd_abs)
        )));

        let predicate = make_map!(|s: AbsState| (
            bucket_locked.guard_invariant(hash, s.cache)
        ));
        bucket_locked.guard.open(g, predicate);

        let resp = match cmd {
            Command::Set(cmd) => {
                bucket_locked.set(cmd.key, cmd.value, cmd.expiry, now, unix_now, &mut g.state.cache);
                Response::Set
            }
            Command::Get(cmd) => {
                let value = bucket_locked.get(&cmd.key, now, &mut g.state.cache);
                Response::Get { key: cmd.key, value }
            }
            Command::Delete(cmd) => {
                Response::Delete { found: bucket_locked.delete(&cmd.key, now, &mut g.state.cache) }
            }
        };

        let resp_abs = resp.as_abs();
        g.state.con_state = g.state.con_state.set(client_id, ConState::HaveResponse(resp_abs));
        g.release(Action::ProcessCommand(client_id, cmd_abs, resp_abs, now, unix_now));

        bucket_guard.unlock();

        resp
    }
}
