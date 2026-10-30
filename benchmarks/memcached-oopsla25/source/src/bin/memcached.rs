// Disable unused_mut, because it has false positives in Prusti.
// The extra braces are there to work around a Prusti limitation with constants in loops.
#![allow(unused_mut, unused_braces)]

use std::net::TcpListener;
use std::time::Instant;

use prusti_contracts::*;
use prusti_refinement::prelude::*;

use prusti_refinement::abs_types::{AbsMap, AbsSeq};
use prusti_refinement::std_wrappers::ArcWrap;
use prusti_refinement::make_map;

use memcached_lib::model::{ConId, MemcacheModel, McGuard, McStateHandle};
use memcached_lib::model::action::Action;
use memcached_lib::model::guard::GuardKind;
use memcached_lib::model::message::AbsResponse;
use memcached_lib::model::state::{AbsState, ConState, NetworkConState};
use memcached_lib::storage::Storage;
use memcached_lib::protocol::protocol::{ProtocolClient, ProtocolListener};

#[requires(storage.type_invariant())]
#[requires(!g.locked())]
#[ensures(!g.locked())]
#[requires(client_guard.invariant_any(GuardKind::Connection(client.id())))]
#[requires(client_guard.last_state().con_state.get(client.id()) === ConState::Idle)]
fn handle_client(storage: &Storage, mut client: ProtocolClient, mut client_guard: McGuard, g: &mut McStateHandle) -> std::io::Result<()> {
    let client_id = client.id();
    loop {
        body_invariant!(!g.locked());
        body_invariant!(client.id() == client_id);
        body_invariant!(client_guard.invariant_any(GuardKind::Connection(client.id())));
        body_invariant!(client_guard.last_state().con_state.get(client.id()) === ConState::Idle);

        // Receive command
        g.acquire();
        client_guard.open(g, make_map!(|s: AbsState| (
            s.con_state.get(client.id()) === ConState::Idle
        )));

        let cmd = match client.read(g) {
            Ok(cmd) => cmd,
            Err(err) => {
                g.release_stutter();

                return Err(err);
            }
        };
        let cmd_abs = cmd.as_abs();
        g.state.con_state = g.state.con_state.set(client.id(), ConState::HaveCommand(cmd_abs));
        g.release(Action::ReceiveCommand(client.id(), cmd_abs));

        // Process command
        let resp = storage.process(cmd, client.id(), &mut client_guard, g);
        let resp_abs = resp.as_abs();

        // Send response
        g.acquire();
        client_guard.open(g, make_map!(|s: AbsState| (
            s.con_state.get(client.id()) === ConState::HaveResponse(resp_abs)
        )));

        if let Err(err) = client.write(resp, g) {
            g.release_stutter();
            return Err(err);
        }
        g.state.con_state = g.state.con_state.set(client.id(), ConState::Idle);
        g.release(Action::SendResponse(client.id(), resp_abs));
    }
}

#[trusted]
/// Work around Prusti bug:
/// cannot generate fold-unfold Viper statements. Failed to subtract fractional permissions: invalid substraction: read - write
/// Replacing the call to TcpListener::bind with a call to this is enough to make it work.
fn tcp_bind_wrap<A: std::net::ToSocketAddrs>(addr: A) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr)
}

fn main() -> std::io::Result<()> {
    // Work around Prusti bug "Consistency error: No matching identifier ... found"
    let _: AbsResponse = AbsResponse::Set;

    let init_state = AbsState {
        now: Instant::now(),
        unix_now: 0,
        network_id: 0,
        network_state: AbsMap::new(NetworkConState::Pending),
        network_cmd: AbsMap::new(AbsSeq::empty()),
        network_res: AbsMap::new(AbsSeq::empty()),
        con_state: AbsMap::new(ConState::Idle),
        cache: AbsMap::new(None),
    };
    let (mut g, mut dispenser) = MemcacheModel::new(init_state);

    let mut server_guard = dispenser.take(GuardKind::Server);

    let storage_guards = dispenser.split(make_map!(|kind: GuardKind| (
        match kind {
            GuardKind::StorageBucket(_) => true,
            _ => false,
        }
    )));
    let storage = ArcWrap::new(Storage::new(storage_guards));

    let listener = match tcp_bind_wrap("127.0.0.1:11212") {
        Ok(listener) => ProtocolListener::new(listener),
        Err(_) => {
            // assume this does not happen
            prusti_assume!(false);
            panic!()
        }
    };

    let mut next_id: u32 = 0;
    loop {
        body_invariant!(!g.locked());
        body_invariant!(server_guard.invariant_any(GuardKind::Server));
        body_invariant!(server_guard.last_state().network_id === next_id);
        body_invariant!(forall(|id: u32| id >= next_id ==>
            !dispenser.created().contains(GuardKind::Connection(id))
        ));

        g.acquire();
        server_guard.open(&g, make_map!(|s: AbsState| (
            s.network_id === next_id
        )));
        match listener.accept(&mut g) {
            Ok(client) => {
                #[cfg(not(prusti))]
                eprintln!("New connection.");

                g.release(Action::AcceptConnection);

                let client_guard = dispenser.take(GuardKind::Connection(next_id));

                // Assumption: next_id should be a natural number
                // instead of u32, but this is not supported by Prusti.
                prusti_assume!(next_id < u32::MAX);
                next_id += 1;

                let storage_clone = storage.clone();
                let client_id = client.id();
                prusti_refinement::thread_spawn!(
                    g: McStateHandle,
                    requires(storage_clone.deref_pure().type_invariant())
                    requires(client_guard.invariant_any(GuardKind::Connection(client_id)))
                    requires(client_guard.last_state().con_state.get(client_id) === ConState::Idle)
                    requires(client.id() == client_id)
                    param(client_id: ConId)
                    param(storage_clone: ArcWrap<Storage>)
                    param(client: ProtocolClient)
                    param(client_guard: McGuard)
                    |g| {
                        // Work around Prusti bug "Consistency error: No matching identifier"
                        let _ = ConState::Idle;
                        let _ = Action::AdvanceTime;

                        if let Err(err) = handle_client(&storage_clone.deref(), client, client_guard, g) {
                            let _ = err;
                            #[cfg(not(prusti))]
                            eprintln!("Connection closed: {}", err);
                        }
                    }
                );
            }
            Err(err) => {
                let _ = err;
                #[cfg(not(prusti))]
                eprintln!("Failed to accept connection: {}", err);

                g.release_stutter();
            }
        }
    }
}
