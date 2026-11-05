use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[rustfmt::skip]
#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), ReadA(usize), ReadAB(usize) }
#[rustfmt::skip]
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack(usize), ValueA(usize, u64), ValueAB(usize, u64, u64) }

const CLIENTS: usize = 2;
#[rustfmt::skip]
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
#[rustfmt::skip]
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::ReadA(i)); let Rep::ValueA(..) = recv_msg_block::<Rep>() else { panic!("ValueA expected") }; }
            _ => { send_msg(node, Req::ReadAB(i)); let Rep::ValueAB(..) = recv_msg_block::<Rep>() else { panic!("ValueAB expected") }; }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: [ThreadId; 3],
    clients: [ThreadId; 2],
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Cell {
    value: u64,
    timestamp: u64,
}
type Data = [Cell; 2];
type Patch = [Option<Cell>; 2];
// Identity serialization of the requested cells is a collision-free digest.
type Digest = Data;
fn reconcile(left: Cell, right: Cell) -> Cell {
    if right.timestamp > left.timestamp {
        right
    } else {
        left
    }
}
fn apply(cells: &mut Data, patch: Patch) {
    for k in 0..2 {
        if let Some(cell) = patch[k] {
            cells[k] = reconcile(cells[k], cell);
        }
    }
}
fn project(cells: Data, ab: bool) -> Data {
    [cells[0], if ab { cells[1] } else { Cell::default() }]
}
fn digest(cells: Data) -> Digest {
    cells
}

#[derive(Clone, Debug, PartialEq)]
enum Internal {
    Mutation {
        coordinator: usize,
        id: usize,
        patch: Patch,
    },
    MutationAck {
        id: usize,
        from: usize,
    },
    Read {
        coordinator: usize,
        id: usize,
        ab: bool,
        digest_only: bool,
        full: bool,
    },
    Data {
        id: usize,
        from: usize,
        full: bool,
        cells: Data,
    },
    Digest {
        id: usize,
        from: usize,
        digest: Digest,
    },
    Repair {
        coordinator: usize,
        id: usize,
        patch: Patch,
    },
    RepairAck {
        id: usize,
        from: usize,
    },
}
const SETUP: u32 = 0;
const PAYLOAD: u32 = 1;
const NOTICE: u32 = 2;
// The API's typed receive cannot select heterogeneously. A notice has the same
// transport type as client requests, but is only sent between invisible nodes.
fn exchange(setup: &Setup, from: usize, to: usize, message: Internal) {
    send_tagged_msg(setup.nodes[to], PAYLOAD, message);
    send_tagged_msg(setup.nodes[to], NOTICE, Req::ReadAB(3 + from));
}

#[derive(Debug)]
enum Pending {
    Write {
        id: usize,
        client: usize,
        acks: [bool; 3],
    },
    Read(ReadState),
}
#[derive(Debug)]
struct ReadState {
    id: usize,
    client: usize,
    ab: bool,
    contacts: [usize; 2],
    data: Option<Data>,
    digest: Option<Digest>,
    full: bool,
    responses: [Option<Data>; 3],
    result: Option<Data>,
    waiting: [bool; 3],
}
fn reply_read(setup: &Setup, read: &ReadState, cells: Data) {
    let reply = if read.ab {
        Rep::ValueAB(read.client, cells[0].value, cells[1].value)
    } else {
        Rep::ValueA(read.client, cells[0].value)
    };
    send_msg(setup.clients[read.client], reply);
}
fn start_read(setup: &Setup, me: usize, id: usize, client: usize, ab: bool) -> Pending {
    let others: Vec<_> = (0..3).filter(|&n| n != me).collect();
    let other = others[usize::from(nondet())];
    let contacts = [me, other];
    for (index, &to) in contacts.iter().enumerate() {
        exchange(
            setup,
            me,
            to,
            Internal::Read {
                coordinator: me,
                id,
                ab,
                digest_only: index != 0,
                full: false,
            },
        );
    }
    Pending::Read(ReadState {
        id,
        client,
        ab,
        contacts,
        data: None,
        digest: None,
        full: false,
        responses: [None; 3],
        result: None,
        waiting: [false; 3],
    })
}
fn advance_read(setup: &Setup, me: usize, blocking: bool, read: &mut ReadState) -> bool {
    if !read.full {
        if let (Some(data), Some(remote_digest)) = (read.data, read.digest) {
            if digest(data) == remote_digest {
                reply_read(setup, read, data);
                return true;
            }
            read.full = true;
            // Fresh reads of BOTH participants; do not reuse the initial data.
            for &to in &read.contacts {
                exchange(
                    setup,
                    me,
                    to,
                    Internal::Read {
                        coordinator: me,
                        id: read.id,
                        ab: read.ab,
                        digest_only: false,
                        full: true,
                    },
                );
            }
        }
    } else if read.result.is_none() && read.contacts.iter().all(|&n| read.responses[n].is_some()) {
        let x = read.responses[read.contacts[0]].unwrap();
        let y = read.responses[read.contacts[1]].unwrap();
        let merged = [reconcile(x[0], y[0]), reconcile(x[1], y[1])];
        read.result = Some(merged);
        if blocking {
            for &to in &read.contacts {
                let original = read.responses[to].unwrap();
                let patch = std::array::from_fn(|k| {
                    if (k == 0 || read.ab) && original[k] != merged[k] {
                        Some(merged[k])
                    } else {
                        None
                    }
                });
                if patch.iter().any(Option::is_some) {
                    read.waiting[to] = true;
                    exchange(
                        setup,
                        me,
                        to,
                        Internal::Repair {
                            coordinator: me,
                            id: read.id,
                            patch,
                        },
                    );
                }
            }
        }
        // source threshold: max(0, 2 - number of empty repairs among the 2 contacts)
    }
    if let Some(merged) = read.result {
        if !read.waiting.iter().any(|&w| w) {
            reply_read(setup, read, merged);
            return true;
        }
    }
    false
}
fn node(me: usize, blocking: bool) {
    let setup: Setup = recv_tagged_msg_block(|_, tag| tag == Some(SETUP));
    // n2 distributes the immutable address book; main performs no messaging.
    if me == 2 {
        for to in 0..2 {
            send_tagged_msg(setup.nodes[to], SETUP, setup.clone());
        }
    }
    let mut cells = Data::default();
    let mut pending: Option<Pending> = None;
    let mut next_id = 0;
    loop {
        let event: Req = recv_tagged_msg_block(|_, tag| tag.is_none() || tag == Some(NOTICE));
        if let Req::ReadAB(marker) = event {
            if marker >= 3 {
                let sender = setup.nodes[marker - 3];
                let message: Internal =
                    recv_tagged_msg_block(move |from, tag| from == sender && tag == Some(PAYLOAD));
                match message {
                    Internal::Mutation {
                        coordinator,
                        id,
                        patch,
                    } => {
                        apply(&mut cells, patch);
                        exchange(
                            &setup,
                            me,
                            coordinator,
                            Internal::MutationAck { id, from: me },
                        );
                    }
                    Internal::Read {
                        coordinator,
                        id,
                        ab,
                        digest_only,
                        full,
                    } => {
                        let snapshot = project(cells, ab);
                        let response = if digest_only {
                            Internal::Digest {
                                id,
                                from: me,
                                digest: digest(snapshot),
                            }
                        } else {
                            Internal::Data {
                                id,
                                from: me,
                                full,
                                cells: snapshot,
                            }
                        };
                        exchange(&setup, me, coordinator, response);
                    }
                    Internal::Repair {
                        coordinator,
                        id,
                        patch,
                    } => {
                        apply(&mut cells, patch);
                        exchange(
                            &setup,
                            me,
                            coordinator,
                            Internal::RepairAck { id, from: me },
                        );
                    }
                    Internal::MutationAck { id, from } => {
                        if let Some(Pending::Write {
                            id: current,
                            client,
                            acks,
                        }) = &mut pending
                        {
                            if id == *current {
                                acks[from] = true;
                                if acks.iter().filter(|&&ack| ack).count() >= 2 {
                                    send_msg(setup.clients[*client], Rep::Ack(*client));
                                    pending = None;
                                }
                            }
                        } // Late third acknowledgements belong to their old operation.
                    }
                    response => {
                        if let Some(Pending::Read(read)) = &mut pending {
                            match response {
                                Internal::Data {
                                    id,
                                    from,
                                    full,
                                    cells,
                                } if id == read.id => {
                                    if full {
                                        read.responses[from] = Some(cells);
                                    } else {
                                        read.data = Some(cells);
                                    }
                                }
                                Internal::Digest {
                                    id,
                                    from: _,
                                    digest,
                                } if id == read.id => read.digest = Some(digest),
                                Internal::RepairAck { id, from } if id == read.id => {
                                    read.waiting[from] = false
                                }
                                _ => {}
                            }
                            if advance_read(&setup, me, blocking, read) {
                                pending = None;
                            }
                        }
                    }
                }
                continue;
            }
        }
        assert!(pending.is_none(), "clients have one outstanding request");
        let id = next_id;
        next_id += 1;
        pending = Some(match event {
            Req::Write(client, value) => {
                assert_eq!(client, me);
                let cell = Cell {
                    value,
                    timestamp: value,
                };
                for to in 0..3 {
                    exchange(
                        &setup,
                        me,
                        to,
                        Internal::Mutation {
                            coordinator: me,
                            id,
                            patch: [Some(cell); 2],
                        },
                    );
                }
                Pending::Write {
                    id,
                    client,
                    acks: [false; 3],
                }
            }
            Req::ReadA(client) => {
                assert_eq!(client, me);
                start_read(&setup, me, id, client, false)
            }
            Req::ReadAB(client) => {
                assert_eq!(client, me);
                start_read(&setup, me, id, client, true)
            }
        });
    }
}
fn entry(args: &[String]) -> Entry {
    assert!(
        (2..=3).contains(&args.len()),
        "arguments: <q> <blocking|none> [fifo|bag]"
    );
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1);
    let blocking = match args[1].as_str() {
        "blocking" => true,
        "none" => false,
        _ => panic!("unknown strategy"),
    };
    if let Some(model) = args.get(2) {
        assert!(matches!(model.as_str(), "fifo" | "bag"));
    }
    // Delivery is selected by the harness --model; the optional trailing model
    // is accepted for interface compatibility, not implemented by the protocol.
    Arc::new(move || {
        let mut replicas = Vec::new();
        for i in 0..3 {
            replicas.push(
                thread::Builder::new()
                    .name(format!("n{i}"))
                    .spawn(move || node(i, blocking))
                    .unwrap(),
            );
        }
        let nodes: [ThreadId; 3] = std::array::from_fn(|i| replicas[i].thread().id());
        let c0 = thread::Builder::new()
            .name("c0".into())
            .spawn(move || client(0, q, nodes[0]))
            .unwrap();
        let c0_id = c0.thread().id();
        let c1 = thread::Builder::new()
            .name("c1".into())
            .spawn(move || {
                let setup = Setup {
                    nodes,
                    clients: [c0_id, thread::current().id()],
                };
                traceforge::send_tagged_msg_as(
                    nodes[2],
                    traceforge::Visibility::Invisible,
                    SETUP,
                    setup,
                );
                client(1, q, nodes[1]);
            })
            .unwrap();
        c0.join().unwrap();
        c1.join().unwrap();
        for replica in replicas {
            replica.join().unwrap();
        }
    })
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".into(), "c1".into()],
        spec: &entry,
        imp: &entry,
    })
}
