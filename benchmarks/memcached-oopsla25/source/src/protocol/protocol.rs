use std::io::{self, ErrorKind, Write};
use std::net::{TcpListener, TcpStream};
use prusti_contracts::*;
use prusti_refinement::prelude::*;
use crate::model::{ConId, McStateHandle, MemcacheModel};
use crate::model::state::{AbsState, NetworkConState};
use crate::protocol::buffer::BufferClient;
use crate::protocol::message::{Command, Response};
use crate::protocol::parser::parse_command;
use crate::protocol::serialiser::ResponseLine;

pub struct ProtocolClient {
    client: BufferClient,
}

impl ProtocolClient {
    #[trusted]
    pub fn new(stream: TcpStream) -> ProtocolClient {
        ProtocolClient {
            client: BufferClient::new(stream),
        }
    }

    #[trusted]
    #[pure]
    pub fn id(&self) -> ConId {
        0
    }

    #[trusted]
    #[requires(g.locked())]
    #[requires(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.locked())]
    #[ensures(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.id() === old(g.id()))]
    #[ensures(self.id() === old(self.id()))]
    #[ensures(match result {
        Ok(cmd) =>
            match old(g.state).network_cmd.get(self.id()).split_head() {
                Some((head, tail)) =>
                    head === cmd.as_abs() &&
                    g.state === AbsState {
                        network_cmd: old(g.state).network_cmd.set(self.id(), tail),
                        ..old(g.state)
                    },
                None => false,
            },
        Err(_) =>
            g.state === old(g.state) &&
            g.state.network_state.get(self.id()) === NetworkConState::Closed,
    })]
    pub fn read(&mut self, g: &mut McStateHandle) -> io::Result<Command> {
        let _ = g;
        // Read and scan for newline
        let mut scan_pos = 0;
        loop {
            if scan_pos == self.client.read_buf().len() {
                self.client.read_into_buffer()?;
            } else if self.client.read_buf()[scan_pos] != b'\n' {
                scan_pos += 1;
            } else {
                break;
            }
        }
        let end_pos = if scan_pos != 0 && self.client.read_buf()[scan_pos - 1] == b'\r' {
            scan_pos - 1
        } else {
            scan_pos
        };

        // Parse command
        let mut cmd = match parse_command(&self.client.read_buf()[..end_pos]) {
            Ok(cmd) => cmd,
            Err(()) => {
                let _ = self.client.stream.write(b"CLIENT_ERROR invalid or unsupported command line\r\n");
                return Err(io::Error::new(ErrorKind::Other, "Bad command line."));
            }
        };
        self.client.read_advance(scan_pos + 1);

        match &mut cmd {
            Command::Set(cmd) => {
                // Read value
                self.client.read_into(&mut cmd.value)?;

                // Read CR LF terminator
                while self.client.read_buf().len() < 2 {
                    self.client.read_into_buffer()?;
                }
                if &self.client.read_buf()[..2] != b"\r\n" {
                    let _ = self.client.stream.write(b"CLIENT_ERROR bad data chunk\r\n");
                    return Err(io::Error::new(ErrorKind::Other, "Bad data chunk."));
                }
                self.client.read_advance(2);
            }
            _ => {}
        }

        Ok(cmd)
    }

    #[trusted]
    #[requires(g.locked())]
    #[requires(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.locked())]
    #[ensures(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.id() === old(g.id()))]
    #[ensures(self.id() === old(self.id()))]
    #[ensures(match result {
        Ok(()) => {
            let queue_res = old(g.state).network_res.get(self.id());
            g.state === AbsState {
                network_res: old(g.state).network_res.set(self.id(), queue_res.append(res.as_abs())),
                ..old(g.state)
            }
        }
        Err(_) =>
            g.state === old(g.state) &&
            g.state.network_state.get(self.id()) === NetworkConState::Closed,
    })]
    pub fn write(&mut self, res: Response, g: &mut McStateHandle) -> io::Result<()> {
        let _ = g;
        match res {
            Response::Set => {
                self.client.stream.write_all(b"STORED\r\n")?;
            }
            Response::Get { key, value: Some(value) } => {
                let mut resp = ResponseLine::new();
                resp.put(b"VALUE ");
                resp.put(&key);
                resp.put(b" 0 ");
                resp.put_usize(value.len());
                resp.put(b"\r\n");

                self.client.stream.write_all(resp.buf())?;
                self.client.stream.write_all(&value)?;
                self.client.stream.write_all(b"\r\nEND\r\n")?;
            }
            Response::Get { key: _, value: None} => {
                self.client.stream.write_all(b"END\r\n")?;
            }
            Response::Delete { found: true } => {
                self.client.stream.write_all(b"DELETED\r\n")?;
            }
            Response::Delete { found: false } => {
                self.client.stream.write_all(b"NOT_FOUND\r\n")?;
            }
        }
        Ok(())
    }
}

pub struct ProtocolListener {
    listener: TcpListener,
}

impl ProtocolListener {
    #[trusted]
    pub fn new(listener: TcpListener) -> ProtocolListener {
        ProtocolListener {
            listener,
        }
    }

    #[trusted]
    #[requires(g.locked())]
    #[requires(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.locked())]
    #[ensures(g.env_state() === MemcacheModel::get_env_state(g.state))]
    #[ensures(g.id() === old(g.id()))]
    #[ensures(match result {
        Ok(client) => {
            old(g.state).network_state.get(client.id()) === NetworkConState::Pending &&
            client.id() === old(g.state).network_id &&
            g.state === AbsState {
                network_id: old(g.state).network_id + 1,
                network_state: old(g.state).network_state.set(client.id(), NetworkConState::Established),
                ..old(g.state)
            }
        }
        Err(_) => g.state === old(g.state),
    })]
    pub fn accept(&self, g: &mut McStateHandle) -> io::Result<ProtocolClient> {
        let _ = g;
        let (stream, _addr) = self.listener.accept()?;
        Ok(ProtocolClient::new(stream))
    }
}
