use prusti_contracts::*;

use crate::model::message::{AbsCommand, AbsResponse};
use crate::proof_utils::slice_to_seq;

pub enum Command {
    Set(SetCommand),
    Get(GetCommand),
    Delete(DeleteCommand),
}

#[allow(unused)]
pub struct SetCommand {
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
    pub expiry: i64,
}

pub struct GetCommand {
    pub key: Box<[u8]>,
}

pub struct DeleteCommand {
    pub key: Box<[u8]>,
}

pub enum Response {
    Set,
    Get { key: Box<[u8]>, value: Option<Box<[u8]>> },
    Delete { found: bool },
}

impl Command {
    #[pure]
    pub fn as_abs(&self) -> AbsCommand {
        match self {
            Command::Set(SetCommand { key, value, expiry }) =>
                AbsCommand::Set {
                    key: slice_to_seq(key),
                    value: slice_to_seq(value),
                    expiry: *expiry,
                },
            Command::Get(GetCommand { key }) =>
                AbsCommand::Get { key: slice_to_seq(key) },
            Command::Delete(DeleteCommand { key }) =>
                AbsCommand::Delete { key: slice_to_seq(key) },
        }
    }
}

impl Response {
    #[pure]
    pub fn as_abs(&self) -> AbsResponse {
        match self {
            Response::Set => AbsResponse::Set,
            Response::Get { key, value } =>
                AbsResponse::Get {
                    key: slice_to_seq(key),
                    value: match value {
                        Some(value) => Some(slice_to_seq(value)),
                        None => None,
                    }
                },
            Response::Delete { found } =>
                AbsResponse::Delete { found: *found },
        }
    }
}
