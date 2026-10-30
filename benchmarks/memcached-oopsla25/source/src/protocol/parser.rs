use prusti_contracts::*;

use crate::protocol::message::{
    Command,
    DeleteCommand,
    GetCommand,
    SetCommand,
};
use crate::storage::{KEY_MAX_LEN, VALUE_MAX_LEN};

#[trusted]
pub fn parse_command(input: &[u8]) -> Result<Command, ()> {
    let input = consume_spaces(input);
    let (input, cmd) = parse_string(input)?;
    match cmd {
        b"set" => Ok(Command::Set(parse_set(input)?)),
        b"get" => Ok(Command::Get(parse_get(input)?)),
        b"delete" => Ok(Command::Delete(parse_delete(input)?)),
        _ => Err(()),
    }
}

#[trusted]
fn parse_set(input: &[u8]) -> Result<SetCommand, ()> {
    let input = consume_spaces(input);
    let (input, key) = parse_key(input)?;
    let input = consume_spaces(input);
    let (input, flags) = parse_u32(input)?;
    let input = consume_spaces(input);
    let (input, expiry) = parse_i64(input)?;
    let input = consume_spaces(input);
    let (input, value_len) = parse_u32(input)?;
    let input = consume_spaces(input);
    if input.len() != 0 {
        return Err(());
    }
    if flags != 0 {
        return Err(()); // flags unsupported
    }
    if value_len as usize > VALUE_MAX_LEN {
        return Err(());
    }
    Ok(SetCommand {
        key: key.to_owned().into_boxed_slice(),
        value: vec![0u8; value_len as usize].into_boxed_slice(),
        expiry,
    })
}

#[trusted]
fn parse_get(input: &[u8]) -> Result<GetCommand, ()> {
    let input = consume_spaces(input);
    let (input, key) = parse_key(input)?;
    let input = consume_spaces(input);
    if input.len() != 0 {
        // Multiple keys unsupported
        return Err(());
    }
    Ok(GetCommand {
        key: key.to_owned().into_boxed_slice(),
    })
}

#[trusted]
fn parse_delete(input: &[u8]) -> Result<DeleteCommand, ()> {
    let input = consume_spaces(input);
    let (input, key) = parse_key(input)?;
    let input = consume_spaces(input);
    if input.len() != 0 {
        return Err(());
    }
    Ok(DeleteCommand {
        key: key.to_owned().into_boxed_slice(),
    })
}

#[trusted]
fn parse_string(input: &[u8]) -> Result<(&[u8], &[u8]), ()> {
    let mut len: usize = 0;
    while len != input.len() && input[len] != b' ' {
        len += 1;
    }
    if len == 0 {
        return Err(());
    }
    Ok((&input[len..], &input[..len]))
}

#[trusted]
fn parse_key(input: &[u8]) -> Result<(&[u8], &[u8]), ()> {
    let (input, string) = parse_string(input)?;
    if string.len() > KEY_MAX_LEN {
        return Err(());
    }
    Ok((input, string))
}

#[trusted]
fn parse_u64(input: &[u8]) -> Result<(&[u8], u64), ()> {
    let mut val_len = 0;
    let mut val: u64 = 0;
    while val_len != input.len() {
        match input[val_len] {
            b' ' => {
                break;
            }
            n @ b'0'..=b'9' => {
                val = val
                    .checked_mul(10).ok_or(())?
                    .checked_add((n - b'0').into()).ok_or(())?;
            }
            _ => {
                return Err(());
            }
        }
        val_len += 1;
    }
    if val_len == 0 {
        return Err(());
    }
    return Ok((&input[val_len..], val));
}

#[trusted]
fn parse_u32(input: &[u8]) -> Result<(&[u8], u32), ()> {
    let (input, num) = parse_u64(input)?;
    Ok((input, num.try_into().map_err(|_| ())?))
}

#[trusted]
fn parse_i64(mut input: &[u8]) -> Result<(&[u8], i64), ()> {
    let mut negative = false;
    if input.len() == 0 {
        return Err(());
    }
    if input[0] == b'-' {
        input = &input[1..];
        negative = true;
    }
    let (input, num) = parse_u64(input)?;
    let mut num: i128 = num.into();
    if negative {
        num = -num;
    }
    let num: i64 = num.try_into().map_err(|_| ())?;
    Ok((input, num))
}

#[trusted]
fn consume_spaces(mut input: &[u8]) -> &[u8] {
    while input.len() != 0 && input[0] == b' ' {
        input = &input[1..];
    }
    input
}
