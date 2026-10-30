use prusti_contracts::*;

use crate::storage::KEY_MAX_LEN;

pub const RESPONSE_LEN: usize = 100 + KEY_MAX_LEN;

pub struct ResponseLine {
    buf: [u8; RESPONSE_LEN],
    pos: usize,
}

impl ResponseLine {
    #[trusted]
    pub fn new() -> Self {
        ResponseLine {
            buf: [0; RESPONSE_LEN],
            pos: 0,
        }
    }

    #[trusted]
    pub fn buf(&self) -> &[u8] {
        &self.buf[..self.pos]
    }

    #[trusted]
    pub fn put(&mut self, val: &[u8]) {
        self.buf[self.pos..self.pos + val.len()].copy_from_slice(val);
        self.pos += val.len();
    }

    #[trusted]
    pub fn put_usize(&mut self, val: usize) {
        let mut len = 1;
        let mut copy = val;
        while copy >= 10 {
            copy = copy / 10;
            len += 1;
        }
        let mut pos = len - 1;
        let mut copy = val;
        loop {
            self.buf[self.pos + pos] = b'0' + ((copy % 10) as u8);
            copy = copy / 10;
            if pos == 0 {
                break;
            }
            pos -= 1;
        }
        self.pos += len;
    }
}
