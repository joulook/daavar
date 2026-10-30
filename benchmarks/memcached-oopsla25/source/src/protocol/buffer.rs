use std::io::{self, ErrorKind, Read};
use std::net::TcpStream;
use prusti_contracts::*;

pub struct BufferClient {
    pub stream: TcpStream,

    read_buf: Vec<u8>,
    read_start: usize,
    read_end: usize,
}

impl BufferClient {
    #[trusted]
    pub fn new(stream: TcpStream) -> BufferClient {
        BufferClient {
            stream,
            read_buf: vec![0u8; 16*1024],
            read_start: 0,
            read_end: 0,
        }
    }

    #[trusted]
    pub fn read_buf(&self) -> &[u8] {
        &self.read_buf[self.read_start..self.read_end]
    }

    #[trusted]
    pub fn read_advance(&mut self, len: usize) {
        self.read_start += len;
    }

    #[trusted]
    pub fn read_into_buffer(&mut self) -> io::Result<()> {
        if self.read_start != 0 {
            // Move existing data to start of buffer
            self.read_buf.copy_within(self.read_start..self.read_end, 0);
            self.read_end -= self.read_start;
            self.read_start = 0;
        }
        if self.read_end == self.read_buf.len() {
            return Err(io::Error::new(ErrorKind::Other, "Read buffer overflow."));
        }

        // Read data
        loop {
            match self.stream.read(&mut self.read_buf[self.read_end..]) {
                Err(err) => {
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    } else {
                        return Err(err);
                    }
                }
                Ok(0) => {
                    return Err(io::Error::new(ErrorKind::Other, "Client ended the connection."));
                }
                Ok(n) => {
                    self.read_end += n;
                    return Ok(());
                }
            }
        }
    }

    /// Read data into buf until it is full.
    /// Data is first taken from the read buffer, then directly read into buf.
    #[trusted]
    pub fn read_into(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let buf_pos = std::cmp::min(buf.len(), self.read_buf().len());
        if buf_pos != 0 {
            buf[..buf_pos].copy_from_slice(&self.read_buf()[..buf_pos]);
            self.read_advance(buf_pos);
        }
        if buf_pos != buf.len() {
            self.stream.read_exact(&mut buf[buf_pos..])?;
        }
        return Ok(());
    }
}
