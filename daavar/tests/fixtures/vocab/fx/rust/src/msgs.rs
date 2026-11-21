//! The fixture's vocabulary (mirrored by `fx.Fx`): a ping, its pong, a shutdown.

#[derive(Clone, PartialEq, Debug)]
pub enum ToServer {
    Ping { n: u32 },
    Shutdown,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ToClient {
    Pong { n: u32 },
}
