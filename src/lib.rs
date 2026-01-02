use std::io;
use std::net::ToSocketAddrs;

use compio::buf::BufResult;
use compio::net::TcpListener;
use socket2::{Domain, Protocol, Socket, Type};

pub mod args;
pub mod cache;
pub mod metrics;
pub mod proto;
pub mod server;

pub trait BufResultExt<T, B> {
    fn result(self) -> io::Result<(T, B)>;
}

impl<T, B> BufResultExt<T, B> for BufResult<T, B> {
    fn result(self) -> io::Result<(T, B)> {
        self.0.map(|v| (v, self.1))
    }
}

pub fn create_listener(listen: &str, backlog: i32) -> io::Result<TcpListener> {
    let addr = listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid address"))?;

    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;

    TcpListener::from_std(socket.into())
}
