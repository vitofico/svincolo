use std::{
    io::{self, IoSlice},
    net::Shutdown,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Duration,
};

use futures_util::FutureExt;
use socket2::SockRef;
use tokio::{
    io::{AsyncRead, AsyncWrite, Interest, ReadBuf, Ready},
    net::TcpStream,
    time::sleep,
};

// Hyper owns the I/O. Admission can observe the same socket without reading
// into the HTTP stream or duplicating its file descriptor.
#[derive(Clone)]
pub struct ClientSocket(Arc<TcpStream>);

impl ClientSocket {
    pub fn new(stream: TcpStream) -> Self {
        Self(Arc::new(stream))
    }

    pub fn is_closed(&self) -> bool {
        self.0
            .ready(Interest::READABLE | Interest::ERROR)
            .now_or_never()
            .is_some_and(closed)
    }

    pub async fn closed(&self) {
        // On Linux priority interest includes read-closed events without
        // waking for ordinary readable bytes.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let interest = Interest::PRIORITY;
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let interest = Interest::READABLE;
        while !closed(self.0.ready(interest | Interest::ERROR).await) {
            // Pending bytes can leave readiness set. Let Hyper consume them
            // or observe a later close without spinning on the readiness flag.
            sleep(Duration::from_millis(20)).await;
        }
    }

    fn poll_write_ready(
        &self,
        cx: &mut Context<'_>,
        write: impl Fn(&TcpStream) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match write(&self.0) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
        }
    }
}

fn closed(event: io::Result<Ready>) -> bool {
    event.map_or(true, |ready| ready.is_read_closed() || ready.is_error())
}

impl AsyncRead for ClientSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            ready!(self.0.poll_read_ready(cx))?;
            match self.0.try_read_buf(buf) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result.map(|_| ())),
            }
        }
    }
}

impl AsyncWrite for ClientSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_ready(cx, |socket| socket.try_write(buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_ready(cx, |socket| socket.try_write_vectored(bufs))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(SockRef::from(self.0.as_ref()).shutdown(Shutdown::Write))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };

    #[tokio::test]
    async fn observes_read_closure_without_consuming_pending_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = ClientSocket::new(stream);
        let payload = [42; 512];
        peer.write_all(&payload).await.unwrap();
        assert!(
            timeout(Duration::from_millis(50), socket.closed())
                .await
                .is_err()
        );
        drop(peer);
        timeout(Duration::from_secs(2), socket.closed())
            .await
            .unwrap();
        assert!(socket.is_closed());
        let mut remaining = [0; 512];
        socket.read_exact(&mut remaining).await.unwrap();
        assert_eq!(remaining, payload);
    }
}
