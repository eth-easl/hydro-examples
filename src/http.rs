//! Reusable Axum ↔ Hydro `sidecar_bidi` bridge.
//!
//! Applications own routes, extractors, validation, and response formatting.
//! Each request carries its own reply channel through the local dataflow.
use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, PollSender};

use hydro_lang::forward_handle::ForwardHandle;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use stageleft::QuotedWithContext;

/// The typed Hydro boundary for an HTTP service on one process.
pub type Endpoint<'a, I, O, P> = (
    Stream<Message<I, O>, Process<'a, P>>,
    ForwardHandle<'a, Stream<Message<O, O>, Process<'a, P>, Unbounded, NoOrder>>,
);

/// Attach an HTTP server to a Hydro process using `sidecar_bidi`.
/// The quoted router factory runs on the deployed process. Complete the returned
/// response handle with replies produced using `Message::map`.
/// Address/configuration are captured at planning time; binding happens at startup.
pub fn serve<'a, I: Send + 'static, O: Send + 'static, P: 'a, F>(
    process: &Process<'a, P>,
    address: SocketAddr,
    config: Config,
    router: impl QuotedWithContext<'a, F, Process<'a, P>>,
) -> Endpoint<'a, I, O, P>
where
    F: FnOnce(Bridge<I, O>) -> Router + 'static,
{
    assert!(config.capacity > 0, "HTTP bridge capacity must be positive");
    let address = address.to_string();
    let capacity = config.capacity;
    let timeout = config.timeout;
    process.sidecar_bidi(q!(move || {
        // Stageleft rewrites this constructor path correctly inside the quote.
        let mut config = self::Config::default();
        config.capacity = capacity;
        config.timeout = timeout;
        self::start(
            address.parse().expect("validated bind address"),
            config,
            router,
        )
        .expect("bind HTTP endpoint")
    }))
}

/// A local request and its reply channel. Move it through the dataflow with `map`.
/// This envelope is intentionally neither cloneable nor serializable.
#[derive(Debug)]
pub struct Message<T, O> {
    pub payload: T,
    reply: oneshot::Sender<O>,
}
impl<T, O> Message<T, O> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Message<U, O> {
        Message {
            payload: f(self.payload),
            reply: self.reply,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Maximum queued items in each sidecar channel, not total in-flight work.
    pub capacity: usize,
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: 64,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Overloaded,
    Unavailable,
    Timeout,
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Self::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "request queue full"),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "processing stream unavailable",
            ),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "request timed out"),
        }
        .into_response()
    }
}

/// Cloneable Axum state. Payloads need not implement Clone or Serialize.
pub struct Bridge<I, O> {
    requests: mpsc::Sender<Message<I, O>>,
    timeout: Duration,
    closed: CancellationToken,
}
impl<I, O> Clone for Bridge<I, O> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            timeout: self.timeout,
            closed: self.closed.clone(),
        }
    }
}
impl<I, O> Bridge<I, O> {
    /// Submit work without waiting for queue space, then await its reply.
    /// Timeout/cancellation drops the receiver; already submitted work continues.
    pub async fn call(&self, payload: I) -> Result<O, Error> {
        if self.closed.is_cancelled() {
            return Err(Error::Unavailable);
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .try_send(Message { payload, reply })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Overloaded,
                mpsc::error::TrySendError::Closed(_) => Error::Unavailable,
            })?;
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => Err(Error::Unavailable),
            result = tokio::time::timeout(self.timeout, response) => {
                result.map_err(|_| Error::Timeout)?.map_err(|_| Error::Unavailable)
            }
        }
    }
}

pub type Channels<I, O> = (ReceiverStream<Message<I, O>>, PollSender<Message<O, O>>);

/// Create the bridge and dataflow channels for an application-managed server.
pub fn connect<I: Send + 'static, O: Send + 'static>(
    config: Config,
) -> (Bridge<I, O>, Channels<I, O>) {
    let (requests, incoming) = mpsc::channel(config.capacity);
    let (outgoing, mut responses) = mpsc::channel::<Message<O, O>>(config.capacity);
    let closed = CancellationToken::new();
    let bridge = Bridge {
        requests,
        timeout: config.timeout,
        closed: closed.clone(),
    };
    tokio::spawn(async move {
        while let Some(response) = responses.recv().await {
            // A disconnected or timed-out HTTP caller no longer needs the reply.
            let _ = response.reply.send(response.payload);
        }
        closed.cancel();
    });
    (
        bridge,
        (ReceiverStream::new(incoming), PollSender::new(outgoing)),
    )
}

/// Call inside a `sidecar_bidi` initializer. Supply an application-owned Router;
/// body limits, paths, methods, health endpoints, and HTTP codecs stay in the app.
/// Requires a running Tokio runtime. Bind errors are returned synchronously.
pub fn start<I: Send + 'static, O: Send + 'static>(
    address: SocketAddr,
    config: Config,
    router: impl FnOnce(Bridge<I, O>) -> Router,
) -> std::io::Result<Channels<I, O>> {
    let listener = std::net::TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let (bridge, channels) = connect(config);
    let router = router(bridge);
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("HTTP server failed");
    });
    eprintln!("HTTP listening on http://{address}");
    Ok(channels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn correlates_out_of_order_replies() {
        // Different, non-Clone payload types demonstrate application independence.
        struct Input(u32);
        struct Output(String);
        let (bridge, (mut incoming, outgoing)) = connect::<Input, Output>(Config::default());
        let a = bridge.clone();
        let first = tokio::spawn(async move { a.call(Input(1)).await.unwrap() });
        let one = incoming.next().await.unwrap();
        let b = bridge.clone();
        let second = tokio::spawn(async move { b.call(Input(2)).await.unwrap() });
        let two = incoming.next().await.unwrap();
        let sender = outgoing.get_ref().unwrap();
        sender
            .send(two.map(|x| Output(x.0.to_string())))
            .await
            .unwrap();
        sender
            .send(one.map(|x| Output(x.0.to_string())))
            .await
            .unwrap();
        assert_eq!(first.await.unwrap().0, "1");
        assert_eq!(second.await.unwrap().0, "2");
    }

    #[tokio::test]
    async fn full_queue_rejects_new_work() {
        let (bridge, (_incoming, _outgoing)) = connect::<(), ()>(Config {
            capacity: 1,
            timeout: Duration::from_millis(5),
        });
        // A timed-out request remains queued until Hydro consumes it.
        assert_eq!(bridge.call(()).await, Err(Error::Timeout));
        assert_eq!(bridge.call(()).await, Err(Error::Overloaded));
    }

    #[tokio::test]
    async fn closed_input_returns_unavailable() {
        let (bridge, (incoming, _outgoing)) = connect::<(), ()>(Config::default());
        drop(incoming);
        assert_eq!(bridge.call(()).await, Err(Error::Unavailable));
    }

    #[tokio::test]
    async fn closed_output_rejects_pending_and_subsequent_calls() {
        let (bridge, (mut incoming, outgoing)) = connect::<(), ()>(Config {
            capacity: 1,
            timeout: Duration::from_millis(50),
        });
        let b = bridge.clone();
        let task = tokio::spawn(async move { b.call(()).await });
        let _request = incoming.next().await.unwrap();
        drop(outgoing);
        assert_eq!(task.await.unwrap(), Err(Error::Unavailable));
        assert_eq!(bridge.call(()).await, Err(Error::Unavailable));
        assert!(bridge.requests.try_reserve().is_ok());
    }

    #[tokio::test]
    async fn timeout_discards_late_reply() {
        let (bridge, (mut incoming, outgoing)) = connect::<(), ()>(Config {
            timeout: Duration::from_millis(5),
            ..Config::default()
        });
        assert_eq!(bridge.call(()).await, Err(Error::Timeout));
        let late = incoming.next().await.unwrap();
        assert!(late.reply.is_closed());
        outgoing.get_ref().unwrap().send(late).await.unwrap();
        tokio::task::yield_now().await;
        assert!(bridge.requests.try_reserve().is_ok());
    }
}
