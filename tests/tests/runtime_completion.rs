//! Checked completion through real HTTP, WebSocket and registered background work.
//! Panic, timeout and incomplete handlers must prevent successful shutdown.

use portfu::prelude::{PortfuError, Response, Server, ServerBuilder, ServerHandle, State};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

#[path = "runtime_completion/background.rs"]
mod background;
#[path = "runtime_completion/ownership.rs"]
mod ownership;
#[path = "runtime_completion/websocket.rs"]
mod websocket;

pub struct HeldRequest {
    entered: Semaphore,
    release: Semaphore,
    completed: AtomicBool,
    cancelled: AtomicBool,
    panic_on_release: AtomicBool,
}

impl Default for HeldRequest {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            completed: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            panic_on_release: AtomicBool::new(false),
        }
    }
}

struct HandlerCompletion(Arc<HeldRequest>);

impl Drop for HandlerCompletion {
    fn drop(&mut self) {
        if !self.0.completed.load(Ordering::Acquire) {
            self.0.cancelled.store(true, Ordering::Release);
        }
    }
}

#[portfu::prelude::get("/__held_http")]
pub async fn held_request(probe: State<HeldRequest>) -> Result<Response, PortfuError> {
    let _completion = HandlerCompletion(probe.0.clone());
    probe.0.entered.add_permits(1);
    probe.0.release.acquire().await.unwrap().forget();
    assert!(
        !probe.0.panic_on_release.load(Ordering::Acquire),
        "injected HTTP handler panic"
    );
    probe.0.completed.store(true, Ordering::Release);
    Ok(Response::from_status_and_message(
        http::StatusCode::OK,
        "held response completed",
    ))
}

fn fixture(grace: Duration) -> (Server, ServerHandle, u16, Arc<HeldRequest>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let probe = Arc::new(HeldRequest::default());
    let server = ServerBuilder::new()
        .host("127.0.0.1")
        .port(port)
        .shutdown_grace_period(grace)
        .global_state::<HeldRequest>(probe.clone())
        .build();
    let handle = server.handle();
    (server, handle, port, probe)
}

async fn connect(port: u16) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("test HTTP listener did not open")
}

async fn request(port: u16) -> std::io::Result<Vec<u8>> {
    let mut stream = connect(port).await;
    stream
        .write_all(b"GET /__held_http HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response)
}

#[tokio::test]
async fn grace_timeout_cannot_be_reported_as_completion() {
    let (server, handle, port, probe) = fixture(Duration::from_millis(50));
    let serving = tokio::spawn(server.run());
    let client = tokio::spawn(request(port));
    tokio::time::timeout(Duration::from_secs(10), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    handle.shutdown();
    let outcome = tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap();
    let _response = tokio::time::timeout(Duration::from_secs(10), client)
        .await
        .unwrap()
        .unwrap();
    assert!(!probe.completed.load(Ordering::Acquire));
    assert!(probe.cancelled.load(Ordering::Acquire));
    assert!(
        outcome.is_err(),
        "grace deadline cancelled an admitted response but returned success"
    );
}

#[tokio::test]
async fn an_observed_http_worker_panic_remains_failed() {
    let (server, handle, port, probe) = fixture(Duration::from_secs(5));
    probe.panic_on_release.store(true, Ordering::Release);
    let serving = tokio::spawn(server.run());
    let client = tokio::spawn(request(port));
    tokio::time::timeout(Duration::from_secs(10), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    probe.release.add_permits(1);
    let _response = tokio::time::timeout(Duration::from_secs(10), client)
        .await
        .unwrap()
        .unwrap();
    assert!(probe.cancelled.load(Ordering::Acquire));
    assert!(!probe.completed.load(Ordering::Acquire));
    handle.shutdown();
    let outcome = tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(
        outcome.is_err(),
        "HTTP worker panic disappeared before the drain receipt"
    );
}

#[tokio::test]
async fn an_empty_listener_can_drain_and_close() {
    let (server, handle, port, _probe) = fixture(Duration::from_secs(5));
    let serving = tokio::spawn(server.run());
    drop(connect(port).await);
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(TcpStream::connect(("127.0.0.1", port)).await.is_err());
}
