//! Keep an admitted WebSocket handler owned across graceful server shutdown.
//! The existing upgrade implementation and wire format are unchanged.

use super::*;
use portfu::prelude::WebSocket;

#[portfu::prelude::websocket("/__held_socket")]
pub async fn held_socket(_socket: WebSocket, probe: State<HeldRequest>) -> Result<(), PortfuError> {
    let _completion = HandlerCompletion(probe.0.clone());
    probe.0.entered.add_permits(1);
    probe.0.release.acquire().await.unwrap().forget();
    probe.0.completed.store(true, Ordering::Release);
    Ok(())
}

async fn upgraded_socket(port: u16) -> TcpStream {
    let mut socket = connect(port).await;
    socket
        .write_all(
            concat!(
                "GET /__held_socket HTTP/1.1\r\nHost: localhost\r\n",
                "Upgrade: websocket\r\nConnection: Upgrade\r\n",
                "Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\n",
                "Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(10), async {
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            assert!(
                response.len() < 8192,
                "upgrade headers exceeded the fixture bound"
            );
            response.push(socket.read_u8().await.unwrap());
        }
        response
    })
    .await
    .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 101 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
    socket
}

#[tokio::test]
async fn portfu_joins_an_admitted_websocket_handler_before_success() {
    let (server, handle, port, probe) = fixture(Duration::from_secs(5));
    let mut serving = tokio::spawn(server.run());
    let mut client = upgraded_socket(port).await;
    tokio::time::timeout(Duration::from_secs(10), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    handle.shutdown();
    let early = tokio::time::timeout(Duration::from_millis(100), &mut serving).await;
    let returned_before_handler = early.is_ok();
    probe.release.add_permits(1);
    let outcome = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(10), serving)
            .await
            .unwrap(),
    }
    .unwrap();
    let mut final_bytes = Vec::new();
    let _read = tokio::time::timeout(
        Duration::from_secs(10),
        client.read_to_end(&mut final_bytes),
    )
    .await
    .unwrap();
    assert!(
        !returned_before_handler,
        "Portfu reported completion before its admitted WebSocket handler settled"
    );
    assert!(probe.completed.load(Ordering::Acquire));
    assert!(!probe.cancelled.load(Ordering::Acquire));
    outcome.unwrap();
}
