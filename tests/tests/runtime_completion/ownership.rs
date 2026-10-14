//! A finished outer server must release the scoped host/store observers it owned.
//! These use the actual Server::run boundary, including startup before admission.

use super::*;

#[tokio::test]
async fn completed_http_server_releases_its_scoped_owners() {
    let (server, handle, port, _probe) = fixture(Duration::from_secs(5));
    let scoped_owners = Arc::downgrade(&server.scoped_state);
    let serving = tokio::spawn(server.run());
    drop(connect(port).await);
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(handle);
    assert!(
        scoped_owners.upgrade().is_none(),
        "completed server retained its scoped host/store owners through a self-reference"
    );
}

#[tokio::test]
async fn refused_http_startup_releases_its_scoped_owners() {
    let (server, handle, port, _probe) = fixture(Duration::from_secs(5));
    // Occupy the selected listener before run: no server worker has been admitted.
    let occupied = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let scoped_owners = Arc::downgrade(&server.scoped_state);
    let result = tokio::time::timeout(Duration::from_secs(10), server.run())
        .await
        .unwrap();
    drop(handle);
    drop(occupied);
    assert!(
        matches!(result, Err(PortfuError::Io(ref error))
        if error.kind() == std::io::ErrorKind::AddrInUse),
        "{result:?}"
    );
    assert!(
        scoped_owners.upgrade().is_none(),
        "listener startup refusal retained its scoped host/store owners"
    );
}
