//! Exercise Portfu's separately tracked background inventory and real task result.

use super::*;

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Error,
    Panic,
}

struct BackgroundProbe {
    entered: Semaphore,
    release: Semaphore,
    settled: Semaphore,
    completed: AtomicBool,
    outcome: Outcome,
}

struct Settled(Arc<BackgroundProbe>);

impl Drop for Settled {
    fn drop(&mut self) {
        self.0.settled.add_permits(1);
    }
}

#[portfu::prelude::task]
pub async fn held_background(server: State<Server>) -> Result<(), PortfuError> {
    let probe = server
        .0
        .scoped_state
        .read()
        .await
        .get("default")
        .and_then(|state| state.get::<Arc<BackgroundProbe>>())
        .cloned();
    let Some(probe) = probe else {
        return Ok(());
    };
    let _settled = Settled(probe.clone());
    probe.entered.add_permits(1);
    probe.release.acquire().await.unwrap().forget();
    match probe.outcome {
        Outcome::Success => {
            probe.completed.store(true, Ordering::Release);
            Ok(())
        }
        Outcome::Error => Err(PortfuError::Internal("injected background failure".into())),
        Outcome::Panic => panic!("injected background panic"),
    }
}

async fn background_fixture(outcome: Outcome) -> (Server, ServerHandle, Arc<BackgroundProbe>) {
    let (server, handle, _port, _request) = fixture(Duration::from_secs(5));
    let probe = Arc::new(BackgroundProbe {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        settled: Semaphore::new(0),
        completed: AtomicBool::new(false),
        outcome,
    });
    server
        .scoped_state
        .write()
        .await
        .get_mut("default")
        .unwrap()
        .insert(probe.clone());
    (server, handle, probe)
}

#[tokio::test]
async fn portfu_joins_admitted_background_work_before_success() {
    let (server, handle, probe) = background_fixture(Outcome::Success).await;
    let mut serving = tokio::spawn(server.run());
    tokio::time::timeout(Duration::from_secs(10), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    handle.shutdown();
    let early = tokio::time::timeout(Duration::from_millis(100), &mut serving).await;
    let returned_before_work = early.is_ok();
    probe.release.add_permits(1);
    let outcome = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(10), serving)
            .await
            .unwrap(),
    }
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), probe.settled.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        !returned_before_work,
        "Portfu returned before admitted background work completed"
    );
    assert!(
        probe.completed.load(Ordering::Acquire),
        "Portfu cancelled admitted background work"
    );
    outcome.unwrap();
}

async fn failed_background(outcome: Outcome) {
    let (server, handle, probe) = background_fixture(outcome).await;
    let serving = tokio::spawn(server.run());
    tokio::time::timeout(Duration::from_secs(10), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    probe.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), probe.settled.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    handle.shutdown();
    let result = tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_err(),
        "Portfu discarded the registered background worker's failure"
    );
}

#[tokio::test]
async fn portfu_retains_a_background_error() {
    failed_background(Outcome::Error).await;
}

#[tokio::test]
async fn portfu_retains_a_background_panic() {
    failed_background(Outcome::Panic).await;
}
