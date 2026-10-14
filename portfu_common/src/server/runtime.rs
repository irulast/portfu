use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub(crate) struct ServerRuntime {
    graceful: CancellationToken,
    force: CancellationToken,
    failed: Arc<AtomicBool>,
    http_connections: TaskTracker,
    #[cfg(feature = "websocket")]
    websocket_connections: TaskTracker,
    background_tasks: TaskTracker,
}

impl Default for ServerRuntime {
    fn default() -> Self {
        Self {
            graceful: CancellationToken::new(),
            force: CancellationToken::new(),
            failed: Arc::new(AtomicBool::new(false)),
            http_connections: TaskTracker::new(),
            #[cfg(feature = "websocket")]
            websocket_connections: TaskTracker::new(),
            background_tasks: TaskTracker::new(),
        }
    }
}

impl ServerRuntime {
    pub fn cancellation(&self) -> CancellationToken {
        self.graceful.clone()
    }

    #[cfg(feature = "websocket")]
    pub fn is_shutting_down(&self) -> bool {
        self.graceful.is_cancelled()
    }

    pub fn spawn_http<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked(&self.http_connections, task);
    }

    #[cfg(feature = "websocket")]
    pub fn spawn_websocket<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked(&self.websocket_connections, task);
    }

    pub fn spawn_background<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Graceful stop requests cooperation; it does not abandon a callback
        // that has already started. Framework intervals wake between callbacks.
        self.spawn_tracked(&self.background_tasks, task);
    }

    pub fn record_failure(&self) {
        self.failed.store(true, Ordering::Release);
    }

    pub fn begin_shutdown(&self) {
        self.graceful.cancel();
    }

    pub fn force_shutdown(&self) {
        self.record_failure();
        self.begin_shutdown();
        self.close_trackers();
        self.force.cancel();
    }

    pub async fn drain(&self, grace_period: Duration) -> bool {
        self.begin_shutdown();
        self.http_connections.close();
        self.background_tasks.close();
        let drained = timeout(grace_period, self.wait_all()).await.is_ok();
        if !drained {
            self.force_shutdown();
            // Cancellation is a request, not a completion receipt. Retain the
            // drain until every admitted future has actually ended or dropped.
            self.wait_all().await;
        }
        drained && !self.failed.load(Ordering::Acquire)
    }

    async fn wait_all(&self) {
        // An HTTP upgrade can add a WebSocket until its HTTP worker has ended.
        self.http_connections.wait().await;
        #[cfg(feature = "websocket")]
        {
            self.websocket_connections.close();
            self.websocket_connections.wait().await;
        }
        self.background_tasks.wait().await;
    }

    fn close_trackers(&self) {
        self.http_connections.close();
        #[cfg(feature = "websocket")]
        self.websocket_connections.close();
        self.background_tasks.close();
    }

    fn spawn_tracked<F>(&self, tracker: &TaskTracker, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let force = self.force.clone();
        // Capture before spawning, including an admitted future that is never
        // polled. Panic or cancellation permanently fails this runtime's proof.
        let completion = WorkerCompletion {
            failed: self.failed.clone(),
            completed: false,
        };
        tracker.spawn(async move {
            let mut completion = completion;
            let completed = {
                // Destroy the application future before issuing its receipt;
                // a destructor panic must still leave the guard armed.
                let task = task;
                tokio::pin!(task);
                tokio::select! {
                    _ = force.cancelled() => false,
                    _ = &mut task => true,
                }
            };
            completion.completed = completed;
        });
    }
}

struct WorkerCompletion {
    failed: Arc<AtomicBool>,
    completed: bool,
}

impl Drop for WorkerCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.failed.store(true, Ordering::Release);
        }
    }
}
