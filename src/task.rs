use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[async_trait]
pub trait Task: Send + Sync {
    async fn run(&self);
}

#[derive(Default)]
pub struct TaskRunner {
    tasks: Vec<Arc<dyn Task>>,
    tracker: TaskTracker,
    token: CancellationToken,
}

impl TaskRunner {
    pub fn new(token: CancellationToken) -> Self {
        Self {
            token,
            ..Default::default()
        }
    }

    pub fn add(&mut self, task: Arc<dyn Task>) {
        self.tasks.push(task);
    }

    /// Spawns all tasks
    /// For CPU-bound tasks (like arbitrage detection) consider a blocking thread pool (spawn_blocking)
    pub fn start(&self) {
        for task in &self.tasks {
            let task = Arc::clone(task);
            self.tracker.spawn(async move {
                task.run().await;
            });
        }
    }

    pub async fn shutdown(&self) {
        // Signal all tasks to stop
        self.token.cancel();

        // Stop accepting new tasks
        self.tracker.close();

        // Wait for all running tasks to finish
        self.tracker.wait().await;
    }
}
