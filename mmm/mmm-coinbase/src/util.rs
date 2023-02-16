use tokio::time::{Duration, Instant};

pub struct RateLimit {
    last_run: Instant,
    interval: Duration,
}

impl RateLimit {
    pub fn new(interval: Duration) -> Self {
        Self {
            last_run: Instant::now() - interval,
            interval,
        }
    }

    pub async fn wait(&mut self) {
        let diff = Instant::now() - self.last_run;
        if diff < self.interval {
            tokio::time::sleep(self.interval - diff).await;
        }
        self.last_run = Instant::now();
    }
}
