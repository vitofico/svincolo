use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

use crate::config::{Config, FileInputs};
use crate::queue::Queue;
use crate::rate::Account;

#[derive(Default)]
pub struct Capacity {
    used: AtomicUsize,
    pub rate: Account,
    pub queue: Queue,
    pub changed: Notify,
}

impl Capacity {
    pub fn available(&self, limit: usize) -> bool {
        self.used.load(Ordering::Relaxed) < limit
    }

    pub fn reserve(self: &Arc<Self>, count: usize, limit: usize) -> Option<Reservation> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(count).filter(|total| *total <= limit)
            })
            .ok()?;
        Some(Reservation {
            capacity: Arc::clone(self),
            count,
        })
    }
}

pub struct Reservation {
    capacity: Arc<Capacity>,
    count: usize,
}

impl Reservation {
    pub fn grow_to(&mut self, count: usize, limit: usize) -> bool {
        assert!(count >= self.count);
        if count == self.count {
            return true;
        }
        let added = count - self.count;
        let grew = self
            .capacity
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(added).filter(|total| *total <= limit)
            })
            .is_ok();
        if grew {
            self.count = count;
        }
        grew
    }

    pub fn shrink_to(&mut self, count: usize) {
        assert!(count <= self.count);
        if count < self.count {
            self.capacity
                .used
                .fetch_sub(self.count - count, Ordering::Relaxed);
            self.count = count;
            self.capacity.changed.notify_waiters();
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.capacity.used.fetch_sub(self.count, Ordering::Relaxed);
        self.capacity.changed.notify_waiters();
    }
}

pub struct Snapshot {
    pub config: Config,
    pub callers: HashMap<String, Arc<Capacity>>,
    pub upstreams: HashMap<String, Arc<Capacity>>,
    pub cloud: CancellationToken,
    pub generation: u64,
    pub published_at: Instant,
    pub upload_queue: bool,
}

#[derive(Default)]
pub struct Admission {
    callers: HashMap<String, Weak<Capacity>>,
    upstreams: HashMap<String, Weak<Capacity>>,
    generation: u64,
}

impl Admission {
    pub fn snapshot(&mut self, config: Config, cloud: CancellationToken) -> Arc<Snapshot> {
        self.generation += 1;
        self.callers.retain(|_, counter| counter.strong_count() > 0);
        self.upstreams
            .retain(|_, counter| counter.strong_count() > 0);
        Arc::new(Snapshot {
            callers: config
                .callers
                .iter()
                .map(|caller| {
                    (
                        caller.name.clone(),
                        capacity(&mut self.callers, &caller.name),
                    )
                })
                .collect(),
            upstreams: config
                .routes
                .values()
                .map(|route| route.upstream.name.as_str())
                .map(|name| (name.to_owned(), capacity(&mut self.upstreams, name)))
                .collect(),
            upload_queue: config
                .routes
                .values()
                .any(|route| route.upstream.queue_limit > 0),
            config,
            cloud,
            generation: self.generation,
            published_at: Instant::now(),
        })
    }
}

fn capacity(registry: &mut HashMap<String, Weak<Capacity>>, name: &str) -> Arc<Capacity> {
    if let Some(capacity) = registry.get(name).and_then(Weak::upgrade) {
        return capacity;
    }
    let capacity = Arc::new(Capacity::default());
    registry.insert(name.to_owned(), Arc::downgrade(&capacity));
    capacity
}

pub struct ReloadSignal {
    #[cfg(unix)]
    hangup: tokio::signal::unix::Signal,
}

impl ReloadSignal {
    pub fn new() -> std::io::Result<Self> {
        #[cfg(unix)]
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            #[cfg(unix)]
            hangup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        self.hangup.recv().await;
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    }
}

pub async fn reload(
    path: PathBuf,
    mut inputs: Arc<FileInputs>,
    mut admission: Admission,
    current: &watch::Sender<Arc<Snapshot>>,
    mut signal: ReloadSignal,
) {
    let interval = Duration::from_secs(10);
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_error = None;
    loop {
        tokio::select! {
            _ = ticks.tick() => {},
            () = signal.recv() => {},
        }
        let files = Arc::clone(&inputs);
        let path = path.clone();
        let candidate = tokio::task::spawn_blocking(move || {
            if files.changed() {
                Config::load_snapshot(&path).map(Some)
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap_or_else(|_| Err("configuration reader failed".into()));
        let previous = Arc::clone(&current.borrow());
        let (next, loaded) = match candidate {
            Ok(Some((config, files))) if config.listen == previous.config.listen => (config, files),
            Ok(None) => {
                last_error = None;
                continue;
            }
            other => {
                let error = match other {
                    Err(error) => error,
                    _ => "server.listen changes require restart".into(),
                };
                if last_error.as_ref() != Some(&error) {
                    eprintln!("svincolo: configuration reload rejected: {error}");
                    last_error = Some(error);
                }
                continue;
            }
        };
        let cloud = if previous.config.local_only && !next.local_only {
            CancellationToken::new()
        } else {
            previous.cloud.clone()
        };
        if next.local_only {
            cloud.cancel();
        }
        current.send_replace(admission.snapshot(next, cloud));
        inputs = Arc::new(loaded);
        last_error = None;
        eprintln!("svincolo: configuration reloaded");
    }
}
