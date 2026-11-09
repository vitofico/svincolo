use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::http::StatusCode;
use tokio::{
    sync::{Notify, watch},
    time::{Instant, sleep_until},
};

use crate::{
    client_socket::ClientSocket,
    config::{OverLimit, Priority},
    rate::{AttemptBudget, RateLimited},
    state::{Capacity, Reservation},
    upstream::Failure,
};

#[derive(Default)]
pub struct Queue {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Default)]
struct State {
    sequence: u64,
    waiting: HashMap<u64, Waiting>,
    running: HashMap<String, usize>,
}

struct Waiting {
    caller: String,
    limit: Option<(Arc<Capacity>, usize)>,
    priority: Priority,
    ready_at: Instant,
    phase: Phase,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Dispatch,
    Upload,
}

struct Ticket<'a> {
    queue: &'a Queue,
    id: u64,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.queue.state.lock().unwrap().waiting.remove(&self.id);
        self.queue.changed.notify_waiters();
    }
}

struct Running {
    upstream: Arc<Capacity>,
    caller: String,
    slot: Option<Reservation>,
}

impl Drop for Running {
    fn drop(&mut self) {
        let queue = &self.upstream.queue;
        let mut state = queue.state.lock().unwrap();
        let active = state
            .running
            .get_mut(&self.caller)
            .expect("registered dispatch");
        *active -= 1;
        if *active == 0 {
            state.running.remove(&self.caller);
        }
        drop(self.slot.take());
        queue.changed.notify_waiters();
    }
}

impl State {
    fn next(&self, now: Instant) -> Option<u64> {
        self.waiting
            .iter()
            .filter(|(_, entry)| {
                entry.ready_at <= now
                    && entry
                        .limit
                        .as_ref()
                        .is_none_or(|(scope, limit)| scope.available(*limit))
            })
            .min_by_key(|(id, entry)| {
                (
                    entry.phase,
                    entry.priority,
                    self.running.get(&entry.caller).copied().unwrap_or(0),
                    **id,
                )
            })
            .map(|(id, _)| *id)
    }
}

#[derive(Clone, Copy)]
pub struct Limit<'a> {
    pub capacity: &'a Arc<Capacity>,
    pub max: usize,
}

impl Limit<'_> {
    fn reserve(self) -> Option<Reservation> {
        self.capacity.reserve(1, self.max)
    }
}

pub struct CallerLimit<'a> {
    pub name: &'a str,
    pub limit: Limit<'a>,
    pub over_limit: OverLimit,
}

#[derive(Default)]
pub struct Slots {
    pub active: Option<Reservation>,
    pub caller: Option<Reservation>,
    running: Option<Running>,
}

impl Slots {
    pub fn upload(active: Reservation, caller: Option<Reservation>) -> Self {
        Self {
            active: Some(active),
            caller,
            ..Self::default()
        }
    }

    pub fn dispatched(&self) -> bool {
        self.running.is_some()
    }
}

pub struct Wait<'a> {
    pub limit: usize,
    pub duration: Duration,
    pub priority: Priority,
    pub budget: &'a Arc<Capacity>,
    pub bytes: usize,
    pub max_bytes: usize,
    pub resumed: Option<(u64, Instant)>,
}

pub struct Dispatch<'a> {
    pub active: Limit<'a>,
    pub caller: Option<CallerLimit<'a>>,
    pub upstream: Limit<'a>,
    pub wait: Wait<'a>,
    pub slots: Slots,
    pub rate: AttemptBudget<'a>,
    pub drain: watch::Receiver<bool>,
    pub peer: &'a ClientSocket,
}

struct Blocked {
    failure: Failure,
    wait: Option<Duration>,
}

impl From<Failure> for Blocked {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            wait: None,
        }
    }
}

impl From<RateLimited> for Blocked {
    fn from(error: RateLimited) -> Self {
        Self {
            wait: Some(error.wait),
            failure: error.into(),
        }
    }
}

impl Dispatch<'_> {
    pub async fn admit(&mut self) -> Result<(), Failure> {
        if self.slots.dispatched() {
            return self.charge();
        }
        self.admit_phase(Phase::Dispatch, || {}).await
    }

    pub async fn resume_upload(&mut self, before_wait: impl FnOnce()) -> Result<(), Failure> {
        self.admit_phase(Phase::Upload, before_wait).await
    }

    async fn admit_phase(
        &mut self,
        phase: Phase,
        before_wait: impl FnOnce(),
    ) -> Result<(), Failure> {
        self.check_drain()?;
        let queue = &self.upstream.capacity.queue;
        let now = Instant::now();
        let deadline = self
            .wait
            .resumed
            .map_or(now + self.wait.duration, |(_, deadline)| deadline);
        let blocked = {
            let mut state = queue.state.lock().unwrap();
            if !state.waiting.values().any(|entry| entry.phase <= phase) {
                match self.try_enter(&mut state, phase) {
                    Ok(()) => return Ok(()),
                    Err(blocked) => blocked,
                }
            } else {
                self.refusing_caller()?;
                UPSTREAM_BUSY.into()
            }
        };
        if self.wait.limit == 0 || self.wait.duration.is_zero() || !self.can_wait(&blocked.failure)
        {
            return Err(blocked.failure);
        }
        if now >= deadline {
            return Err(QUEUE_TIMEOUT);
        }

        let _bytes = self
            .wait
            .budget
            .reserve(self.wait.bytes, self.wait.max_bytes)
            .ok_or(QUEUE_FULL)?;
        before_wait();
        self.slots = Slots::default();
        let ticket = {
            let mut state = queue.state.lock().unwrap();
            if state.waiting.len() >= self.wait.limit {
                return Err(QUEUE_FULL);
            }
            let id = self.wait.resumed.map_or_else(
                || {
                    let id = state.sequence;
                    state.sequence += 1;
                    id
                },
                |(id, _)| id,
            );
            state.waiting.insert(
                id,
                Waiting {
                    caller: self.caller_name().to_owned(),
                    limit: self
                        .caller
                        .as_ref()
                        .map(|caller| (Arc::clone(caller.limit.capacity), caller.limit.max)),
                    priority: self.wait.priority,
                    ready_at: now + blocked.wait.unwrap_or_default(),
                    phase,
                },
            );
            Ticket { queue, id }
        };
        queue.changed.notify_waiters();
        let caller_capacity = self.caller.as_ref().map(|caller| caller.limit.capacity);
        let peer_closed = self.peer.closed();
        tokio::pin!(peer_closed);
        loop {
            let queue_change = queue.changed.notified();
            let global_change = self.active.capacity.changed.notified();
            let caller_change = caller_capacity.map(|capacity| capacity.changed.notified());
            let caller_change = async {
                if let Some(change) = caller_change {
                    change.await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            self.check_drain()?;
            self.refusing_caller()?;
            let now = Instant::now();
            if now >= deadline {
                return Err(QUEUE_TIMEOUT);
            }
            let ready_at = {
                let mut state = queue.state.lock().unwrap();
                if state.next(now) == Some(ticket.id) {
                    match self.try_enter(&mut state, phase) {
                        Ok(()) => {
                            state.waiting.remove(&ticket.id);
                            if phase == Phase::Upload {
                                self.wait.resumed = Some((ticket.id, deadline));
                            }
                            return Ok(());
                        }
                        Err(blocked) if !self.can_wait(&blocked.failure) => {
                            return Err(blocked.failure);
                        }
                        Err(blocked) => {
                            if let Some(wait) = blocked.wait {
                                state.waiting.get_mut(&ticket.id).unwrap().ready_at = now + wait;
                                queue.changed.notify_waiters();
                            }
                        }
                    }
                }
                state.waiting[&ticket.id].ready_at
            };
            let wake_at = if ready_at > now {
                deadline.min(ready_at)
            } else {
                deadline
            };
            tokio::select! {
                biased;
                _ = self.drain.wait_for(|draining| *draining) => return Err(DRAINING),
                () = &mut peer_closed => return Err(DISCONNECTED),
                () = queue_change => {},
                () = global_change => {},
                () = caller_change => {},
                () = sleep_until(wake_at) => {},
            }
        }
    }

    fn try_enter(&mut self, state: &mut State, phase: Phase) -> Result<(), Blocked> {
        self.check_drain()?;
        self.rate.check_caller(std::time::Instant::now())?;
        if self.slots.caller.is_none()
            && self
                .caller
                .as_ref()
                .is_some_and(|caller| !caller.limit.capacity.available(caller.limit.max))
        {
            return Err(CALLER_BUSY.into());
        }
        if !self.upstream.capacity.available(self.upstream.max) {
            return Err(UPSTREAM_BUSY.into());
        }
        let active = if self.slots.active.is_none() {
            Some(self.active.reserve().ok_or(SERVER_BUSY)?)
        } else {
            None
        };
        let caller = if let (None, Some(caller)) = (&self.slots.caller, &self.caller) {
            Some(caller.limit.reserve().ok_or(CALLER_BUSY)?)
        } else {
            None
        };
        if phase == Phase::Dispatch {
            let upstream = self.upstream.reserve().ok_or(UPSTREAM_BUSY)?;
            self.charge()?;
            let name = self.caller_name().to_owned();
            *state.running.entry(name.clone()).or_default() += 1;
            self.slots.running = Some(Running {
                upstream: Arc::clone(self.upstream.capacity),
                caller: name,
                slot: Some(upstream),
            });
        } else {
            if self.peer.is_closed() {
                return Err(DISCONNECTED.into());
            }
            self.rate.check_upstream(std::time::Instant::now())?;
        }
        self.slots.active = active.or(self.slots.active.take());
        self.slots.caller = caller.or(self.slots.caller.take());
        Ok(())
    }

    fn refusing_caller(&mut self) -> Result<(), Failure> {
        if self
            .caller
            .as_ref()
            .is_some_and(|caller| caller.over_limit == OverLimit::Refuse)
        {
            if self.slots.caller.is_none()
                && self
                    .caller
                    .as_ref()
                    .is_some_and(|caller| !caller.limit.capacity.available(caller.limit.max))
            {
                return Err(CALLER_BUSY);
            }
            self.rate.check_caller(std::time::Instant::now())?;
        }
        Ok(())
    }

    fn can_wait(&self, failure: &Failure) -> bool {
        failure.status == StatusCode::TOO_MANY_REQUESTS
            && (!matches!(
                failure.code,
                "caller_concurrency_limit" | "caller_rate_limit"
            ) || self
                .caller
                .as_ref()
                .is_some_and(|caller| caller.over_limit == OverLimit::Queue))
    }

    fn caller_name(&self) -> &str {
        self.caller.as_ref().map_or("", |caller| caller.name)
    }

    fn check_drain(&self) -> Result<(), Failure> {
        if *self.drain.borrow() {
            Err(DRAINING)
        } else {
            Ok(())
        }
    }

    fn charge(&mut self) -> Result<(), Failure> {
        if self.peer.is_closed() {
            return Err(DISCONNECTED);
        }
        self.rate
            .charge(std::time::Instant::now())
            .map_err(Into::into)
    }
}

const fn refusal(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retry_after: u64,
) -> Failure {
    Failure {
        status,
        code,
        message,
        retry_after: Some(retry_after),
        reached: "false",
        attempted: false,
    }
}

pub const CALLER_BUSY: Failure = refusal(
    StatusCode::TOO_MANY_REQUESTS,
    "caller_concurrency_limit",
    "This caller has reached its concurrency limit.",
    1,
);
const UPSTREAM_BUSY: Failure = refusal(
    StatusCode::TOO_MANY_REQUESTS,
    "upstream_concurrency_limit",
    "The selected upstream has reached its concurrency limit.",
    1,
);
const SERVER_BUSY: Failure = refusal(
    StatusCode::TOO_MANY_REQUESTS,
    "server_busy",
    "The server is at capacity.",
    1,
);
const QUEUE_FULL: Failure = refusal(
    StatusCode::TOO_MANY_REQUESTS,
    "queue_full",
    "The selected upstream queue has no room.",
    1,
);
const QUEUE_TIMEOUT: Failure = refusal(
    StatusCode::TOO_MANY_REQUESTS,
    "queue_timeout",
    "The request could not be admitted before its queue deadline.",
    5,
);
const DRAINING: Failure = refusal(
    StatusCode::SERVICE_UNAVAILABLE,
    "draining",
    "The router is shutting down.",
    1,
);
const DISCONNECTED: Failure = Failure {
    status: StatusCode::BAD_REQUEST,
    code: "client_disconnected",
    message: "The client closed the connection before provider dispatch.",
    retry_after: None,
    reached: "false",
    attempted: false,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranks_priority_then_fewer_running_then_arrival_and_skips_blocked_callers() {
        let now = Instant::now();
        let blocked = Arc::new(Capacity::default());
        let hold = blocked.reserve(1, 1).unwrap();
        let mut queue = State::default();
        for (id, caller, priority) in [
            (0, "background", Priority::Background),
            (1, "busy", Priority::Interactive),
            (2, "blocked", Priority::Interactive),
            (3, "idle", Priority::Interactive),
            (4, "idle", Priority::Interactive),
        ] {
            queue.waiting.insert(
                id,
                Waiting {
                    caller: caller.into(),
                    priority,
                    limit: (id == 2).then(|| (Arc::clone(&blocked), 1)),
                    ready_at: now,
                    phase: Phase::Dispatch,
                },
            );
        }
        queue.running.insert("busy".into(), 2);
        assert_eq!(queue.next(now), Some(3));
        queue.waiting.remove(&3);
        assert_eq!(queue.next(now), Some(4));
        drop(hold);
        assert_eq!(queue.next(now), Some(2));
        queue.waiting.remove(&2);
        queue.waiting.get_mut(&4).unwrap().ready_at = now + Duration::from_millis(1);
        assert_eq!(queue.next(now), Some(1));
        assert_eq!(queue.next(now + Duration::from_millis(1)), Some(4));
        queue.waiting.get_mut(&4).unwrap().phase = Phase::Upload;
        assert_eq!(queue.next(now + Duration::from_millis(1)), Some(1));
    }
}
