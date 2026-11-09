use std::{
    num::NonZeroU32,
    sync::Mutex,
    time::{Duration, Instant},
};

const TOKEN: u128 = 60_000_000_000;

#[derive(Clone, Copy)]
pub struct Rate {
    pub per_minute: Option<NonZeroU32>,
    pub generation: u64,
    pub effective_at: Instant,
}

struct Bucket {
    rate: Rate,
    credit: u128,
    updated: Instant,
}

impl Bucket {
    fn new(rate: Rate, now: Instant) -> Self {
        Self {
            rate,
            credit: max_credit(rate.per_minute),
            updated: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        if let Some(rate) = self.rate.per_minute {
            let elapsed = now.saturating_duration_since(self.updated).as_nanos();
            self.credit = (self.credit + elapsed * u128::from(rate.get()))
                .min(max_credit(self.rate.per_minute));
        }
        self.updated = now;
    }

    fn refresh(&mut self, rate: Rate, now: Instant) {
        if rate.generation > self.rate.generation {
            self.refill(rate.effective_at.max(self.updated).min(now));
            let cap = max_credit(rate.per_minute);
            self.credit = if self.rate.per_minute.is_none() {
                cap
            } else {
                self.credit.min(cap)
            };
            self.rate = rate;
        }
        self.refill(now);
    }

    fn wait(&self) -> Option<Duration> {
        let rate = self.rate.per_minute?;
        if self.credit >= TOKEN {
            return None;
        }
        Some(Duration::from_nanos(
            (TOKEN - self.credit).div_ceil(u128::from(rate.get())) as u64,
        ))
    }

    fn charge(&mut self) {
        if self.rate.per_minute.is_some() {
            self.credit -= TOKEN;
        }
    }
}

fn max_credit(rate: Option<NonZeroU32>) -> u128 {
    rate.map_or(0, |rate| u128::from(rate.get()) * TOKEN)
}

#[derive(Default)]
pub struct Account(Mutex<Option<Bucket>>);

pub struct AttemptBudget<'a> {
    caller: Option<(&'a Account, Rate)>,
    upstream: (&'a Account, Rate),
}

pub struct RateLimited {
    pub code: &'static str,
    pub message: &'static str,
    pub wait: Duration,
}

impl RateLimited {
    pub fn retry_after(&self) -> u64 {
        self.wait.as_secs() + u64::from(self.wait.subsec_nanos() != 0)
    }
}

impl<'a> AttemptBudget<'a> {
    pub fn new(caller: Option<(&'a Account, Rate)>, upstream: (&'a Account, Rate)) -> Self {
        Self { caller, upstream }
    }

    pub fn check_caller(&self, now: Instant) -> Result<(), RateLimited> {
        if let Some((account, rate)) = self.caller {
            let mut caller = account.0.lock().unwrap();
            check_caller(bucket(&mut caller, rate, now))?;
        }
        Ok(())
    }

    pub fn check_upstream(&self, now: Instant) -> Result<(), RateLimited> {
        let (account, rate) = self.upstream;
        check_upstream(bucket(&mut account.0.lock().unwrap(), rate, now))
    }

    pub fn charge(&mut self, now: Instant) -> Result<(), RateLimited> {
        // All attempts take the caller lock before the upstream lock. Checking both
        // before debiting either keeps an upstream refusal from using caller credit.
        let mut caller = self.caller.map(|(account, _)| account.0.lock().unwrap());
        if let (Some(caller), Some((_, rate))) = (&mut caller, self.caller) {
            check_caller(bucket(caller, rate, now))?;
        }
        let (account, rate) = self.upstream;
        let mut upstream = account.0.lock().unwrap();
        let upstream = bucket(&mut upstream, rate, now);
        check_upstream(upstream)?;
        if let Some(caller) = &mut caller {
            caller.as_mut().expect("caller bucket checked").charge();
        }
        upstream.charge();
        self.caller = None;
        Ok(())
    }
}

fn check_upstream(bucket: &Bucket) -> Result<(), RateLimited> {
    if let Some(wait) = bucket.wait() {
        return Err(RateLimited {
            code: "upstream_rate_limit",
            message: "The selected upstream has reached its request rate limit.",
            wait,
        });
    }
    Ok(())
}

fn check_caller(bucket: &Bucket) -> Result<(), RateLimited> {
    if let Some(wait) = bucket.wait() {
        return Err(RateLimited {
            code: "caller_rate_limit",
            message: "This caller has reached its request rate limit.",
            wait,
        });
    }
    Ok(())
}

fn bucket(current: &mut Option<Bucket>, rate: Rate, now: Instant) -> &mut Bucket {
    let current = current.get_or_insert_with(|| Bucket::new(rate, now));
    current.refresh(rate, now.max(current.updated));
    current
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn refill_and_rate_changes_preserve_credit_and_ignore_older_settings() {
        let now = Instant::now();
        let initial = Rate {
            per_minute: NonZeroU32::new(2),
            generation: 1,
            effective_at: now,
        };
        let mut bucket = Bucket::new(initial, now);
        bucket.charge();
        bucket.charge();
        assert_eq!(bucket.wait(), Some(Duration::from_secs(30)));
        let effective_at = now + Duration::from_secs(10);
        let changed = Rate {
            per_minute: NonZeroU32::new(1),
            generation: 2,
            effective_at,
        };
        bucket.refresh(changed, effective_at);
        assert_eq!(bucket.wait(), Some(Duration::from_secs(40)));
        bucket.refresh(initial, effective_at + Duration::from_secs(20));
        assert_eq!(bucket.wait(), Some(Duration::from_secs(20)));
        bucket.refresh(changed, effective_at + Duration::from_secs(40));
        assert_eq!(bucket.wait(), None);
        bucket.charge();
        assert_eq!(bucket.wait(), Some(Duration::from_secs(60)));
    }
}
