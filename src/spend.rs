use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::StatusCode;
use serde::{Deserialize, de::DeserializeOwned};

use crate::upstream::Failure;

#[derive(Deserialize)]
struct Scopes<T> {
    app: HashMap<String, T>,
    provider: HashMap<String, T>,
}

#[derive(Deserialize)]
struct Cap {
    period: String,
    hard: Option<u64>,
}

#[derive(Deserialize)]
struct Snapshot {
    recorded_at: u64,
    periods: HashMap<String, Period>,
}

#[derive(Deserialize)]
struct Period {
    start: u64,
    end: u64,
    #[serde(flatten)]
    totals: Scopes<u64>,
}

fn unavailable() -> Failure {
    Failure {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "spend_state_unavailable",
        message: "The spending ledger is unavailable.",
        reached: "false",
        retry_after: Some(30),
    }
}

async fn read<T: DeserializeOwned>(dir: &Path, name: &str) -> Result<T, Failure> {
    let bytes = tokio::fs::read(dir.join(name))
        .await
        .map_err(|_| unavailable())?;
    serde_json::from_slice(&bytes).map_err(|_| unavailable())
}

pub(crate) async fn check(dir: &Path, app: &str, provider: &str) -> Result<(), Failure> {
    let limits: Scopes<Cap> = read(dir, "limits.json").await?;
    let app_cap = limits.app.get(app).filter(|cap| cap.hard.is_some());
    let provider_cap = limits
        .provider
        .get(provider)
        .filter(|cap| cap.hard.is_some());
    if app_cap.is_none() && provider_cap.is_none() {
        return Ok(());
    }
    let state: Snapshot = read(dir, "usage.json").await?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| unavailable())?
        .as_secs();
    if state.recorded_at > now || now - state.recorded_at > 120 {
        return Err(unavailable());
    }
    let mut wait = 0;
    for (cap, is_app) in [(app_cap, true), (provider_cap, false)] {
        if let Some(cap) = cap {
            let period = state.periods.get(&cap.period).ok_or_else(unavailable)?;
            if !(period.start..period.end).contains(&now) {
                return Err(unavailable());
            }
            let used = if is_app {
                period.totals.app.get(app)
            } else {
                period.totals.provider.get(provider)
            };
            if cap
                .hard
                .is_some_and(|hard| used.copied().unwrap_or(0) >= hard)
            {
                wait = wait.max(period.end - now);
            }
        }
    }
    if wait > 0 {
        return Err(Failure {
            status: StatusCode::PAYMENT_REQUIRED,
            code: "spend_cap_reached",
            message: "An application or provider spending cap was reached.",
            reached: "false",
            retry_after: Some(wait),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn checks_both_scopes_at_equality_and_ignores_soft_only_rules() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let limits = json!({"app":{"app-a":{"hard":25,"period":"day"},"soft":{"soft":1,"hard":null,"period":"week"}},
                            "provider":{"cloud":{"hard":50,"period":"month"}}});
        let period =
            json!({"start":now-60,"end":now+120,"app":{"app-a":25},"provider":{"cloud":50}});
        std::fs::write(path.join("limits.json"), limits.to_string()).unwrap();
        let snapshot = json!({"recorded_at":now,"periods":{"day":period,"month":period}});
        std::fs::write(path.join("usage.json"), snapshot.to_string()).unwrap();
        for (caller, provider) in [("app-a", "local"), ("soft", "cloud")] {
            let error = check(path, caller, provider).await.err().unwrap();
            assert_eq!(error.status, StatusCode::PAYMENT_REQUIRED);
            assert_eq!(error.reached, "false");
            assert!(error.retry_after.is_some());
        }
        assert!(check(path, "soft", "local").await.is_ok());
        std::fs::remove_file(path.join("usage.json")).unwrap();
        assert!(check(path, "soft", "local").await.is_ok());
        assert_eq!(
            check(path, "app-a", "local").await.err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
