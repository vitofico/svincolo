mod client_socket;
mod config;
mod managed;
mod model;
mod multipart;
mod queue;
mod rate;
mod server;
mod state;
mod upstream;

use std::{env, path::PathBuf, process::ExitCode, time::Duration};

const USAGE: &str = "Usage: svincolo serve [--config PATH] | svincolo health --url URL";

fn main() -> ExitCode {
    let result = match tokio::runtime::Runtime::new() {
        Ok(runtime) => {
            let result = runtime.block_on(run());
            // A stalled system DNS lookup can outlive the server's shutdown grace period.
            runtime.shutdown_background();
            result
        }
        Err(error) => Err((1, format!("runtime initialisation failed: {error}"))),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("svincolo: {message}");
            ExitCode::from(code)
        }
    }
}

async fn run() -> Result<(), (u8, String)> {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => {
            println!("{USAGE}");
            Ok(())
        }
        [flag] if flag == "--version" => {
            println!("svincolo {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [command] if command == "serve" => {
            let path = env::var_os("SVINCOLO_CONFIG")
                .map(PathBuf::from)
                .ok_or_else(|| (64, "set SVINCOLO_CONFIG or pass --config PATH".into()))?;
            serve(path).await
        }
        [command, flag, path] if command == "serve" && flag == "--config" => {
            serve(path.into()).await
        }
        [command, flag, url] if command == "health" && flag == "--url" => {
            health(url.to_str().unwrap_or_default()).await
        }
        _ => Err((64, USAGE.into())),
    }
}

async fn serve(path: PathBuf) -> Result<(), (u8, String)> {
    let (config, inputs) = config::Config::load_snapshot(&path).map_err(|error| (78, error))?;
    server::serve(config, path, inputs)
        .await
        .map_err(|error| (1, error))
}

async fn health(url: &str) -> Result<(), (u8, String)> {
    let mut url = reqwest::Url::parse(url)
        .map_err(|_| (64, "health requires a valid HTTP(S) origin".into()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err((64, "health requires a valid HTTP(S) origin".into()));
    }
    url.set_path("/readyz");
    let ca = env::var_os("SVINCOLO_EXTRA_CA");
    let response = upstream::client(ca.as_deref().map(std::path::Path::new))
        .map_err(|message| (1, message.into()))?
        .get(url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map_err(|_| (1, "router health check failed".into()))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err((
            1,
            format!("router readiness returned {}", response.status()),
        ));
    }
    Ok(())
}
