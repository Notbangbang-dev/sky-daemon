mod agent;
mod config;
mod runtime;

#[cfg(unix)]
#[tokio::main]
async fn main() {
    use std::sync::Arc;

    tracing_subscriber::fmt::init();

    let cfg = config::Config::from_env();
    if cfg.node_token.is_empty() {
        eprintln!("SKY_NODE_TOKEN is required");
        std::process::exit(1);
    }

    let rt: Arc<dyn runtime::ContainerRuntime> =
        Arc::new(runtime::DockerRuntime::new(cfg.docker_socket.clone()));
    let (dispatcher, events_rx) =
        agent::Dispatcher::new(rt, cfg.volumes_root.clone(), cfg.backups_root.clone());
    let dispatcher = Arc::new(dispatcher);

    let ct = tokio_util::sync::CancellationToken::new();
    let ct_signal = ct.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ct_signal.cancel();
    });

    tracing::info!(
        "sky-daemon {} connecting to {}",
        agent::AGENT_VERSION,
        cfg.panel_ws_url
    );
    agent::client::run(
        &cfg.panel_ws_url,
        &cfg.node_token,
        dispatcher,
        events_rx,
        cfg.heartbeat_interval,
        ct,
    )
    .await;
}

#[cfg(not(unix))]
fn main() {
    eprintln!("sky-daemon only supports unix targets (it drives Docker over a unix socket).");
    std::process::exit(1);
}
