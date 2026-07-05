mod agent;
mod config;
mod db;
mod runtime;

#[cfg(unix)]
#[tokio::main]
async fn main() {
    use std::sync::Arc;

    // Default to INFO so operators can see startup reconcile counts and any
    // dropped-command / connection warnings without setting RUST_LOG.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = config::Config::from_env();
    if cfg.node_token.is_empty() {
        eprintln!("SKY_NODE_TOKEN is required");
        std::process::exit(1);
    }

    let rt: Arc<dyn runtime::ContainerRuntime> = Arc::new(runtime::DockerRuntime::new(
        cfg.docker_socket.clone(),
        cfg.container_dns.clone(),
    ));
    let (mut dispatcher, events_rx) =
        agent::Dispatcher::new(rt, cfg.volumes_root.clone(), cfg.backups_root.clone());
    if let Some(dbcfg) = cfg.database.as_ref() {
        dispatcher.set_db(Arc::new(db::DbAdmin::new(dbcfg)));
        tracing::info!(
            "database provisioning enabled (admin {}:{}, public host {})",
            dbcfg.admin_host,
            dbcfg.admin_port,
            dbcfg.public_host
        );
    }
    let dispatcher = Arc::new(dispatcher);

    // Rebuild container tracking from what Docker still has, so a daemon
    // restart resumes heartbeats/stats for servers that are still running.
    dispatcher.reconcile().await;

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
