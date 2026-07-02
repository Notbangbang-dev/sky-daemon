//! Dials `ws_url` and keeps the connection alive, reconnecting with capped
//! exponential backoff whenever it drops, until the cancellation token
//! fires.

use protocol::EventPayload;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::dispatcher::Dispatcher;
use super::session::Session;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// `events_rx` is owned here (not per-`Session`) so events emitted by the
/// dispatcher's background tasks (e.g. console output) while disconnected
/// just queue up rather than being lost on reconnect.
pub async fn run(
    ws_url: &str,
    node_token: &str,
    dispatcher: Arc<Dispatcher>,
    mut events_rx: mpsc::UnboundedReceiver<EventPayload>,
    heartbeat_interval: Duration,
    ct: CancellationToken,
) {
    let mut backoff = MIN_BACKOFF;

    while !ct.is_cancelled() {
        match connect_async(ws_url).await {
            Ok((ws, _)) => {
                info!("connected to {ws_url}");
                backoff = MIN_BACKOFF;

                let mut session = Session::new(
                    ws,
                    node_token.to_string(),
                    dispatcher.clone(),
                    heartbeat_interval,
                );
                if let Err(err) = session.run(ct.clone(), &mut events_rx).await {
                    // `{err:#}` prints the full anyhow context chain (e.g. the
                    // underlying serde error), not just the top-level message,
                    // so a decode failure names the offending field/variant.
                    error!("session ended: {err:#}");
                }
            }
            Err(err) => {
                error!("dial {ws_url} failed: {err} (retrying in {backoff:?})");
            }
        }

        tokio::select! {
            _ = ct.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
