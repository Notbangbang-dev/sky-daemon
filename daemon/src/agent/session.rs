//! One live WebSocket connection to panel-api: sending a signed hello,
//! periodic signed heartbeats, and verifying + dispatching inbound signed
//! commands. Deliberately decoupled from dialing/reconnection (see
//! `client.rs`) so it can be exercised in tests against a local WebSocket
//! server without any real reconnect logic involved.

use anyhow::{bail, Context, Result};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use protocol::{envelope_type, CommandPayload, Envelope, EventPayload, HelloPayload};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

use super::dispatcher::Dispatcher;
use super::nonce_cache::NonceCache;

pub const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How long an incoming envelope's nonce is remembered for replay
/// detection. Comfortably wider than `protocol::MAX_CLOCK_SKEW_SECS` so a
/// message can never both pass the freshness check and have its nonce
/// cache entry expire before a replay would be caught.
const NONCE_CACHE_TTL: Duration = Duration::from_secs(120);

pub struct Session<S> {
    ws: S,
    node_token: String,
    dispatcher: Arc<Dispatcher>,
    heartbeat_interval: Duration,
    incoming_nonces: NonceCache,
}

impl<S> Session<S>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message>
        + Unpin,
    <S as Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(
        ws: S,
        node_token: String,
        dispatcher: Arc<Dispatcher>,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            ws,
            node_token,
            dispatcher,
            heartbeat_interval,
            incoming_nonces: NonceCache::new(NONCE_CACHE_TTL),
        }
    }

    /// Blocks until the connection closes, a protocol error occurs, or `ct`
    /// is cancelled. `events_rx` is owned by the caller (see `client.rs`)
    /// and outlives any single connection — the dispatcher's background
    /// console-streaming tasks keep sending into it across reconnects, so a
    /// dropped connection just means those events queue up rather than
    /// getting lost.
    pub async fn run(
        &mut self,
        ct: tokio_util::sync::CancellationToken,
        events_rx: &mut mpsc::UnboundedReceiver<EventPayload>,
    ) -> Result<()> {
        let hello = HelloPayload {
            node_token: self.node_token.clone(),
            agent_version: AGENT_VERSION.to_string(),
            capabilities: vec![protocol::CAP_PULL_IMAGE.to_string()],
        };
        self.send_signed(envelope_type::HELLO, &hello)
            .await
            .context("send hello")?;

        let mut heartbeat_timer = tokio::time::interval(self.heartbeat_interval);
        heartbeat_timer.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = ct.cancelled() => return Ok(()),
                _ = heartbeat_timer.tick() => {
                    let payload = self.dispatcher.heartbeat().await;
                    self.send_signed(envelope_type::HEARTBEAT, &payload).await.context("send heartbeat")?;
                }
                event = events_rx.recv() => {
                    if let Some(event) = event {
                        self.send_signed(envelope_type::EVENT, &event).await.context("send event")?;
                    }
                }
                msg = self.ws.next() => {
                    match msg {
                        None => return Ok(()),
                        Some(Err(err)) => return Err(err.into()),
                        Some(Ok(Message::Text(text))) => self.handle_incoming(text.as_str()).await?,
                        Some(Ok(Message::Close(_))) => return Ok(()),
                        Some(Ok(_)) => {} // ping/pong/binary frames need no application-level handling
                    }
                }
            }
        }
    }

    async fn handle_incoming(&mut self, text: &str) -> Result<()> {
        let envelope: Envelope = match serde_json::from_str(text) {
            Ok(e) => e,
            Err(err) => {
                warn!("dropping malformed envelope: {err}");
                return Ok(());
            }
        };

        if !envelope.verify(self.node_token.as_bytes()) {
            bail!(
                "rejecting envelope with invalid signature or stale timestamp (type={})",
                envelope.kind
            );
        }
        if !self.incoming_nonces.check_and_record(&envelope.nonce) {
            bail!("rejecting replayed nonce (type={})", envelope.kind);
        }

        if envelope.kind != envelope_type::COMMAND {
            return Ok(());
        }

        let cmd: CommandPayload = envelope
            .decode_payload()
            .context("decode command payload")?;
        let ack = self.dispatcher.handle(&cmd).await;
        self.send_signed(envelope_type::ACK, &ack)
            .await
            .context("send ack")?;
        Ok(())
    }

    async fn send_signed<T: serde::Serialize>(&mut self, kind: &str, payload: &T) -> Result<()> {
        let envelope = Envelope::signed(self.node_token.as_bytes(), kind, payload)?;
        let text = serde_json::to_string(&envelope)?;
        self.ws
            .send(Message::Text(text))
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::FakeRuntime;
    use futures_util::TryStreamExt;
    use protocol::{AckPayload, Action, ContainerSpec};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{accept_async, connect_async, WebSocketStream};

    async fn local_ws_pair() -> (
        WebSocketStream<TcpStream>,
        WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_async(stream).await.unwrap()
        });

        let (client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let server = server_task.await.unwrap();
        (server, client)
    }

    fn test_dispatcher() -> (Arc<Dispatcher>, mpsc::UnboundedReceiver<EventPayload>) {
        let (dispatcher, events_rx) = Dispatcher::new(
            Arc::new(FakeRuntime::new()),
            std::env::temp_dir(),
            std::env::temp_dir(),
        );
        (Arc::new(dispatcher), events_rx)
    }

    fn test_dispatcher_with_runtime() -> (
        Arc<Dispatcher>,
        mpsc::UnboundedReceiver<EventPayload>,
        Arc<FakeRuntime>,
    ) {
        let rt = Arc::new(FakeRuntime::new());
        let (dispatcher, events_rx) =
            Dispatcher::new(rt.clone(), std::env::temp_dir(), std::env::temp_dir());
        (Arc::new(dispatcher), events_rx, rt)
    }

    #[tokio::test]
    async fn sends_signed_hello_and_heartbeat() {
        let (mut server, client) = local_ws_pair().await;
        let (dispatcher, mut events_rx) = test_dispatcher();
        let mut session = Session::new(
            client,
            "node-secret".to_string(),
            dispatcher,
            Duration::from_millis(30),
        );

        let ct = tokio_util::sync::CancellationToken::new();
        let ct2 = ct.clone();
        tokio::spawn(async move {
            let _ = session.run(ct2, &mut events_rx).await;
        });

        let hello_msg = server.try_next().await.unwrap().unwrap();
        let hello_env: Envelope = serde_json::from_str(hello_msg.to_text().unwrap()).unwrap();
        assert_eq!(hello_env.kind, envelope_type::HELLO);
        assert!(hello_env.verify(b"node-secret"));
        let hello: HelloPayload = hello_env.decode_payload().unwrap();
        assert_eq!(hello.node_token, "node-secret");

        let hb_msg = server.try_next().await.unwrap().unwrap();
        let hb_env: Envelope = serde_json::from_str(hb_msg.to_text().unwrap()).unwrap();
        assert_eq!(hb_env.kind, envelope_type::HEARTBEAT);
        assert!(hb_env.verify(b"node-secret"));

        ct.cancel();
    }

    #[tokio::test]
    async fn dispatches_signed_command_and_acks() {
        let (mut server, client) = local_ws_pair().await;
        let (dispatcher, mut events_rx) = test_dispatcher();
        let mut session = Session::new(
            client,
            "node-secret".to_string(),
            dispatcher,
            Duration::from_secs(3600),
        );

        let ct = tokio_util::sync::CancellationToken::new();
        let ct2 = ct.clone();
        let handle = tokio::spawn(async move {
            let _ = session.run(ct2, &mut events_rx).await;
        });

        // Drain hello.
        server.try_next().await.unwrap().unwrap();

        let spec = ContainerSpec {
            image: "test-image".into(),
            ..Default::default()
        };
        let cmd = CommandPayload {
            command_id: "cmd-1".into(),
            action: Action::Create,
            server_id: "server-1".into(),
            spec: Some(spec),
            ..Default::default()
        };
        let cmd_env = Envelope::signed(b"node-secret", envelope_type::COMMAND, &cmd).unwrap();
        server
            .send(Message::Text(serde_json::to_string(&cmd_env).unwrap()))
            .await
            .unwrap();

        let ack_msg = server.try_next().await.unwrap().unwrap();
        let ack_env: Envelope = serde_json::from_str(ack_msg.to_text().unwrap()).unwrap();
        assert_eq!(ack_env.kind, envelope_type::ACK);
        let ack: AckPayload = ack_env.decode_payload().unwrap();
        assert!(ack.ok, "expected ack ok, got error: {:?}", ack.error);
        assert_eq!(ack.command_id, "cmd-1");

        ct.cancel();
        handle.abort();
    }

    #[tokio::test]
    async fn console_output_streams_as_signed_events_after_start() {
        let (mut server, client) = local_ws_pair().await;
        let (dispatcher, mut events_rx, rt) = test_dispatcher_with_runtime();
        let dispatcher_handle = dispatcher.clone();
        let mut session = Session::new(
            client,
            "node-secret".to_string(),
            dispatcher,
            Duration::from_secs(3600),
        );

        let ct = tokio_util::sync::CancellationToken::new();
        let ct2 = ct.clone();
        let handle = tokio::spawn(async move {
            let _ = session.run(ct2, &mut events_rx).await;
        });

        server.try_next().await.unwrap().unwrap(); // drain hello

        let spec = ContainerSpec {
            image: "test-image".into(),
            ..Default::default()
        };
        let create = CommandPayload {
            command_id: "c1".into(),
            action: Action::Create,
            server_id: "server-1".into(),
            spec: Some(spec),
            ..Default::default()
        };
        server
            .send(Message::Text(
                serde_json::to_string(
                    &Envelope::signed(b"node-secret", envelope_type::COMMAND, &create).unwrap(),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        server.try_next().await.unwrap().unwrap(); // create ack

        let start = CommandPayload {
            command_id: "c2".into(),
            action: Action::Start,
            server_id: "server-1".into(),
            ..Default::default()
        };
        server
            .send(Message::Text(
                serde_json::to_string(
                    &Envelope::signed(b"node-secret", envelope_type::COMMAND, &start).unwrap(),
                )
                .unwrap(),
            ))
            .await
            .unwrap();

        // The start ack and a state_changed event race in undefined order;
        // drain envelopes until we've seen both kinds.
        let mut saw_ack = false;
        let mut saw_state_changed = false;
        while !saw_ack || !saw_state_changed {
            let msg = server.try_next().await.unwrap().unwrap();
            let env: Envelope = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            match env.kind.as_str() {
                envelope_type::ACK => saw_ack = true,
                envelope_type::EVENT => saw_state_changed = true,
                _ => {}
            }
        }

        // `start` also attaches a console in the background; give that a
        // moment to land before pushing fake output through it.
        let container_id = wait_for_container_id(&dispatcher_handle, "server-1").await;
        rt.emit_output(&container_id, "Server started!");

        let event_msg = server.try_next().await.unwrap().unwrap();
        let event_env: Envelope = serde_json::from_str(event_msg.to_text().unwrap()).unwrap();
        assert_eq!(event_env.kind, envelope_type::EVENT);
        assert!(event_env.verify(b"node-secret"));
        let event: EventPayload = event_env.decode_payload().unwrap();
        assert_eq!(event.server_id, "server-1");
        assert_eq!(event.message, "Server started!");

        ct.cancel();
        handle.abort();
    }

    async fn wait_for_container_id(dispatcher: &Dispatcher, server_id: &str) -> String {
        for _ in 0..50 {
            if let Some(id) = dispatcher.container_id_for_server(server_id) {
                return id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {server_id} to be tracked");
    }

    #[tokio::test]
    async fn rejects_command_signed_with_wrong_secret() {
        let (mut server, client) = local_ws_pair().await;
        let (dispatcher, mut events_rx) = test_dispatcher();
        let mut session = Session::new(
            client,
            "node-secret".to_string(),
            dispatcher,
            Duration::from_secs(3600),
        );

        let ct = tokio_util::sync::CancellationToken::new();
        let ct2 = ct.clone();
        let handle = tokio::spawn(async move { session.run(ct2, &mut events_rx).await });

        server.try_next().await.unwrap().unwrap(); // hello

        let cmd = CommandPayload {
            command_id: "cmd-1".into(),
            action: Action::Start,
            server_id: "server-1".into(),
            ..Default::default()
        };
        let forged = Envelope::signed(b"wrong-secret", envelope_type::COMMAND, &cmd).unwrap();
        server
            .send(Message::Text(serde_json::to_string(&forged).unwrap()))
            .await
            .unwrap();

        // The session should error out (bad signature) rather than act on it.
        let result = handle.await.unwrap();
        assert!(result.is_err());
    }
}
