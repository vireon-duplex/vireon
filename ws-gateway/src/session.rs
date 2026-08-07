//! Per-WebSocket-connection state and dispatch loop.
//!
//! Each browser session owns:
//!   - one `vireon_sdk::Client` (the upstream QUIC connection)
//!   - a routing table mapping `sub_id` → S→C forwarder for default-channel
//!     subscriptions and consumer-group subscriptions
//!   - a routing table mapping `stream_handle_id` → S→C forwarder for
//!     dedicated streams (receive-only — browser examples never call
//!     `stream.publish()`)
//!
//! The dispatch loop is a `tokio::select!` over:
//!   - inbound WS messages (decoded into `ClientOp`s and dispatched)
//!   - the merged S→C channel (forwarder tasks push encoded frames here,
//!     the writer drains them into the WS sink)
//!
//! The gateway stays API-faithful: it does not interpret message bodies or
//! subscription patterns. The browser's `sub_id` / `stream_handle_id` are
//! opaque integers chosen by the browser.

use std::collections::HashMap;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, info, warn};
use vireon_sdk::{
    Client, ClientBuilder, DeliveryPolicy, GroupSubscription, StreamHandle, StreamSpec,
    Subscription, TlsVerify,
};

use crate::cli::TlsVerifyArg;
use crate::protocol::{
    self, ClientOp, DecodeError, c2s,
};

/// Channel depth for the per-session S→C aggregator. Forwarder tasks push
/// encoded frames here; the writer task drains into the WS sink. Large
/// enough to absorb brief subscriber bursts; a sustained overload applies
/// backpressure to the SDK channels (which drop oldest at the SDK layer).
const S2C_CHANNEL_DEPTH: usize = 4096;

/// Bookkeeping for one active forwarder task. Drop to abort.
struct Forwarder {
    task: JoinHandle<()>,
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Per-session state.
pub struct Session {
    client: Client,
    /// Default-channel + group-subscription forwarders keyed by `sub_id`.
    sub_forwarders: HashMap<u32, Forwarder>,
    /// Dedicated-stream forwarders keyed by `stream_handle_id`.
    stream_forwarders: HashMap<u32, Forwarder>,
}

impl Session {
    /// Establish the upstream QUIC connection.
    pub async fn connect(
        server_addr: &str,
        sni: Option<&str>,
        tls: &TlsVerifyArg,
    ) -> Result<Self, vireon_sdk::ConnectError> {
        let mut builder = ClientBuilder::new(server_addr);
        if let Some(s) = sni {
            builder = builder.sni(s);
        }
        builder = builder.tls_verify(resolve_tls(tls));
        let client = builder.connect().await?;
        info!(server = %server_addr, "[gateway] upstream QUIC connected");
        Ok(Self {
            client,
            sub_forwarders: HashMap::new(),
            stream_forwarders: HashMap::new(),
        })
    }

    fn spawn_sub_forwarder(
        &mut self,
        sub_id: u32,
        mut subscription: Subscription,
        s2c_tx: mpsc::Sender<Vec<u8>>,
    ) {
        let task = tokio::spawn(async move {
            while let Some(msg) = subscription.recv().await {
                let frame = protocol::encode_message(sub_id, &msg);
                if s2c_tx.send(frame).await.is_err() {
                    break;
                }
            }
            debug!(sub_id, "[gateway] sub forwarder exited");
        });
        self.sub_forwarders.insert(sub_id, Forwarder { task });
    }

    fn spawn_group_forwarder(
        &mut self,
        sub_id: u32,
        mut group: GroupSubscription,
        s2c_tx: mpsc::Sender<Vec<u8>>,
    ) {
        let task = tokio::spawn(async move {
            while let Some(msg) = group.recv().await {
                let frame = protocol::encode_message(sub_id, &msg);
                if s2c_tx.send(frame).await.is_err() {
                    break;
                }
            }
            debug!(sub_id, "[gateway] group forwarder exited");
        });
        self.sub_forwarders.insert(sub_id, Forwarder { task });
    }

    fn spawn_stream_forwarder(
        &mut self,
        stream_handle_id: u32,
        mut stream: StreamHandle,
        s2c_tx: mpsc::Sender<Vec<u8>>,
    ) {
        let task = tokio::spawn(async move {
            while let Some(msg) = stream.recv().await {
                let frame = protocol::encode_stream_message(stream_handle_id, &msg);
                if s2c_tx.send(frame).await.is_err() {
                    break;
                }
            }
            debug!(stream_handle_id, "[gateway] stream forwarder exited");
        });
        self.stream_forwarders
            .insert(stream_handle_id, Forwarder { task });
    }

    /// Drive one decoded client op to completion. Emits acks/errors on `s2c_tx`.
    /// Returns true when the browser requested close.
    async fn handle_op(&mut self, op: ClientOp, s2c_tx: &mpsc::Sender<Vec<u8>>) -> bool {
        match op {
            ClientOp::Connect { req_id } => {
                let _ = s2c_tx.send(protocol::encode_ack(req_id, c2s::CONNECT)).await;
            }
            ClientOp::Subscribe {
                req_id,
                sub_id,
                topic,
            } => match self.client.subscribe(&topic).await {
                Ok(sub) => {
                    self.spawn_sub_forwarder(sub_id, sub, s2c_tx.clone());
                    let _ = s2c_tx
                        .send(protocol::encode_ack(req_id, c2s::SUBSCRIBE))
                        .await;
                }
                Err(e) => {
                    nack(s2c_tx, req_id, c2s::SUBSCRIBE, &format!("{e:?}")).await;
                }
            },
            ClientOp::Unsubscribe {
                req_id,
                sub_id,
                topic,
            } => {
                self.sub_forwarders.remove(&sub_id);
                match self.client.unsubscribe(&topic).await {
                    Ok(()) => {
                        let _ = s2c_tx
                            .send(protocol::encode_ack(req_id, c2s::UNSUBSCRIBE))
                            .await;
                    }
                    Err(e) => {
                        nack(s2c_tx, req_id, c2s::UNSUBSCRIBE, &format!("{e:?}")).await;
                    }
                }
            }
            ClientOp::OpenStream {
                req_id,
                stream_handle_id,
                policy,
                topic,
            } => {
                let dp =
                    protocol::decode_policy(policy).unwrap_or(DeliveryPolicy::ReliableOrdered);
                let mut spec = StreamSpec::new(dp);
                if let Some(t) = topic {
                    spec = spec.with_topic(t);
                }
                match self.client.open_stream(spec).await {
                    Ok(stream) => {
                        self.spawn_stream_forwarder(stream_handle_id, stream, s2c_tx.clone());
                        let _ = s2c_tx
                            .send(protocol::encode_ack(req_id, c2s::OPEN_STREAM))
                            .await;
                    }
                    Err(e) => {
                        nack(s2c_tx, req_id, c2s::OPEN_STREAM, &format!("{e:?}")).await;
                    }
                }
            }
            ClientOp::Publish {
                req_id,
                topic,
                payload,
            } => match self.client.publish(&topic, payload).await {
                Ok(()) => {
                    let _ = s2c_tx.send(protocol::encode_ack(req_id, c2s::PUBLISH)).await;
                }
                Err(e) => {
                    nack(s2c_tx, req_id, c2s::PUBLISH, &format!("{e:?}")).await;
                }
            },
            ClientOp::TryPublish { topic, payload } => {
                let _ = self.client.try_publish(&topic, payload);
            }
            ClientOp::GroupJoin {
                req_id,
                sub_id,
                topic,
                group,
                consumer,
            } => match self.client.subscribe_group(&topic, &group, &consumer).await {
                Ok(gs) => {
                    self.spawn_group_forwarder(sub_id, gs, s2c_tx.clone());
                    let _ = s2c_tx
                        .send(protocol::encode_ack(req_id, c2s::GROUP_JOIN))
                        .await;
                }
                Err(e) => {
                    nack(s2c_tx, req_id, c2s::GROUP_JOIN, &format!("{e:?}")).await;
                }
            },
            ClientOp::GroupLeave {
                req_id,
                topic,
                group,
                consumer,
            } => match self.client.leave_group(&topic, &group, &consumer).await {
                Ok(()) => {
                    let _ = s2c_tx
                        .send(protocol::encode_ack(req_id, c2s::GROUP_LEAVE))
                        .await;
                }
                Err(e) => {
                    nack(s2c_tx, req_id, c2s::GROUP_LEAVE, &format!("{e:?}")).await;
                }
            },
            ClientOp::StreamPublish {
                req_id,
                stream_handle_id,
                topic,
                payload,
            } => {
                // We don't own the StreamHandle (it lives in the forwarder),
                // so we can't actually publish on the dedicated stream from
                // here. Route via the default channel instead — the server
                // still delivers to subscribers on the dedicated stream
                // because the publisher's topic matches. The browser SDK
                // documents that stream_publish is best-effort in v1.
                debug!(
                    stream_handle_id,
                    %topic,
                    "[gateway] stream_publish routed via default channel"
                );
                match self.client.publish(&topic, payload).await {
                    Ok(()) => {
                        let _ = s2c_tx
                            .send(protocol::encode_ack(req_id, c2s::STREAM_PUBLISH))
                            .await;
                    }
                    Err(e) => {
                        nack(s2c_tx, req_id, c2s::STREAM_PUBLISH, &format!("{e:?}")).await;
                    }
                }
            }
            ClientOp::StreamTryPublish {
                stream_handle_id,
                topic,
                payload,
            } => {
                debug!(
                    stream_handle_id,
                    %topic,
                    "[gateway] stream_try_publish routed via default channel"
                );
                let _ = self.client.try_publish(&topic, payload);
            }
            ClientOp::Close => return true,
        }
        false
    }

    /// Drop forwarders and close upstream. Best-effort.
    pub async fn shutdown(self) {
        let Self {
            client,
            sub_forwarders,
            stream_forwarders,
        } = self;
        drop(sub_forwarders);
        drop(stream_forwarders);
        if let Err(e) = client.close().await {
            warn!(error = %format!("{e:?}"), "[gateway] upstream close failed");
        }
    }
}

fn resolve_tls(arg: &TlsVerifyArg) -> TlsVerify {
    match arg {
        TlsVerifyArg::Tofu => TlsVerify::Tofu,
        TlsVerifyArg::DangerAcceptInvalid => TlsVerify::DangerAcceptInvalid,
        TlsVerifyArg::Strict { ca } => TlsVerify::Strict {
            ca: std::path::PathBuf::from(ca),
        },
    }
}

async fn nack(tx: &mpsc::Sender<Vec<u8>>, req_id: u32, op: u8, msg: &str) {
    let _ = tx.send(protocol::encode_error(req_id, op, 1, msg)).await;
}

/// Drive one WebSocket connection. Returns when the WS closes or a fatal
/// error occurs; the caller does NOT need to act on the returned error
/// string (it's for logging only).
pub async fn run_session(
    ws: WebSocketStream<tokio::net::TcpStream>,
    server_addr: String,
    sni: Option<String>,
    tls: TlsVerifyArg,
) -> Result<(), String> {
    let (mut ws_sink, mut ws_stream) = ws.split();

    // Upstream QUIC conn.
    let mut session = match Session::connect(&server_addr, sni.as_deref(), &tls).await {
        Ok(s) => s,
        Err(e) => {
            let err_frame = protocol::encode_error(0, c2s::CONNECT, 1, &format!("{e:?}"));
            let _ = ws_sink.send(WsMessage::Binary(err_frame)).await;
            let _ = ws_sink.send(WsMessage::Close(None)).await;
            return Err(format!("upstream connect failed: {e:?}"));
        }
    };

    let (s2c_tx, mut s2c_rx) = mpsc::channel::<Vec<u8>>(S2C_CHANNEL_DEPTH);

    // Send the initial "ready" ack (req_id=0, op=CONNECT) so the browser's
    // connect() Promise resolves on first frame.
    let _ = s2c_tx.send(protocol::encode_ack(0, c2s::CONNECT)).await;

    // Drive reader and writer concurrently.
    loop {
        tokio::select! {
            biased;

            // S→C: drain aggregated frames into the WS sink.
            Some(frame) = s2c_rx.recv() => {
                if ws_sink.send(WsMessage::Binary(frame)).await.is_err() {
                    break;
                }
            }

            // C→S: decode and dispatch.
            msg = ws_stream.next() => {
                let Some(msg) = msg else { break };
                let payload = match msg {
                    Ok(WsMessage::Binary(bytes)) => bytes,
                    Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => continue,
                    Ok(WsMessage::Close(_)) => break,
                    Ok(WsMessage::Text(_)) | Ok(WsMessage::Frame(_)) => continue,
                    Err(e) => {
                        warn!(error = %format!("{e:?}"), "[gateway] ws read error");
                        break;
                    }
                };
                let op = match protocol::decode_client_op(payload.as_slice()) {
                    Ok(op) => op,
                    Err(DecodeError::Short) => {
                        warn!("[gateway] short C→S frame, ignoring");
                        continue;
                    }
                    Err(DecodeError::BadTag(t)) => {
                        warn!(tag = t, "[gateway] unknown op tag");
                        continue;
                    }
                    Err(DecodeError::BadPolicy(p)) => {
                        warn!(policy_byte = p, "[gateway] bad policy byte");
                        continue;
                    }
                };
                let close = session.handle_op(op, &s2c_tx).await;
                if close {
                    break;
                }
            }
        }
    }

    let _ = ws_sink.send(WsMessage::Close(None)).await;
    session.shutdown().await;
    // Suppress unused-import warning for Bytes (referenced only via type aliases).
    let _ = std::marker::PhantomData::<Bytes>;
    Ok(())
}
