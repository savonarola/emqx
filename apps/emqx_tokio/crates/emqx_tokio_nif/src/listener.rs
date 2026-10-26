use crate::{
    mqtt::{self, Message, Packet, Version},
    state::{Connection, Endpoint, NodeState, Output},
};
use bytes::BytesMut;
use rustler::{Encoder, LocalPid, OwnedEnv};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{BufReader, IoSlice},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::{sleep, Instant},
};
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const READ_SIZE: usize = 8192;
const WRITE_BATCH: usize = 64;

/// Listener configuration from Erlang.
#[derive(Clone, PartialEq, rustler::NifMap)]
pub struct Config {
    pub id: String,
    pub bind: String,
    pub enable: bool,
    pub tls: bool,
    pub certfile: String,
    pub keyfile: String,
}

/// A running listener: its configuration, its stop signal, and its accept task.
pub struct Listener {
    config: Config,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

/// The native listeners of one runtime, by listener ID.
#[derive(Default)]
pub struct Manager {
    listeners: HashMap<String, Listener>,
}

impl Manager {
    /// Start, replace, or stop a listener. Does nothing when it already runs with the same config.
    pub async fn apply(
        &mut self,
        state: Arc<NodeState>,
        config: Config,
        restart: bool,
        owner: LocalPid,
    ) -> Result<(), String> {
        if !restart
            && self
                .listeners
                .get(&config.id)
                .is_some_and(|l| l.config == config && !l.task.is_finished())
        {
            return Ok(());
        }
        // Load and validate the new certificate before closing the old listener.
        let tls = if config.enable && config.tls {
            let c = config.clone();
            Some(
                tokio::task::spawn_blocking(move || tls_config(&c))
                    .await
                    .map_err(|e| e.to_string())??,
            )
        } else {
            None
        };
        self.stop(&config.id).await;
        if !config.enable {
            return Ok(());
        }
        let socket = TcpListener::bind(&config.bind)
            .await
            .map_err(|e| e.to_string())?;
        let (stop, receiver) = watch::channel(false);
        let id = config.id.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = accept(state, socket, tls, receiver).await {
                let _ = OwnedEnv::new().send_and_clear(&owner, |env| {
                    (crate::atoms::native_listener_error(), id, error.to_string()).encode(env)
                });
            }
        });
        self.listeners
            .insert(config.id.clone(), Listener { config, stop, task });
        Ok(())
    }

    /// Signal a listener to stop and wait until its connections have ended.
    pub async fn stop(&mut self, id: &str) {
        if let Some(listener) = self.listeners.remove(id) {
            listener.stop.send_replace(true);
            let _ = listener.task.await;
        }
    }

    pub async fn stop_all(&mut self) {
        let ids: Vec<_> = self.listeners.keys().cloned().collect();
        for id in ids {
            self.stop(&id).await;
        }
    }

    /// IDs of the listeners whose accept task still runs.
    pub fn running(&self) -> Vec<String> {
        self.listeners
            .iter()
            .filter(|(_, l)| !l.task.is_finished())
            .map(|(id, _)| id.clone())
            .collect()
    }
}

/// Load the certificate chain and the private key into a TLS acceptor.
fn tls_config(config: &Config) -> Result<TlsAcceptor, String> {
    let certs = std::fs::File::open(&config.certfile).map_err(|e| e.to_string())?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(certs))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let key = std::fs::File::open(&config.keyfile).map_err(|e| e.to_string())?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key))
        .map_err(|e| e.to_string())?
        .ok_or("missing private key")?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| e.to_string())?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// MQTT session state of one connection, owned by its reader.
struct Channel {
    state: Arc<NodeState>,
    ep: Arc<Endpoint>,
    client: Arc<str>,
    will: Option<Message>,
    version: Version,
    connected: bool,
    // Set when processing a CONNECT fails, so that the error can be returned in CONNACK.
    connect_failed: bool,
    session_expiry: u32,
    // Sends PUBACK packet IDs from the reader to the writer, which owns the inflight window.
    acknowledgements: Option<mpsc::Sender<u16>>,
}

impl Drop for Channel {
    // Runs on every exit, including a panic: leave the router, then publish any remaining will.
    fn drop(&mut self) {
        self.state.detach(&self.ep);
        if !self.state.quiet.load(Ordering::Acquire) {
            if let Some(will) = self.will.take() {
                self.state.publish(&self.ep, &self.client, will, true);
            }
        }
    }
}

/// Accept connections until stopped. Each connection runs as one task: TLS handshake, then `serve`.
async fn accept(
    state: Arc<NodeState>,
    socket: TcpListener,
    tls: Option<TlsAcceptor>,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut channels = JoinSet::new();
    let result = loop {
        tokio::select! {
            _ = stop.changed() => break Ok(()),
            // Reap finished connection tasks.
            _ = channels.join_next(), if !channels.is_empty() => (),
            accepted = socket.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                let _ = stream.set_nodelay(true);
                // Register the connection in a slot. Without a live representative it is dropped.
                let Some((ep, output)) = state.attach() else { continue; };
                let channel = Channel {
                    state: state.clone(), ep, client: "".into(), will: None,
                    version: Version::V4, connected: false, connect_failed: false,
                    session_expiry: 0, acknowledgements: None,
                };
                let tls = tls.clone();
                let shutdown = stop.clone();
                channels.spawn(async move {
                    // The close signal (overflow, slot cleared) ends the task at once.
                    let mut close = channel.ep.close.subscribe();
                    if *close.borrow() { return; }
                    tokio::select! {
                        _ = close.changed() => (),
                        _ = async {
                            if let Some(tls) = tls {
                                if let Ok(Ok(stream)) = tokio::time::timeout(CONNECT_TIMEOUT, tls.accept(stream)).await {
                                    serve(channel, stream, output, shutdown).await;
                                }
                            } else { serve(channel, stream, output, shutdown).await; }
                        } => (),
                    }
                });
            }
        }
    };
    drop(socket);
    // Give connections 1 s to send DISCONNECT and flush, then abort the rest.
    if tokio::time::timeout(Duration::from_secs(1), async {
        while channels.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        channels.abort_all();
    }
    while channels.join_next().await.is_some() {}
    result
}

/// Run a connection's reader and writer. After the reader ends, the writer has 1 s to flush.
async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut channel: Channel,
    stream: S,
    mut output: mpsc::Receiver<Output>,
    mut shutdown: watch::Receiver<bool>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let ep = channel.ep.clone();
    let writing = write(&mut writer, &mut output, &ep);
    tokio::pin!(writing);
    tokio::select! {
        result = async {
            tokio::select! {
                result = channel.read(&mut reader) => result,
                _ = shutdown.changed() => Err(0x8b),
            }
        } => {
            // Report the error to the client: CONNACK before CONNECT completed, DISCONNECT for MQTT 5 after.
            if let Err(reason) = result {
                // MQTT 3.1.1 CONNACK codes exist only for a bad version and a rejected client ID.
                if !channel.connected {
                    if channel.connect_failed
                        && (channel.version == Version::V5 || matches!(reason, 0x84 | 0x85))
                    {
                        ep.send(&mqtt::connack(channel.version, reason, None, channel.state.limits.max_packet_size, 0));
                    }
                } else if channel.version == Version::V5 {
                    ep.send(&mqtt::disconnect(reason));
                }
            }
            ep.close_after_flush();
            let _ = tokio::time::timeout(Duration::from_secs(1), &mut writing).await;
        },
        _ = &mut writing => (),
    }
}

/// Writer-side QoS 1 state: packet IDs waiting for PUBACK, and deliveries waiting for a free ID.
#[derive(Default)]
struct Inflight {
    ids: HashSet<u16>,
    next: u16,
    pending: VecDeque<Output>,
}

impl Inflight {
    /// Take the next packet ID that is not in flight. 0 is not a valid ID.
    fn allocate(&mut self) -> u16 {
        loop {
            self.next = self.next.wrapping_add(1).max(1);
            if self.ids.insert(self.next) {
                return self.next;
            }
        }
    }

    /// Move pending deliveries into the batch while the inflight window has room.
    fn ready(&mut self, window: usize, batch: &mut Vec<Output>) {
        let available = window - self.ids.len();
        for _ in 0..available.min(WRITE_BATCH) {
            if let Some(delivery) = self.pending.pop_front() {
                batch.push(delivery);
            } else {
                break;
            }
        }
    }
}

// Write every queued packet with one vectored write instead of one write per packet.
async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    output: &mut mpsc::Receiver<Output>,
    ep: &Endpoint,
) -> std::io::Result<()> {
    let mut batch = Vec::with_capacity(WRITE_BATCH);
    let mut ready = Vec::with_capacity(WRITE_BATCH);
    let mut inflight = Inflight::default();
    let mut acknowledgements: Option<mpsc::Receiver<u16>> = None;
    loop {
        let window = ep.connection.get().map_or(1, |c| c.window);
        inflight.ready(window, &mut batch);
        // Nothing to write: wait for new output or for a PUBACK that frees a packet ID.
        if batch.is_empty() {
            tokio::select! {
                count = output.recv_many(&mut batch, WRITE_BATCH) => {
                    if count == 0 { return Ok(()); }
                },
                id = async {
                    match &mut acknowledgements {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(id) = id { inflight.ids.remove(&id); }
                    else { acknowledgements = None; }
                    continue;
                }
            }
        }
        // Assign packet IDs and build headers. QoS 1 beyond the window waits in `pending`.
        let mut close = false;
        let mut headers: [Option<mqtt::DeliveryHeader>; WRITE_BATCH] =
            std::array::from_fn(|_| None);
        let mut pubacks = [[0; 4]; WRITE_BATCH];
        let version = ep.connection.get().map_or(Version::V4, |c| c.version);
        let window = ep.connection.get().map_or(1, |c| c.window);
        for item in batch.drain(..) {
            match item {
                Output::Connected(receiver, connack) => {
                    acknowledgements = Some(receiver);
                    ready.push(Output::Packet(connack));
                }
                Output::Delivery(_, 1) if inflight.ids.len() >= window => {
                    inflight.pending.push_back(item)
                }
                Output::Delivery(ref msg, qos) => {
                    let id = if qos == 1 { inflight.allocate() } else { 0 };
                    headers[ready.len()] = Some(mqtt::DeliveryHeader::new(msg, version, qos, id));
                    ready.push(item);
                }
                Output::Puback(id) => {
                    pubacks[ready.len()] = mqtt::puback(id);
                    ready.push(item);
                }
                Output::Close => {
                    ep.written(1);
                    close = true;
                    break;
                }
                Output::Packet(_) => ready.push(item),
            }
        }
        // Point the write buffers at the headers and at the shared message slices, without copying.
        let mut slices = [IoSlice::new(&[]); WRITE_BATCH * 5];
        let mut count = 0;
        for (index, item) in ready.iter().enumerate() {
            match item {
                Output::Packet(packet) => {
                    slices[count] = IoSlice::new(packet);
                    count += 1;
                }
                Output::Delivery(msg, _) => {
                    for part in headers[index].as_ref().unwrap().slices(msg, version) {
                        if !part.is_empty() {
                            slices[count] = part;
                            count += 1;
                        }
                    }
                }
                Output::Puback(_) => {
                    slices[count] = IoSlice::new(&pubacks[index]);
                    count += 1;
                }
                _ => unreachable!(),
            }
        }
        let mut slices = &mut slices[..count];
        while !slices.is_empty() {
            match writer.write_vectored(slices).await? {
                0 => return Err(std::io::ErrorKind::WriteZero.into()),
                n => IoSlice::advance_slices(&mut slices, n),
            }
        }
        // Release the output-limit credit of the written items.
        ep.written(ready.len());
        ready.clear();
        if close {
            return writer.shutdown().await;
        }
    }
}

impl Channel {
    /// Read and handle packets until DISCONNECT (Ok) or an error (its reason code).
    async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> Result<(), u8> {
        let mut buf = BytesMut::with_capacity(READ_SIZE);
        let mut interval = Some(CONNECT_TIMEOUT);
        let mut last_packet = Instant::now();
        // One timer per channel. It is re-armed only when it fires before the deadline.
        let timer = sleep(CONNECT_TIMEOUT);
        tokio::pin!(timer);
        loop {
            let mut received = false;
            let mut published = false;
            // Handle every complete packet in the buffer. A second CONNECT is a protocol error.
            while let Some(packet) = {
                let connect = buf.first() == Some(&0x10);
                if self.connected && connect {
                    return Err(mqtt::PROTOCOL);
                }
                mqtt::decode(
                    &mut buf,
                    self.state.limits.max_packet_size,
                    &mut self.version,
                )
                .inspect_err(|_| self.connect_failed = !self.connected && connect)?
            } {
                received = true;
                published |= matches!(packet, Packet::Publish(_, _));
                if !self.connected {
                    match packet {
                        Packet::Connect {
                            client,
                            keepalive,
                            will,
                            properties,
                        } => {
                            let assigned = client.is_empty();
                            self.client = if client.is_empty() {
                                format!("native-{}", self.ep.id).into()
                            } else {
                                client.into()
                            };
                            self.will = will;
                            // QoS 1 window: min(client Receive Maximum, max_inflight).
                            let window = self
                                .state
                                .limits
                                .max_inflight
                                .min(properties.receive_maximum.unwrap_or(u16::MAX) as usize);
                            let (sender, receiver) = mpsc::channel(window);
                            self.acknowledgements = Some(sender);
                            let _ = self.ep.connection.set(Connection {
                                version: self.version,
                                maximum_packet_size: properties
                                    .maximum_packet_size
                                    .unwrap_or(u32::MAX)
                                    as usize,
                                window,
                            });
                            // The keepalive deadline is 1.5 times the client's keepalive.
                            interval = (keepalive > 0)
                                .then(|| Duration::from_millis(keepalive as u64 * 1500));
                            if let Some(interval) = interval {
                                timer.as_mut().reset(Instant::now() + interval);
                            }
                            let connack = mqtt::connack(
                                self.version,
                                0,
                                assigned.then_some(&self.client),
                                self.state.limits.max_packet_size,
                                properties.session_expiry,
                            );
                            if connack.len() > self.ep.connection.get().unwrap().maximum_packet_size
                            {
                                self.will = None;
                                self.connect_failed = true;
                                return Err(mqtt::PACKET_TOO_LARGE);
                            }
                            self.session_expiry = properties.session_expiry;
                            self.ep.enqueue(Output::Connected(receiver, connack));
                            self.connected = true;
                        }
                        _ => return Err(mqtt::PROTOCOL),
                    }
                } else if !self.packet(packet).await? {
                    return Ok(());
                }
            }
            // A publisher stops reading its socket while its representative queue is over the limit.
            if published {
                self.state.wait_capacity(self.ep.slot).await;
            }
            // Count the wait as activity so that a paused client does not time out.
            if received {
                last_packet = Instant::now();
            }
            // The decoder checks the packet size before it accepts a body.
            buf.reserve(READ_SIZE);
            // Wait for more bytes or for the keepalive deadline.
            tokio::select! {
                read = reader.read_buf(&mut buf) => {
                    if read.map_err(|_| 0x80u8)? == 0 {
                        return Err(0x80);
                    }
                }
                _ = &mut timer, if interval.is_some() => {
                    let deadline = last_packet + interval.unwrap();
                    if deadline <= Instant::now() {
                        return Err(0x8d);
                    }
                    timer.as_mut().reset(deadline);
                }
            }
        }
    }

    /// Handle one packet after CONNECT. Returns false on DISCONNECT.
    async fn packet(&mut self, packet: Packet) -> Result<bool, u8> {
        match packet {
            // Route, then acknowledge QoS 1 at once: nothing is stored.
            Packet::Publish(msg, id) => {
                if !self.state.publish(&self.ep, &self.client, msg, false) {
                    return Err(0x80);
                }
                if let Some(id) = id {
                    self.ep.enqueue(Output::Puback(id));
                }
            }
            // The writer owns the inflight window: pass it the ID. Waits when the channel is full.
            Packet::Puback(id) => {
                self.acknowledgements
                    .as_ref()
                    .unwrap()
                    .send(id)
                    .await
                    .map_err(|_| 0x80u8)?;
            }
            Packet::Subscribe(id, filters) => {
                let mut codes = Vec::with_capacity(filters.len());
                for (filter, options) in filters {
                    let code = if filter.starts_with("$share/") {
                        0x9e
                    } else if !mqtt::valid_filter(&filter) {
                        0x8f
                    } else if self.state.subscribe(&self.ep, &filter, options) {
                        options.qos
                    } else {
                        0x80
                    };
                    // MQTT 3.1.1 has one failure code, 0x80.
                    codes.push(if self.version == Version::V4 && code >= 0x80 {
                        0x80
                    } else {
                        code
                    });
                }
                self.ep.send(&mqtt::suback(self.version, id, &codes, false));
            }
            Packet::Unsubscribe(id, filters) => {
                let codes: Vec<_> = filters
                    .iter()
                    .map(|filter| {
                        if self.state.unsubscribe(&self.ep, filter) {
                            0
                        } else {
                            0x11
                        }
                    })
                    .collect();
                self.ep.send(&mqtt::suback(self.version, id, &codes, true));
            }
            Packet::Ping => self.ep.send(&bytes::Bytes::from_static(&[0xd0, 0])),
            Packet::Disconnect(reason, expiry) => {
                // A non-zero expiry is a protocol error only when CONNECT requested no expiry.
                if expiry != 0 && self.session_expiry == 0 {
                    return Err(mqtt::PROTOCOL);
                }
                // Reason 0x04 (disconnect with will message) keeps the will for publishing.
                if reason != 4 {
                    self.will = None;
                }
                return Ok(false);
            }
            _ => return Err(mqtt::PROTOCOL),
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Limits;

    #[tokio::test]
    async fn task_panic_removes_membership_and_publishes_will() {
        let state = Arc::new(NodeState::new(Limits::default()));
        state.slots[0]
            .inner
            .lock()
            .alive
            .store(true, Ordering::Release);
        state.forwarding.store(false, Ordering::Release);
        let (subscriber, mut output) = state.attach().unwrap();
        let _ = subscriber.connection.set(Connection {
            version: Version::V4,
            maximum_packet_size: usize::MAX,
            window: 32,
        });
        state.subscribe(&subscriber, "will", mqtt::Subscription::default());
        let (publisher, _output) = state.attach().unwrap();
        state.subscribe(&publisher, "temporary", mqtt::Subscription::default());
        let will = Message::copy(b"will", &[], b"gone", 0);
        let channel = Channel {
            state: state.clone(),
            ep: publisher.clone(),
            client: "client".into(),
            will: Some(will),
            version: Version::V4,
            connected: true,
            connect_failed: false,
            session_expiry: 0,
            acknowledgements: None,
        };
        let task = tokio::spawn(async move {
            let _channel = channel;
            panic!("channel failure");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert!(!state.subscribe(&publisher, "after-panic", mqtt::Subscription::default()));
        assert!(matches!(output.try_recv(), Ok(Output::Delivery(msg, 0))
            if msg.topic() == "will" && msg.payload == b"gone"[..]));
    }

    #[test]
    fn packet_ids_skip_inflight_and_wrap() {
        let mut inflight = Inflight::default();
        inflight.ids.extend([1, 2, u16::MAX]);
        inflight.next = u16::MAX - 1;
        assert_eq!(inflight.allocate(), 3);
        inflight.ids.remove(&1);
        inflight.next = u16::MAX;
        assert_eq!(inflight.allocate(), 1);
    }

    #[tokio::test]
    async fn writer_resumes_pending_after_puback() {
        let state = NodeState::new(Limits::default());
        state.slots[0]
            .inner
            .lock()
            .alive
            .store(true, Ordering::Release);
        let (ep, mut output) = state.attach().unwrap();
        let _ = ep.connection.set(Connection {
            version: Version::V5,
            maximum_packet_size: 4096,
            window: 1,
        });
        let (ack, receiver) = mpsc::channel(1);
        ep.enqueue(Output::Connected(
            receiver,
            mqtt::connack(Version::V5, 0x8c, None, 4096, 0),
        ));
        ep.enqueue(Output::Delivery(Message::copy(b"t", &[], b"1", 1), 1));
        ep.enqueue(Output::Delivery(Message::copy(b"t", &[], b"2", 1), 1));
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let task = tokio::spawn(async move { write(&mut writer, &mut output, &ep).await });
        let mut connack = [0; 5];
        reader.read_exact(&mut connack).await.unwrap();
        assert_eq!(connack, [0x20, 3, 0, 0x8c, 0]);
        let mut buf = [0; 9];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [0x32, 7, 0, 1, b't', 0, 1, 0, b'1']);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), reader.read_u8())
                .await
                .is_err()
        );
        ack.send(1).await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [0x32, 7, 0, 1, b't', 0, 2, 0, b'2']);
        task.abort();
    }
}
