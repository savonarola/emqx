use crate::{
    mqtt::{self, Message, Packet},
    state::{Endpoint, NodeState, Output},
};
use bytes::BytesMut;
use rustler::{Encoder, LocalPid, OwnedEnv};
use std::{
    collections::HashMap,
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

#[derive(Clone, PartialEq, rustler::NifMap)]
pub struct Config {
    pub id: String,
    pub bind: String,
    pub enable: bool,
    pub tls: bool,
    pub certfile: String,
    pub keyfile: String,
}

pub struct Listener {
    config: Config,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Default)]
pub struct Manager {
    listeners: HashMap<String, Listener>,
}

impl Manager {
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

    pub fn running(&self) -> Vec<String> {
        self.listeners
            .iter()
            .filter(|(_, l)| !l.task.is_finished())
            .map(|(id, _)| id.clone())
            .collect()
    }
}

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

struct Channel {
    state: Arc<NodeState>,
    ep: Arc<Endpoint>,
    client: Arc<str>,
    will: Option<Message>,
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.state.detach(&self.ep);
        if !self.state.quiet.load(Ordering::Acquire) {
            if let Some(will) = self.will.take() {
                self.state.publish(&self.ep, &self.client, will, true);
            }
        }
    }
}

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
            _ = channels.join_next(), if !channels.is_empty() => (),
            accepted = socket.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                let _ = stream.set_nodelay(true);
                let Some((ep, output)) = state.attach() else { continue; };
                let channel = Channel { state: state.clone(), ep, client: "".into(), will: None };
                let tls = tls.clone();
                channels.spawn(async move {
                    let mut close = channel.ep.close.subscribe();
                    if *close.borrow() { return; }
                    tokio::select! {
                        _ = close.changed() => (),
                        _ = async {
                            if let Some(tls) = tls {
                                if let Ok(Ok(stream)) = tokio::time::timeout(CONNECT_TIMEOUT, tls.accept(stream)).await {
                                    serve(channel, stream, output).await;
                                }
                            } else { serve(channel, stream, output).await; }
                        } => (),
                    }
                });
            }
        }
    };
    drop(socket);
    channels.abort_all();
    while channels.join_next().await.is_some() {}
    result
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut channel: Channel,
    stream: S,
    mut output: mpsc::Receiver<Output>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    tokio::select! {
        _ = channel.read(&mut reader) => (),
        _ = write(&mut writer, &mut output) => (),
    }
}

// Write every queued packet with one vectored write instead of one write per packet.
async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    output: &mut mpsc::Receiver<Output>,
) -> std::io::Result<()> {
    let mut batch = Vec::new();
    while output.recv_many(&mut batch, WRITE_BATCH).await > 0 {
        let mut close = false;
        let mut slices = Vec::with_capacity(batch.len());
        for item in &batch {
            match item {
                Output::Packet(packet) => slices.push(IoSlice::new(packet)),
                Output::Close => {
                    close = true;
                    break;
                }
            }
        }
        let mut slices = &mut slices[..];
        while !slices.is_empty() {
            match writer.write_vectored(slices).await? {
                0 => return Err(std::io::ErrorKind::WriteZero.into()),
                n => IoSlice::advance_slices(&mut slices, n),
            }
        }
        batch.clear();
        if close {
            return writer.shutdown().await;
        }
    }
    Ok(())
}

impl Channel {
    async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> Result<(), ()> {
        let mut buf = BytesMut::with_capacity(READ_SIZE);
        let mut connected = false;
        let mut interval = Some(CONNECT_TIMEOUT);
        let mut last_packet = Instant::now();
        // One timer per channel. It is re-armed only when it fires before the deadline.
        let timer = sleep(CONNECT_TIMEOUT);
        tokio::pin!(timer);
        loop {
            let mut received = false;
            let mut published = false;
            while let Some(packet) = mqtt::decode(&mut buf, self.state.limits.max_packet_size)? {
                received = true;
                published |= matches!(packet, Packet::Publish(_));
                if !connected {
                    match packet {
                        Packet::UnsupportedVersion => {
                            self.ep.send(&bytes::Bytes::from_static(&[0x20, 2, 0, 1]));
                            self.ep.close_after_flush();
                            return std::future::pending().await;
                        }
                        Packet::Connect {
                            client,
                            keepalive,
                            will,
                        } => {
                            self.client = if client.is_empty() {
                                format!("native-{}", self.ep.id).into()
                            } else {
                                client.into()
                            };
                            self.will = will;
                            interval = (keepalive > 0)
                                .then(|| Duration::from_millis(keepalive as u64 * 1500));
                            if let Some(interval) = interval {
                                timer.as_mut().reset(Instant::now() + interval);
                            }
                            connected = true;
                            self.ep.send(&bytes::Bytes::from_static(&[0x20, 2, 0, 0]));
                        }
                        _ => return Err(()),
                    }
                } else if !self.packet(packet)? {
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
            tokio::select! {
                read = reader.read_buf(&mut buf) => {
                    if read.map_err(|_| ())? == 0 {
                        return Err(());
                    }
                }
                _ = &mut timer, if interval.is_some() => {
                    let deadline = last_packet + interval.unwrap();
                    if deadline <= Instant::now() {
                        return Err(());
                    }
                    timer.as_mut().reset(deadline);
                }
            }
        }
    }

    fn packet(&mut self, packet: Packet) -> Result<bool, ()> {
        match packet {
            Packet::Publish(msg) => {
                if !self.state.publish(&self.ep, &self.client, msg, false) {
                    return Err(());
                }
            }
            Packet::Subscribe(id, filters) => {
                let mut body = id.to_be_bytes().to_vec();
                for (filter, _) in filters {
                    let accepted =
                        mqtt::valid_filter(&filter) && self.state.subscribe(&self.ep, &filter);
                    body.push(if accepted { 0 } else { 0x80 });
                }
                self.ep.send(&mqtt::frame(0x90, &body));
            }
            Packet::Unsubscribe(id, filters) => {
                for filter in filters {
                    self.state.unsubscribe(&self.ep, &filter);
                }
                self.ep.send(&mqtt::frame(0xb0, &id.to_be_bytes()));
            }
            Packet::Ping => self.ep.send(&bytes::Bytes::from_static(&[0xd0, 0])),
            Packet::Disconnect => {
                self.will = None;
                return Ok(false);
            }
            _ => return Err(()),
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
        state.subscribe(&subscriber, "will");
        let (publisher, _output) = state.attach().unwrap();
        state.subscribe(&publisher, "temporary");
        let will = Message {
            topic: "will".into(),
            payload: bytes::Bytes::from_static(b"gone"),
            qos: 0,
        };
        let expected = mqtt::publish(will.topic.as_bytes(), &will.payload, false);
        let channel = Channel {
            state: state.clone(),
            ep: publisher.clone(),
            client: "client".into(),
            will: Some(will),
        };
        let task = tokio::spawn(async move {
            let _channel = channel;
            panic!("channel failure");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert!(!state.subscribe(&publisher, "after-panic"));
        assert!(matches!(output.try_recv(), Ok(Output::Packet(packet)) if packet == expected));
    }
}
