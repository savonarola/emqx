mod atoms;
mod listener;
mod mqtt;
mod state;

use parking_lot::Mutex;
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedEnv, ResourceArc, Term};
use state::{Limits, NodeState};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

/// NIF resource: the tokio runtime, the shared node state, and the native listeners.
pub struct TokioRuntime {
    state: Arc<NodeState>,
    handle: Handle,
    runtime: Mutex<Option<Runtime>>,
    listeners: Arc<tokio::sync::Mutex<listener::Manager>>,
}

impl rustler::Resource for TokioRuntime {
    const IMPLEMENTS_DOWN: bool = true;

    /// A representative died: clear its slot, which closes the slot's connections.
    fn down(&self, _env: Env, _pid: LocalPid, monitor: rustler::Monitor) {
        for (index, slot) in self.state.slots.iter().enumerate() {
            let mut inner = slot.inner.lock();
            if inner.representative.is_some_and(|(_, m)| m == monitor) {
                self.state.clear_slot(index, &mut inner);
                break;
            }
        }
    }
}

impl Drop for TokioRuntime {
    // The last resource reference is gone: stop the runtime on its own thread, without wills.
    fn drop(&mut self) {
        self.state.quiet.store(true, Ordering::Release);
        if let Some(rt) = self.runtime.lock().take() {
            std::thread::spawn(move || rt.shutdown_timeout(Duration::from_secs(5)));
        }
    }
}

fn load(env: Env, _info: Term) -> bool {
    env.register::<TokioRuntime>().is_ok()
}

/// Runtime with default limits.
#[rustler::nif(schedule = "DirtyIo")]
fn runtime_new(env: Env) -> NifResult<Term> {
    new_runtime(env, Limits::default())
}

/// Runtime for native listeners. Rejects limits that the router or the codec cannot use.
#[rustler::nif(schedule = "DirtyIo")]
fn native_new(env: Env, limits: Limits) -> NifResult<Term> {
    if limits.representatives == 0
        || limits.worker_threads == 0
        || limits.batch_size == 0
        || limits.output_limit == 0
        || limits.max_packet_size < 2
        || limits.max_packet_size > 268_435_460
        || limits.max_inflight == 0
        || limits.max_inflight > u16::MAX as usize
    {
        return Err(rustler::Error::BadArg);
    }
    new_runtime(env, limits)
}

fn new_runtime(env: Env, limits: Limits) -> NifResult<Term> {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(limits.worker_threads)
        .enable_all()
        .build()
    {
        Ok(rt) => {
            let resource = ResourceArc::new(TokioRuntime {
                state: Arc::new(NodeState::new(limits)),
                handle: rt.handle().clone(),
                runtime: Mutex::new(Some(rt)),
                listeners: Arc::new(tokio::sync::Mutex::new(listener::Manager::default())),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Stop the listeners and the runtime on a separate thread, then send `runtime_stopped` to Reply.
#[rustler::nif]
fn runtime_stop(rt: ResourceArc<TokioRuntime>, reply: LocalPid) -> rustler::Atom {
    rt.state.quiet.store(true, Ordering::Release);
    let runtime = rt.runtime.lock().take();
    let listeners = rt.listeners.clone();
    std::thread::spawn(move || {
        if let Some(runtime) = runtime {
            runtime.block_on(async { listeners.lock().await.stop_all().await });
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
        let _ = OwnedEnv::new().send_and_clear(&reply, |env| atoms::runtime_stopped().encode(env));
    });
    atoms::ok()
}

/// Bind a representative to a slot. A previous one is replaced, and the slot is cleared.
#[rustler::nif]
fn register(
    env: Env,
    rt: ResourceArc<TokioRuntime>,
    index: usize,
    pid: LocalPid,
) -> NifResult<rustler::Atom> {
    let slot = rt.state.slots.get(index).ok_or(rustler::Error::BadArg)?;
    let mut inner = slot.inner.lock();
    if let Some((_, monitor)) = inner.representative {
        rt.demonitor(Some(env), &monitor);
    }
    rt.state.clear_slot(index, &mut inner);
    let monitor = rt.monitor(Some(env), &pid).ok_or(rustler::Error::BadArg)?;
    inner.alive = Arc::new(AtomicBool::new(true));
    inner.representative = Some((pid, monitor));
    Ok(atoms::ok())
}

/// Copy bytes into a new Erlang binary.
fn binary<'a>(env: Env<'a>, data: &[u8]) -> Binary<'a> {
    let mut bin = rustler::NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}

/// The representative drains up to Max events from its slot queue: subscribe, unsubscribe, publish.
#[rustler::nif]
fn take(env: Env, rt: ResourceArc<TokioRuntime>, slot: usize, max: usize) -> NifResult<Term> {
    if slot >= rt.state.slots.len() || max == 0 {
        return Err(rustler::Error::BadArg);
    }
    let (batch, more) = rt.state.take(slot, max);
    let batch: Vec<_> = batch
        .into_iter()
        .map(|event| match event {
            state::Event::Subscribe(filter) => {
                (atoms::subscribe(), binary(env, filter.as_bytes())).encode(env)
            }
            state::Event::Unsubscribe(filter) => {
                (atoms::unsubscribe(), binary(env, filter.as_bytes())).encode(env)
            }
            state::Event::Publish(client, msg) => (
                atoms::publish(),
                binary(env, client.as_bytes()),
                binary(env, msg.topic().as_bytes()),
                binary(env, &msg.payload),
                msg.qos,
                decode_properties(env, &msg.properties),
            )
                .encode(env),
        })
        .collect();
    Ok((batch, if more { atoms::more() } else { atoms::empty() }).encode(env))
}

/// The representative passes broker deliveries. Each goes to the slot's members of its filter.
#[rustler::nif]
fn deliver(
    rt: ResourceArc<TokioRuntime>,
    slot: usize,
    batch: Vec<(Binary, Binary, Binary, u8, Term)>,
) -> NifResult<rustler::Atom> {
    if slot >= rt.state.slots.len() || batch.len() > rt.state.limits.batch_size {
        return Err(rustler::Error::BadArg);
    }
    let batch = batch
        .iter()
        .map(|(filter, topic, payload, qos, properties)| {
            let filter =
                std::str::from_utf8(filter.as_slice()).map_err(|_| rustler::Error::BadArg)?;
            let topic_str = std::str::from_utf8(topic).map_err(|_| rustler::Error::BadArg)?;
            if !mqtt::valid_topic(topic_str) || topic.len() > u16::MAX as usize || *qos > 2 {
                return Err(rustler::Error::BadArg);
            }
            let properties = encode_properties(*properties);
            Ok((
                filter,
                mqtt::Message::copy(topic, &properties, payload, *qos),
            ))
        })
        .collect::<NifResult<Vec<_>>>()?;
    rt.state.deliver(slot, &batch);
    Ok(atoms::ok())
}

/// MQTT 5 PUBLISH property IDs and the keys of the EMQX property map.
fn property_keys() -> [(u8, rustler::Atom); 6] {
    [
        (1, atoms::payload_format_indicator()),
        (2, atoms::message_expiry_interval()),
        (3, atoms::content_type()),
        (8, atoms::response_topic()),
        (9, atoms::correlation_data()),
        (0x26, atoms::user_property()),
    ]
}

/// Convert a raw PUBLISH property block to the EMQX property map, or `undefined` when it is empty.
fn decode_properties<'a>(env: Env<'a>, block: &[u8]) -> Term<'a> {
    if block.is_empty() || block == [0] {
        return atoms::undefined().encode(env);
    }
    let mut map = rustler::types::map::map_new(env);
    let mut users = Vec::new();
    mqtt::walk_properties(block, mqtt::PropertyKind::Publish, |id, value| {
        let key = property_keys()
            .into_iter()
            .find(|(n, _)| *n == id)
            .unwrap()
            .1;
        let value = match value {
            mqtt::PropertyValue::Number(n) => n.encode(env),
            mqtt::PropertyValue::Binary(b) => binary(env, b).encode(env),
            mqtt::PropertyValue::Pair(k, v) => {
                users.push((binary(env, k), binary(env, v)));
                return;
            }
        };
        map = map.map_put(key, value).expect("property map");
    })
    .expect("validated PUBLISH properties");
    if !users.is_empty() {
        map = map
            .map_put(atoms::user_property(), users)
            .expect("property map");
    }
    map
}

/// Build a raw property block from the EMQX property map. Keys outside `property_keys` are ignored.
// Skip a property value that MQTT cannot encode, so that one message does not fail the batch.
fn encode_properties(properties: Term<'_>) -> bytes::Bytes {
    let fits = |b: &Binary| b.len() <= u16::MAX as usize;
    let mut out = Vec::new();
    for (id, key) in property_keys() {
        let Ok(value) = properties.map_get(key) else {
            continue;
        };
        match id {
            1 | 2 => {
                if let Ok(n) = value.decode::<u32>() {
                    if id == 2 || n <= 1 {
                        mqtt::encode_property(&mut out, id, mqtt::PropertyValue::Number(n));
                    }
                }
            }
            0x26 => {
                for pair in value.decode::<Vec<Term>>().unwrap_or_default() {
                    if let Ok((k, v)) = pair.decode::<(Binary, Binary)>() {
                        if fits(&k) && fits(&v) {
                            mqtt::encode_property(&mut out, id, mqtt::PropertyValue::Pair(&k, &v));
                        }
                    }
                }
            }
            _ => {
                if let Ok(b) = value.decode::<Binary>() {
                    if fits(&b) {
                        mqtt::encode_property(&mut out, id, mqtt::PropertyValue::Binary(&b));
                    }
                }
            }
        }
    }
    mqtt::property_block(&out)
}

/// Turn forwarding of native publishes to the broker on or off.
#[rustler::nif]
fn forwarding(rt: ResourceArc<TokioRuntime>, enabled: bool) -> rustler::Atom {
    rt.state.forwarding.store(enabled, Ordering::Release);
    atoms::ok()
}

/// Start, restart, or stop a listener in the background. Replies `{native_result, Ref, Result}`.
#[rustler::nif]
fn listener_apply(
    env: Env,
    rt: ResourceArc<TokioRuntime>,
    config: listener::Config,
    restart: bool,
    reply: LocalPid,
) -> Term {
    let mut msg_env = OwnedEnv::new();
    let reference = env.make_ref();
    let saved = msg_env.save(reference);
    let state = rt.state.clone();
    let listeners = rt.listeners.clone();
    rt.handle.spawn(async move {
        let result = listeners
            .lock()
            .await
            .apply(state, config, restart, reply)
            .await;
        let _ = msg_env.send_and_clear(&reply, |env| {
            let result = match result {
                Ok(()) => atoms::ok().encode(env),
                Err(e) => (atoms::error(), e).encode(env),
            };
            (atoms::native_result(), saved.load(env), result).encode(env)
        });
    });
    reference.encode(env)
}

/// Reply `{native_result, Ref, Ids}` with the IDs of the running listeners.
#[rustler::nif]
fn listener_status(env: Env, rt: ResourceArc<TokioRuntime>, reply: LocalPid) -> Term {
    let mut msg_env = OwnedEnv::new();
    let reference = env.make_ref();
    let saved = msg_env.save(reference);
    let listeners = rt.listeners.clone();
    rt.handle.spawn(async move {
        let running = listeners.lock().await.running();
        let _ = msg_env.send_and_clear(&reply, |env| {
            (atoms::native_result(), saved.load(env), running).encode(env)
        });
    });
    reference.encode(env)
}

/// Stop all listeners without publishing wills. Replies `{native_result, Ref, ok}`.
#[rustler::nif]
fn listeners_stop(env: Env, rt: ResourceArc<TokioRuntime>, reply: LocalPid) -> Term {
    let mut msg_env = OwnedEnv::new();
    let reference = env.make_ref();
    let saved = msg_env.save(reference);
    rt.state.quiet.store(true, Ordering::Release);
    let listeners = rt.listeners.clone();
    rt.handle.spawn(async move {
        listeners.lock().await.stop_all().await;
        let _ = msg_env.send_and_clear(&reply, |env| {
            (atoms::native_result(), saved.load(env), atoms::ok()).encode(env)
        });
    });
    reference.encode(env)
}

rustler::init!("emqx_tokio_nif", load = load);
