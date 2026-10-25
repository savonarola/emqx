mod atoms;
mod listener;
mod mqtt;
mod state;

use parking_lot::Mutex;
use rustler::{
    Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, OwnedEnv, ResourceArc, Term,
};
use state::{Limits, NodeState};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client, RedisResult};

pub struct TokioRuntime {
    state: Arc<NodeState>,
    handle: Handle,
    runtime: Mutex<Option<Runtime>>,
    listeners: Arc<tokio::sync::Mutex<listener::Manager>>,
}

impl TokioRuntime {
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl rustler::Resource for TokioRuntime {
    const IMPLEMENTS_DOWN: bool = true;

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
    fn drop(&mut self) {
        self.state.quiet.store(true, Ordering::Release);
        if let Some(rt) = self.runtime.lock().take() {
            std::thread::spawn(move || rt.shutdown_timeout(Duration::from_secs(5)));
        }
    }
}

pub struct RedisHandle {
    conn: MultiplexedConnection,
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    if env.register::<TokioRuntime>().is_err() {
        return false;
    }
    _ = rustler::resource!(RedisHandle, env);
    true
}

#[rustler::nif(schedule = "DirtyIo")]
fn runtime_new(env: Env) -> NifResult<Term> {
    new_runtime(env, Limits::default())
}

#[rustler::nif(schedule = "DirtyIo")]
fn native_new(env: Env, limits: Limits) -> NifResult<Term> {
    if limits.representatives == 0
        || limits.worker_threads == 0
        || limits.batch_size == 0
        || limits.output_limit == 0
        || limits.max_packet_size < 2
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

#[rustler::nif]
fn runtime_stop(rt: ResourceArc<TokioRuntime>, reply: LocalPid) -> rustler::Atom {
    rt.state.quiet.store(true, Ordering::Release);
    let runtime = rt.runtime.lock().take();
    std::thread::spawn(move || {
        if let Some(runtime) = runtime {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
        let _ = OwnedEnv::new().send_and_clear(&reply, |env| atoms::runtime_stopped().encode(env));
    });
    atoms::ok()
}

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

fn binary<'a>(env: Env<'a>, data: &[u8]) -> Binary<'a> {
    let mut bin = rustler::NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}

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
                binary(env, msg.topic.as_bytes()),
                binary(env, &msg.payload),
                msg.qos,
            )
                .encode(env),
        })
        .collect();
    Ok((batch, if more { atoms::more() } else { atoms::empty() }).encode(env))
}

#[rustler::nif]
fn deliver(
    rt: ResourceArc<TokioRuntime>,
    slot: usize,
    batch: Vec<(Binary, Binary, Binary, bool)>,
) -> NifResult<rustler::Atom> {
    if slot >= rt.state.slots.len() || batch.len() > rt.state.limits.batch_size {
        return Err(rustler::Error::BadArg);
    }
    let batch = batch
        .iter()
        .map(|(filter, topic, payload, retain)| {
            let filter =
                std::str::from_utf8(filter.as_slice()).map_err(|_| rustler::Error::BadArg)?;
            Ok((filter, mqtt::publish(topic, payload, *retain)))
        })
        .collect::<NifResult<Vec<_>>>()?;
    rt.state.deliver(slot, &batch);
    Ok(atoms::ok())
}

#[rustler::nif]
fn forwarding(rt: ResourceArc<TokioRuntime>, enabled: bool) -> rustler::Atom {
    rt.state.forwarding.store(enabled, Ordering::Release);
    atoms::ok()
}

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

#[rustler::nif(schedule = "DirtyIo")]
fn redis_connect(
    env: Env,
    tokio_runtime: ResourceArc<TokioRuntime>,
    url: String,
) -> NifResult<Term> {
    let rt = tokio_runtime.handle();

    let (tx, rx) = tokio::sync::oneshot::channel::<RedisResult<MultiplexedConnection>>();

    rt.spawn(async move {
        let client = Client::open(url).expect("bad redis url");
        let conn_res = client.get_multiplexed_async_connection().await;
        let _ = tx.send(conn_res);
    });

    let conn_res = rt.block_on(async { rx.await.expect("task dropped") });
    match conn_res {
        Ok(conn) => {
            let resource = ResourceArc::new(RedisHandle { conn });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn redis_get<'a>(
    env: Env<'a>,
    tokio_runtime: ResourceArc<TokioRuntime>,
    redis_handle: ResourceArc<RedisHandle>,
    key: Binary<'a>,
    receiver_pid: LocalPid,
) -> NifResult<Term<'a>> {
    let key = key.as_slice().to_vec();
    let rt = tokio_runtime.handle();
    let mut conn = redis_handle.conn.clone();
    let mut msg_env = OwnedEnv::new();

    let ref_term = env.make_ref();
    let saved_ref = msg_env.save(ref_term);

    rt.spawn(async move {
        let result: RedisResult<Option<Vec<u8>>> = conn.get(&key).await;
        let _ = msg_env.send_and_clear(&receiver_pid, |send_env| {
            let r = saved_ref.load(send_env);
            match result {
                Ok(Some(val)) => {
                    let mut bin = OwnedBinary::new(val.len()).unwrap();
                    bin.as_mut_slice().copy_from_slice(&val);
                    (atoms::ok(), r, bin.release(send_env)).encode(send_env)
                }
                Ok(None) => (atoms::ok(), r, atoms::undefined()).encode(send_env),
                Err(e) => (atoms::error(), r, e.to_string()).encode(send_env),
            }
        });
    });

    Ok((atoms::ok(), ref_term).encode(env))
}

#[rustler::nif]
fn redis_set<'a>(
    env: Env<'a>,
    tokio_runtime: ResourceArc<TokioRuntime>,
    redis_handle: ResourceArc<RedisHandle>,
    key: Binary<'a>,
    value: Binary<'a>,
    receiver_pid: LocalPid,
) -> NifResult<Term<'a>> {
    let key = key.as_slice().to_vec();
    let value = value.as_slice().to_vec();
    let rt = tokio_runtime.handle();
    let mut conn = redis_handle.conn.clone();
    let mut msg_env = OwnedEnv::new();

    let ref_term = env.make_ref();
    let saved_ref = msg_env.save(ref_term);

    rt.spawn(async move {
        let result: RedisResult<()> = conn.set(&key, &value).await;
        let _ = msg_env.send_and_clear(&receiver_pid, |send_env| {
            let r = saved_ref.load(send_env);
            match result {
                Ok(_) => (atoms::ok(), r).encode(send_env),
                Err(e) => (atoms::error(), r, e.to_string()).encode(send_env),
            }
        });
    });

    Ok((atoms::ok(), ref_term).encode(env))
}

rustler::init!("emqx_tokio_nif", load = load);
