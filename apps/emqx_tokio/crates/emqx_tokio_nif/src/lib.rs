mod atoms;

use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, OwnedEnv, ResourceArc, Term};
use tokio::runtime::Runtime;

use redis::{AsyncCommands, Client, RedisResult};
use redis::aio::MultiplexedConnection;


pub struct TokioRuntime {
    pub rt: Runtime,
}

impl TokioRuntime {
    pub fn handle(&self) -> &Runtime {
        &self.rt
    }
}

pub struct RedisHandle {
    conn: MultiplexedConnection,
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    _ = rustler::resource!(TokioRuntime, env);
    _ = rustler::resource!(RedisHandle, env);
    true
}

#[rustler::nif]
fn runtime_new(env: Env) -> NifResult<Term> {
    match Runtime::new() {
        Ok(rt) => {
            let resource = ResourceArc::new(TokioRuntime { rt });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn redis_connect(env: Env, tokio_runtime: ResourceArc<TokioRuntime>, url: String) -> NifResult<Term> {
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
                Err(e) => (atoms::error(), r,e.to_string()).encode(send_env),
            }
        });
    });

    Ok((atoms::ok(), ref_term).encode(env))
}

rustler::init!("emqx_tokio_nif", load = load);
