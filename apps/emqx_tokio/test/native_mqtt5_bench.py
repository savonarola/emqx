#!/usr/bin/env python3
"""Run the MQTT 5 QoS 1 point-to-point TLS benchmark with xmq_scn."""

import argparse
import json
import os
from pathlib import Path
import re
import resource
import socket
import statistics
import subprocess
import threading
import time


ROOT = Path(__file__).resolve().parents[3]
IMAGE = "alexeyparshin/xmq@sha256:dd4a1a857ac6d14c74cac0cc833fac9137954681e1933c6892c1023b358aa79c"
# Built from the FlashMQ apt repository; see FLASHMQ_DOCKERFILE.
FLASHMQ_IMAGE = "native-bench/flashmq:1.27.3"
FLASHMQ_DOCKERFILE = """FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg libssl3t64 \\
 && curl -fsSL https://www.flashmq.org/flashmq-repo.gpg > /usr/share/keyrings/flashmq-repo.gpg \\
 && gpg --show-keys --with-colons /usr/share/keyrings/flashmq-repo.gpg \\
    | grep -q 874E6552A197BDD069A1526E1788BCA932A92BDF \\
 && echo "deb [signed-by=/usr/share/keyrings/flashmq-repo.gpg] http://repo.flashmq.org/apt noble main" \\
    > /etc/apt/sources.list.d/flashmq.list \\
 && apt-get update && cd /tmp && apt-get download flashmq \\
 && dpkg-deb -x flashmq_*.deb / && rm -f flashmq_*.deb && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/bin/flashmq"]
"""
PORT = 21883
MODES = ["classic", "native-only", "xmq", "flashmq"]
PUBLISH_RATE = 2
MAX_INFLIGHT = 128
# erlang:memory/0 keys sampled every second, written as atoms into the broker code.
BEAM_MEMORY = ("total", "processes", "binary", "ets", "system")
# Memory is summarized over a window that ends this many seconds before the client exits,
# so that the disconnect phase is excluded.
STEADY_MARGIN = 30
SOURCE_ADDRESSES = "127.0.0.11,127.0.0.12,127.0.0.13,127.0.0.14"
TUNING = {
    "net.ipv4.ip_local_port_range": "1024 65535",
    "net.core.somaxconn": "65535",
    "net.ipv4.tcp_max_syn_backlog": "65535",
}


def command(args):
    return subprocess.check_output(args, text=True).strip()


def cpu_count(cpus):
    count = 0
    for part in cpus.split(","):
        first, _, last = part.partition("-")
        count += int(last or first) - int(first) + 1
    return count


class EmqxBroker:
    """An EMQX node from the test build, stopped through its standard input."""

    def __init__(self, process, reader):
        self.process, self.reader, self.pid = process, reader, process.pid

    def stop(self):
        self.process.stdin.write("\n")
        self.process.stdin.flush()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.reader.join()


class DockerBroker:
    """A broker container with host networking, pinned to the broker CPUs."""

    def __init__(self, directory, cpus, image, args, entrypoint=None):
        self.directory = directory
        self.name = f"native-mqtt5-broker-{os.getpid()}"
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "--network", "host", "--cpuset-cpus", cpus,
             "--ulimit", "nofile=1048576:1048576", "--user", f"{os.getuid()}:{os.getgid()}",
             "-v", f"{directory}:/results",
             *(["--entrypoint", entrypoint] if entrypoint else []), image, *args],
            check=True, capture_output=True,
        )
        self.pid = int(command(["docker", "inspect", "-f", "{{.State.Pid}}", self.name]))
        wait_for_port(directory, self.name)

    def stop(self):
        with (self.directory / "broker.log").open("w") as log:
            subprocess.run(["docker", "logs", self.name], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def wait_for_port(directory, name):
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            return
        except OSError:
            time.sleep(0.2)
    subprocess.run(["docker", "logs", name], stdout=(directory / "broker.log").open("w"),
                   stderr=subprocess.STDOUT)
    raise RuntimeError(f"broker startup failed: {directory}/broker.log")


def start_xmq(directory, cpus, image):
    # The packaged configuration of the reference, with the thread counts of its 8-thread bench host.
    config = {
        "connections": {
            "listener": [{"id": 1, "name": "Encrypted", "bind_ip": "127.0.0.1", "port": PORT,
                          "threads": 4, "protocol": "MQTT+SSL", "enable": True}],
            "ssl_keys": {"cafile": "", "keyfile": "/results/key.pem", "certfile": "/results/cert.pem",
                         "verify_depth": 0},
        },
        "queue_limits": {"max_size": 50000, "max_inflight_messages": 32768},
        "server_limits": {"send_threads": 3, "receive_threads": 4, "delivery_threads": 2,
                          "max_topic_alias": 128},
        "authentication": {"allow_anonymous": True},
        "persistence": {"redis_uri": "redis://localhost:6379", "clean_start": True, "enabled": False,
                        "max_redis_connections": 2, "max_queued_writes": 1000},
        "logging": {"log_to": "/tmp/xmq_server.log", "keep_logs": 1, "min_log_level": "INFO",
                    "log_level": {"connect": "DEBUG", "disconnect": "DEBUG", "subscribe": "DEBUG",
                                  "unsubscribe": "DEBUG", "publish": "ERROR", "ack": "ERROR",
                                  "server_connections": "DEBUG", "server_events": "DEBUG",
                                  "cluster_connections": "INFO", "cluster_events": "INFO"}},
        "web_service": {"listener_port": 21884, "encrypted": True},
        "bridges": [],
        "cluster": {"this_node": {"node_name": "primary", "host_port": "localhost:21885", "encrypted": True},
                    "nodes": []},
    }
    (directory / "xmq_server.conf").write_text(json.dumps(config, indent=2) + "\n")
    return DockerBroker(directory, cpus, image, ["-c", "/results/xmq_server.conf", "-l", "/tmp/xmq.log"],
                        entrypoint="xmq_server")


def start_flashmq(directory, cpus):
    # The reference FlashMQ configuration with one thread per broker CPU, and a TLS listener.
    (directory / "flashmq.conf").write_text(f"""log_file /tmp/flashmq.log
log_level warning
log_subscriptions false
log_publishes false
thread_count {cpu_count(cpus)}
max_qos_msg_pending_per_client 65535
max_qos_bytes_pending_per_client 268435456
listen {{
  protocol mqtt
  inet_protocol ip4
  inet4_bind_address 127.0.0.1
  port {PORT}
  fullchain /results/cert.pem
  privkey /results/key.pem
  allow_anonymous true
  tcp_nodelay true
}}
""")
    if subprocess.run(["docker", "image", "inspect", FLASHMQ_IMAGE], capture_output=True).returncode:
        subprocess.run(["docker", "build", "-q", "-t", FLASHMQ_IMAGE, "-"], input=FLASHMQ_DOCKERFILE,
                       text=True, check=True, capture_output=True)
    return DockerBroker(directory, cpus, FLASHMQ_IMAGE, ["--config-file", "/results/flashmq.conf"])


def start_broker(mode, directory, cpus, image):
    if mode == "xmq":
        return start_xmq(directory, cpus, image)
    if mode == "flashmq":
        return start_flashmq(directory, cpus)
    return start_emqx(mode, directory, cpus)


def start_emqx(mode, directory, cpus):
    native = mode == "native-only"
    schedulers = cpu_count(cpus)
    ssl = f"""#{{<<"certfile">> => <<"{directory}/cert.pem">>,
        <<"keyfile">> => <<"{directory}/key.pem">>}}"""
    listener = (
        f"""<<"native">> => #{{<<"bench">> => #{{<<"bind">> => <<"127.0.0.1:21883">>,
            <<"enable_unauthenticated">> => true,
            <<"ssl_options">> => maps:put(<<"enable">>, true, {ssl})}}}},
            <<"ssl">> => Disabled"""
        if native else
        f"""<<"native">> => #{{}}, <<"ssl">> => #{{<<"default">> => #{{
            <<"bind">> => <<"127.0.0.1:21883">>, <<"enable">> => true, <<"enable_authn">> => false,
            <<"ssl_options">> => maps:put(<<"versions">>, [<<"tlsv1.3">>], {ssl})}}}}"""
    )
    # The inflight and queue limits match the EMQX configuration of the reference benchmark.
    apps = f""", {{emqx_tokio, #{{override_env => [{{max_inflight, {MAX_INFLIGHT}}}]}}}}""" if native else ""
    code = f"""
    Disabled = #{{<<"default">> => #{{<<"enable">> => false}}}},
    Config = #{{<<"listeners">> => #{{<<"tcp">> => Disabled, <<"ws">> => Disabled,
        <<"wss">> => Disabled, {listener}}},
        <<"mqtt">> => #{{<<"max_inflight">> => {MAX_INFLIGHT}, <<"max_mqueue_len">> => 100000}}}},
    Apps = emqx_cth_suite:start([{{emqx_conf, #{{config => Config}}}}, {{emqx, #{{config => false}}}}{apps}],
        #{{work_dir => "{directory}/broker"}}),
    ok = socket:use_registry(false),
    ok = emqx_listeners:start(),
    true = emqx_listeners:is_running('{"native:bench" if native else "ssl:default"}'),
    {str(native).lower()} = is_pid(whereis(emqx_tokio_runtime)),
    {str(native).lower()} = persistent_term:get('$emqx_broker__native_dispatch', false),
    {{ok, MemoryFile}} = file:open("{directory}/memory.csv", [write]),
    _ = spawn(fun() ->
        Sample = fun Sample() ->
            Memory = erlang:memory(),
            Values = [proplists:get_value(Key, Memory) || Key <- {list(BEAM_MEMORY)}],
            io:format(MemoryFile, "~b,~b,~b,~b,~b,~b,~b,~b~n", [erlang:system_time(millisecond) | Values] ++
                [erlang:system_info(process_count), erlang:system_info(port_count)]),
            timer:sleep(1000),
            Sample()
        end,
        Sample()
    end),
    io:format("TLS_BENCH_READY~n"),
    io:get_line(""),
    emqx_cth_suite:stop(Apps), halt().
    """
    evaluate = f'''
    {{:ok, tokens, _}} = :erl_scan.string(~C"""
{code}
""")
    {{:ok, expressions}} = :erl_parse.parse_exprs(tokens)
    :erl_eval.exprs(expressions, [])
    '''
    process = subprocess.Popen(
        ["taskset", "-c", cpus, "elixir", "--erl", f"+S {schedulers}:{schedulers} +P 4194304 +Q 2097152",
         "--name", f"native_tls_bench_{os.getpid()}@127.0.0.1", "-pa",
         str(ROOT / "_build/emqx-enterprise-test/lib/*/ebin"), "-e", evaluate],
        cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env={**os.environ, "ERL_CRASH_DUMP": str(directory / "erl_crash.dump")},
    )
    log = (directory / "broker.log").open("w")
    for line in process.stdout:
        log.write(line)
        log.flush()
        if line.strip() == "TLS_BENCH_READY":
            break
    else:
        log.close()
        raise RuntimeError(f"broker startup failed: {directory}/broker.log")

    def drain():
        for line in process.stdout:
            log.write(line)
            log.flush()
        log.close()

    thread = threading.Thread(target=drain)
    thread.start()
    return EmqxBroker(process, thread)


def sample(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    seconds = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    rss = int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
    return time.time(), seconds, rss


def latency_ms(value):
    match = re.fullmatch(r"([\d.]+)(ns|us|µs|ms|s)", value)
    if not match:
        raise ValueError(f"unrecognized latency: {value}")
    return float(match[1]) * {"ns": 0.000001, "us": 0.001, "µs": 0.001, "ms": 1, "s": 1000}[match[2]]


def memory_summary(directory, samples, start, end, connections):
    """Summarize memory: the idle RSS, and medians over the steady publish window [start, end]."""
    idle_rss = samples[0][2]
    steady_rss = statistics.median(s[2] for s in samples if start <= s[0] <= end)
    rss = {
        "broker_idle_rss_bytes": idle_rss,
        "broker_steady_rss_bytes": steady_rss,
        "broker_rss_per_connection_bytes": (steady_rss - idle_rss) / connections,
    }
    # Only EMQX brokers write BEAM memory samples.
    if not (directory / "memory.csv").exists():
        return rss
    columns = ["time", *BEAM_MEMORY, "process_count", "port_count"]
    # The broker can stop in the middle of the last line.
    rows = [dict(zip(columns, map(int, line.split(","))))
            for line in (directory / "memory.csv").read_text().splitlines()
            if line.count(",") == len(columns) - 1]
    steady = [row for row in rows if start <= row["time"] / 1000 <= end]
    beam = {key: statistics.median(row[key] for row in steady) for key in columns[1:]}
    return {
        **rss,
        **{f"beam_steady_{key}_bytes": beam[key] for key in BEAM_MEMORY},
        # Memory outside the BEAM allocators: the native runtime, TLS, and the C allocator.
        "non_beam_steady_bytes": steady_rss - beam["total"],
        "beam_steady_process_count": beam["process_count"],
        "beam_steady_port_count": beam["port_count"],
    }


def run_rate(args, mode, rate, directory):
    directory.mkdir(parents=True)
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
         "-keyout", str(directory / "key.pem"),
         "-out", str(directory / "cert.pem")], check=True, capture_output=True,
    )
    (directory / "openssl.cnf").write_text("""openssl_conf = default_conf
[default_conf]
ssl_conf = ssl_section
[ssl_section]
system_default = tls_defaults
[tls_defaults]
MinProtocol = TLSv1.3
MaxProtocol = TLSv1.3
""")
    # Point-To-Point-TLS-<N>K.json of the reference: two messages a second per publisher.
    pairs = rate // PUBLISH_RATE
    group = {"protocol_version": 5, "client_count": pairs, "qos": 1,
             "clean_session": True, "topics": "test/topic$clientindex"}
    scenario = {
        "name": f"Point-To-Point-TLS-{rate // 1000}K-{mode}", "type": "Point-To-Point",
        "publishers": {**group, "id_prefix": "test-publisher-"},
        "subscribers": {**group, "id_prefix": "test-subscriber-"},
        "server": {"hostname": "127.0.0.1", "port": PORT, "username": "", "password": ""},
        "parameters": {"connection_rate": args.connection_rate, "duration_sec": args.duration,
                       "payload_size": 16, "publish_rate": PUBLISH_RATE,
                       "keep_alive_sec": 300, "max_inflight_messages": 1024},
    }
    (directory / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n")
    broker = start_broker(mode, directory, args.broker_cpus, args.image)
    samples = []
    stopped = threading.Event()

    def monitor():
        while not stopped.wait(1):
            samples.append(sample(broker.pid))

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()
    container = f"native-mqtt5-bench-{os.getpid()}"
    try:
        with (directory / "client.log").open("w") as log:
            subprocess.run(
                ["docker", "run", "--rm", "--name", container, "--network", "host", "--cpuset-cpus", args.client_cpus,
                 "--ulimit", "nofile=1048576:1048576", "-v", f"{directory}:/results:ro",
                 "-e", "OPENSSL_CONF=/results/openssl.cnf", "--entrypoint", "xmq_scn", args.image,
                 "--scenario", "/results/scenario.json", "--encrypted",
                 "--cafile", "/results/cert.pem",
                 # One source address has fewer ports than 75,000 or 100,000 connections need.
                 "--bind-to-interfaces", SOURCE_ADDRESSES,
                 "--result-interval", f"{max(1, args.duration // 10)}s"],
                stdout=log, stderr=subprocess.STDOUT, check=True,
                timeout=args.duration + 2 * pairs / args.connection_rate + 600,
            )
        finished = time.time()
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        stopped.set()
        monitor_thread.join()
        (directory / "resources.json").write_text(json.dumps(samples) + "\n")
        broker.stop()

    output = (directory / "client.log").read_text()
    publish_output = output.rsplit(f"Scenario: {scenario['name']}\n", 1)[1]
    averages = re.findall(r"^Average\s+(\d+)\s+(\S+)\s+(\d+)\s*$", publish_output, re.MULTILINE)
    medians = re.findall(r"^Median\s+(\S+)\s*$", publish_output, re.MULTILINE)
    count, latency, achieved = averages[-1]
    cpu = [(b[0], 100 * (b[1] - a[1]) / (b[0] - a[0])) for a, b in zip(samples, samples[1:])]
    steady_start, steady_end = finished - STEADY_MARGIN - args.duration / 2, finished - STEADY_MARGIN
    summary = {
        "mode": mode, "target_rate": rate, "received": int(count), "achieved_rate": int(achieved),
        "mean_latency_ms": latency_ms(latency), "median_interval_latency_ms": latency_ms(medians[-1]),
        "broker_mean_cpu_percent": statistics.mean(c for _, c in cpu),
        "broker_steady_cpu_percent": statistics.mean(c for t, c in cpu if steady_start <= t <= steady_end),
        "broker_peak_rss_bytes": max(s[2] for s in samples),
        **memory_summary(directory, samples, steady_start, steady_end, 2 * pairs),
    }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--broker-cpus", required=True)
    parser.add_argument("--client-cpus", required=True)
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--connection-rate", type=int, default=10000)
    parser.add_argument("--rates", nargs="+", type=int, default=[50000, 75000, 100000])
    parser.add_argument("--modes", nargs="+", choices=MODES, default=["classic", "native-only"])
    args = parser.parse_args()
    args.results = args.results.resolve()
    assert all(rate > 0 and rate % PUBLISH_RATE == 0 for rate in args.rates)
    assert args.duration > 0 and args.connection_rate > 0
    resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
    original = {key: command(["sysctl", "-n", key]) for key in TUNING}
    args.results.mkdir(parents=True, exist_ok=True)
    (args.results / "environment.json").write_text(json.dumps({
        "arguments": {**vars(args), "results": str(args.results)}, "sysctl_before": original,
        "sysctl_during": TUNING, "load": command(["uptime"]), "memory": command(["free", "-h"]),
        "epmd": command(["epmd", "-names"]), "ports": command(["ss", "-ltn"]),
    }, indent=2) + "\n")
    try:
        for key, value in TUNING.items():
            command(["sudo", "-n", "sysctl", "-w", f"{key}={value}"])
        for mode in args.modes:
            for rate in args.rates:
                summary = run_rate(args, mode, rate, args.results / f"{mode}-{rate}")
                if summary["median_interval_latency_ms"] >= 100:
                    break
    finally:
        for key, value in original.items():
            command(["sudo", "-n", "sysctl", "-w", f"{key}={value}"])


if __name__ == "__main__":
    main()
