#!/usr/bin/env python3
"""Compare native and regular QoS 0 delivery on isolated local test nodes."""

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[3]
NATIVE_PORT = 21883
REGULAR_PORT = 21884
# Publisher and subscriber ports. Every mode except native-only enables the regular listener, so
# native publishes are forwarded to the broker.
MODES = {
    "native-only": (NATIVE_PORT, NATIVE_PORT),
    "native-forwarding": (NATIVE_PORT, NATIVE_PORT),
    "native-to-regular": (NATIVE_PORT, REGULAR_PORT),
    "regular": (REGULAR_PORT, REGULAR_PORT),
}


def utf(value):
    return len(value).to_bytes(2, "big") + value


def packet(header, body):
    length = len(body)
    encoded = bytearray([header])
    while True:
        byte = length % 128
        length //= 128
        encoded.append(byte | (128 if length else 0))
        if not length:
            return bytes(encoded) + body


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed before all messages arrived")
        data.extend(chunk)
    return data


def connect(port, client):
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.sendall(packet(0x10, utf(b"MQTT") + b"\x04\x02\x00\x00" + utf(client)))
    assert receive(sock, 4) == b"\x20\x02\x00\x00"
    return sock


def pair(ports, index, wildcard):
    publisher_port, subscriber_port = ports
    topic = f"bench/{index}/data".encode()
    subscriber = connect(subscriber_port, f"sub-{index}".encode())
    topic_filter = f"bench/{index}/+".encode() if wildcard else topic
    subscriber.sendall(packet(0x82, b"\x00\x01" + utf(topic_filter) + b"\x00"))
    assert receive(subscriber, 5) == b"\x90\x03\x00\x01\x00"
    publisher = connect(publisher_port, f"pub-{index}".encode())
    return publisher, subscriber, topic


def transfer(connection, count, window, payload_size):
    publisher, subscriber, topic = connection
    for offset in range(0, count, window):
        batch = b"".join(
            packet(0x30, utf(topic) + i.to_bytes(8, "big") + b"x" * (payload_size - 8))
            for i in range(offset, min(count, offset + window))
        )
        publisher.sendall(batch)
        assert receive(subscriber, len(batch)) == batch, "lost, reordered, or duplicate delivery"


def measure(ports, args):
    connections = [pair(ports, i, args.wildcard) for i in range(args.pairs)]
    # SUBACK precedes representative registration on native listeners.
    time.sleep(0.2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.pairs) as pool:
        def run(count):
            jobs = [pool.submit(transfer, c, count, args.window, args.payload) for c in connections]
            for job in jobs:
                job.result()

        run(1000)
        rates = []
        for _ in range(args.repeats):
            start = time.perf_counter()
            run(args.messages)
            rates.append(round(args.pairs * args.messages / (time.perf_counter() - start)))

    publisher, subscriber, topic = connections[0]
    wire = packet(0x30, utf(topic) + b"x" * args.payload)
    latency = []
    for _ in range(2000):
        start = time.perf_counter_ns()
        publisher.sendall(wire)
        assert receive(subscriber, len(wire)) == wire
        latency.append((time.perf_counter_ns() - start) / 1000)
    for publisher, subscriber, _ in connections:
        for sock in (publisher, subscriber):
            sock.sendall(b"\xe0\x00")
            sock.close()
    latency.sort()
    return {"messages_per_second": rates, "median_messages_per_second": statistics.median(rates),
            "roundtrip_us": {str(p): round(latency[int(len(latency) * p / 100)], 1) for p in (50, 95, 99)}}


def broker(mode, directory):
    regular = "true" if mode != "native-only" else "false"
    config = f"""
    Disabled = #{{<<"default">> => #{{<<"enable">> => false}}}},
    Config = #{{<<"listeners">> => #{{
        <<"ws">> => Disabled, <<"wss">> => Disabled, <<"ssl">> => Disabled,
        <<"tcp">> => #{{<<"default">> => #{{<<"enable">> => {regular},
            <<"bind">> => <<"127.0.0.1:{REGULAR_PORT}">>, <<"enable_authn">> => false}}}},
        <<"native">> => #{{<<"bench">> => #{{<<"bind">> => <<"127.0.0.1:{NATIVE_PORT}">>,
            <<"enable_unauthenticated">> => true}}}}
    }}}},
    Apps = emqx_cth_suite:start([{{emqx_conf, #{{config => Config}}}},
        {{emqx_tokio, #{{override_env => [{{representatives, 4}}, {{worker_threads, 4}},
            {{output_limit, 65536}}]}}}}],
        #{{work_dir => "{directory}"}}),
    io:format("NATIVE_BENCH_READY~n"),
    io:get_line(""),
    emqx_cth_suite:stop(Apps), halt().
    """
    evaluate = f'''
    {{:ok, tokens, _}} = :erl_scan.string(~C"""
{config}
""")
    {{:ok, expressions}} = :erl_parse.parse_exprs(tokens)
    :erl_eval.exprs(expressions, [])
    '''
    command = ["elixir", "--erl", "+S 4:4 +SDcpu 2 +SDio 2", "--name",
               f"native_bench_{os.getpid()}@127.0.0.1", "-pa",
               str(ROOT / "_build/emqx-enterprise-test/lib/*/ebin"), "-e", evaluate]
    return subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            env={**os.environ, "ERL_CRASH_DUMP": str(Path(directory) / "erl_crash.dump")})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--messages", type=int, default=20000, help="messages per pair per repeat")
    parser.add_argument("--window", type=int, default=256)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--wildcard", action="store_true")
    parser.add_argument("--smoke", action="store_true", help="also publish with MQTTX on each node")
    args = parser.parse_args()
    assert args.payload >= 8 and min(args.pairs, args.messages, args.window, args.repeats) > 0
    print(json.dumps({"parameters": vars(args)}), flush=True)
    for mode, ports in MODES.items():
        with tempfile.TemporaryDirectory(prefix="native-mqtt-") as directory:
            process = broker(mode, directory)
            try:
                startup = []
                for line in process.stdout:
                    startup.append(line)
                    if line.strip() == "NATIVE_BENCH_READY":
                        break
                else:
                    raise RuntimeError(f"broker failed to start ({process.wait()}): {''.join(startup)}")
                if args.smoke:
                    pub, sub, topic = pair(ports, "smoke", False)
                    time.sleep(0.2)
                    result = subprocess.run(["mqttx", "pub", "-h", "127.0.0.1", "-p", str(ports[0]),
                        "-V", "3.1.1", "-q", "0", "-t", topic.decode(), "-m", "hello"],
                        check=True, capture_output=True, text=True, timeout=20)
                    assert "Error:" not in result.stdout + result.stderr, result
                    expected = packet(0x30, utf(topic) + b"hello")
                    assert receive(sub, len(expected)) == expected
                    pub.close()
                    sub.close()
                print(json.dumps({"mode": mode, **measure(ports, args)}), flush=True)
            finally:
                try:
                    process.communicate("\n", timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
