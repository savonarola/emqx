# xmq point-to-point TLS benchmark: classic channels and native-only routing

This benchmark replicates the "Point-to-Point over TLS 1.3" test of
<https://xmq.sptk.net/xmq_tests_encryption> on one host. It compares two broker
modes:

- **Classic:** regular EMQX TLS listener. `emqx_tokio` does not run, so
  `emqx_broker` does not look up native subscribers.
- **Native-only:** native (Rust) TLS listener. Forwarding to the broker is off,
  so messages do not enter the BEAM.

Status: complete. All runs finished, and the host tuning was restored.

## Summary

| Rate | Mode | Achieved | Median latency | Steady CPU | Steady RSS | Per connection |
| --- | --- | --- | --- | --- | --- | --- |
| 50K | Classic | 49,958/s | 6.0 ms | 775% | 8.54 GB | 159 KB |
| 50K | Native-only | 49,983/s | 64 us | 236% | 2.37 GB | 38 KB |
| 75K | Classic | 52,799/s (below target) | 99.5 s | 800% | 18.28 GB | 236 KB |
| 75K | Native-only | 74,964/s | 112 us | 358% | 3.44 GB | 40 KB |
| 100K | Classic | not run | | | | |
| 100K | Native-only | 99,931/s | 276 us | 469% | 4.52 GB | 41 KB |

- At 50K, native-only uses 3.3 times less CPU and 3.6 times less memory than
  classic. Its median latency is 93 times lower.
- Classic reaches the CPU limit of 8 broker threads at about 50K. At 75K it
  delivers 52.8K/s, and the latency grows for the whole run.
- Native-only reaches 100K at 59% of the broker CPUs. Memory grows linearly at
  about 40 KB per connection.
- Almost all native-only memory is outside the BEAM: 4.33 GB of 4.52 GB at
  100K. The BEAM part stays at about 0.18 GB.

## Setup

| Item | Value |
| --- | --- |
| Date | 2026-10-25 to 2026-10-26 |
| Code | branch `20261023-r70-rust-listener`, commits `7f7c3738ad` and `e087fdd94f`. The runs used that code before two later changes in `e087fdd94f`: code comments and the removal of the unused Redis NIFs. |
| Build | test profile (`_build/emqx-enterprise-test`), started with `emqx_cth_suite`; release NIF |
| Host | AMD Ryzen 7 8745HS, 8 cores, 16 threads, 58 GiB RAM, Linux 7.0.0-38-generic, OTP 28 |
| Broker CPUs | 0-3 and 8-11 (4 cores, 8 threads), `+S 8:8` |
| Client CPUs | 4-7 and 12-15, `xmq_scn` 1.0.0 in Docker, host network |
| Client image | `alexeyparshin/xmq@sha256:dd4a1a857ac6d14c74cac0cc833fac9137954681e1933c6892c1023b358aa79c` |
| Source addresses | 127.0.0.11 to 127.0.0.14 (`--bind-to-interfaces`) |
| Host tuning during runs | `somaxconn` 65535, `tcp_max_syn_backlog` 65535, `ip_local_port_range` 1024-65535; restored after the runs |
| Runner | `apps/emqx_tokio/test/native_mqtt5_bench.py` |

Scenario, the same as `Point-To-Point-TLS-<N>K.json` of the reference:

- MQTT 5, QoS 1, clean start, 16-byte payload, 2 messages a second per publisher.
- One topic per publisher and subscriber pair: 25,000, 37,500, and 50,000 pairs
  for 50K, 75K, and 100K messages a second.
- Connect rate 10,000 a second, keepalive 300 s, client inflight limit 1,024.
- 600 s per rate. A higher rate runs only when the median interval latency at
  the previous rate is below 100 ms.
- TLS 1.3 on both sides, self-signed certificate.

Broker limits, the same as the reference EMQX configuration: `mqtt.max_inflight`
128, `mqtt.max_mqueue_len` 100,000. The native listener uses `max_inflight` 128.

## Metrics

- **Achieved rate, latency:** from `xmq_scn`. Latency is publish to receive.
  The median is the median of the interval latencies, as `xmq_scn` reports it.
- **CPU:** broker process CPU from `/proc`. "Steady" is the mean over the steady
  window. 800% is all broker CPUs.
- **Steady window:** 300 s that end 30 s before the client exits. It excludes the
  connect and disconnect phases.
- **RSS:** broker process RSS. "Idle" is before clients connect. "Steady" is the
  median over the steady window.
- **Per connection:** (steady RSS - idle RSS) / connections.
- **BEAM:** `erlang:memory(total)`, median over the steady window.
- **Outside BEAM:** steady RSS - BEAM total. For native-only, this is mostly the
  Rust listener and its TLS state.

## Results

### 50K messages a second (25,000 pairs, 50,000 connections)

| Mode | Achieved | Mean latency | Median latency | Steady CPU | Steady RSS | Per connection | BEAM | Outside BEAM | Processes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Classic | 49,958/s | 8.4 ms | 6.0 ms | 775% | 8.54 GB | 159 KB | 6.00 GB | 2.54 GB | 200,450 |
| Native-only | 49,983/s | 65 us | 64 us | 236% | 2.37 GB | 38 KB | 0.17 GB | 2.19 GB | 443 |

### 75K messages a second (37,500 pairs, 75,000 connections)

| Mode | Achieved | Mean latency | Median latency | Steady CPU | Steady RSS | Per connection | BEAM | Outside BEAM | Processes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Classic | 52,799/s (below target) | 91 s | 99.5 s | 800% | 18.28 GB | 236 KB | 9.36 GB | 8.91 GB | 298,871 |
| Native-only | 74,964/s | 114 us | 112 us | 358% | 3.44 GB | 40 KB | 0.18 GB | 3.27 GB | 443 |

Classic saturates all broker CPUs at 75K. Messages queue, and the latency grows
for the whole run. The classic series stops here; 100K is not run.

### 100K messages a second (50,000 pairs, 100,000 connections)

| Mode | Achieved | Mean latency | Median latency | Steady CPU | Steady RSS | Per connection | BEAM | Outside BEAM | Processes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Classic | not run: median at 75K over 100 ms | | | | | | | | |
| Native-only | 99,931/s | 317 us | 276 us | 469% | 4.52 GB | 41 KB | 0.18 GB | 4.33 GB | 443 |

### Connect phase

Every client connected in every run. `xmq_scn` asks for 10,000 connections a
second. The achieved connect rate was about 3,500 a second for classic and about
7,100 a second for native-only. TLS handshakes limit both.

## Reference results

From <https://xmq.sptk.net/xmq_tests_encryption>, EMQX 6.3.1 at 50K messages a
second. Host: AWS c5n.4xlarge, 16 vCPU (8 physical cores, 2 threads each), about
40 GB RAM, no CPU affinity. The client ran on a separate instance.

| Broker | Achieved | Mean latency | Median latency | CPU | RSS |
| --- | --- | --- | --- | --- | --- |
| EMQX 6.3.1 | 49,650/s | 2.6 s | 2.9 s | 1300% mean, 1623% peak | 9.49 GB median, 10.44 GB peak |
| XMQ 0.9.20 | 49,976/s | 239 us | 239 us | 345% mean | 1.62 GB |
| FlashMQ 1.27.2 | 49,978/s | 225 us | | 352% mean | 2.65 GB |
| HiveMQ 2026.5 | 49,962/s | 423 us | | 626% mean | 4.35 GB |

EMQX did not run 75K and 100K in the reference, because its median at 50K was
over 100 ms. In the reference, 27 EMQX clients did not get CONNACK within 60 s.

Absolute numbers are not comparable with the reference. The reference uses a
different CPU, a real network, and a separate client host. The comparison
between classic and native-only uses the same conditions.

## Files

`raw/` holds the run data. Certificates, keys, and broker work directories are
not kept.

| File | Content |
| --- | --- |
| `raw/environment.json` | Runner arguments, sysctl values before and during the runs, load, memory, EPMD, ports |
| `raw/runner.log` | One summary line per finished rate |
| `raw/<mode>-<rate>/scenario.json` | Scenario passed to `xmq_scn` |
| `raw/<mode>-<rate>/client.log` | `xmq_scn` output: connect phase and publish intervals |
| `raw/<mode>-<rate>/summary.json` | Summary of the rate |
| `raw/<mode>-<rate>/resources.json` | `[time, cpu seconds, rss bytes]` every second |
| `raw/<mode>-<rate>/memory.csv` | `time ms, total, processes, binary, ets, system, process count, port count` every second |
| `raw/<mode>-<rate>/broker.log` | Broker console output |
