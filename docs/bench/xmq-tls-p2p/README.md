# xmq point-to-point TLS benchmark: classic channels and native-only routing

This benchmark replicates the "Point-to-Point over TLS 1.3" test of
<https://xmq.sptk.net/xmq_tests_encryption> on one host. It compares two broker
modes:

- **Classic:** regular EMQX TLS listener. `emqx_tokio` does not run, so
  `emqx_broker` does not look up native subscribers.
- **Native-only:** native (Rust) TLS listener. Forwarding to the broker is off,
  so messages do not enter the BEAM.

The same scenario also runs XMQ and FlashMQ on the same host, to put the EMQX
numbers in scale. See [XMQ and FlashMQ on our host](#xmq-and-flashmq-on-our-host).

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

## XMQ and FlashMQ on our host

Date: 2026-10-26. Same host, CPU split, client, scenario, and host tuning as
above. Each broker runs in Docker with host networking and `--cpuset-cpus
0-3,8-11`. CPU and RSS come from `/proc` of the broker process. There is no
BEAM split.

| Broker | Image | Configuration |
| --- | --- | --- |
| XMQ 1.0.0 | the client image above, entrypoint `xmq_server` | `raw/xmq-*/xmq_server.conf`: one TLS listener with 4 threads; 3 send, 4 receive, 2 delivery threads, as on the reference 8-thread host; anonymous access; persistence off |
| FlashMQ 1.27.3 | `native-bench/flashmq:1.27.3`, built by the runner from the FlashMQ apt repository | `raw/flashmq-*/flashmq.conf`: the reference configuration with `thread_count 8` and a TLS listener |

The reference used XMQ 0.9.20 and FlashMQ 1.27.2. These versions are not
available as packages anymore.

| Rate | Broker | Achieved | Mean latency | Median latency | Steady CPU | Steady RSS | Per connection |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50K | XMQ | 49,986/s | 77 us | 78 us | 285% | 1.70 GB | 31 KB |
| 50K | FlashMQ | 49,988/s | 69 us | 69 us | 250% | 1.93 GB | 32 KB |
| 50K | EMQX native-only | 49,983/s | 65 us | 64 us | 236% | 2.37 GB | 38 KB |
| 75K | XMQ | 74,970/s | 180 us | 189 us | 448% | 2.53 GB | 32 KB |
| 75K | FlashMQ | 74,975/s | 101 us | 102 us | 396% | 2.88 GB | 34 KB |
| 75K | EMQX native-only | 74,964/s | 114 us | 112 us | 358% | 3.44 GB | 40 KB |
| 100K | XMQ | 99,869/s | 355 ms | 459 ms | 615% | 3.39 GB | 33 KB |
| 100K | FlashMQ | 99,951/s | 312 us | 253 us | 541% | 3.84 GB | 35 KB |
| 100K | EMQX native-only | 99,931/s | 317 us | 276 us | 469% | 4.52 GB | 41 KB |

- At 100K, XMQ delivers the rate, but its latency is about 460 ms in every
  minute after the first. The latency does not grow over the run.
- EMQX native-only uses the least CPU at every rate. It uses 6 to 9 KB more
  memory per connection than XMQ and FlashMQ.
- Connect rate: XMQ about 1,200 to 1,450 a second, FlashMQ about 9,100 a
  second. `xmq_scn` asks for 10,000 a second.

### Rerun of the 100K runs

The first XMQ and FlashMQ runs at 100K overlapped CT runs of another
worktree on the same host:

- XMQ, 16:18 to 16:29: a compile and CT runs from 16:19 to 16:27.
- FlashMQ, 16:50 to 17:00: a compile and a CT run from 16:51 to 16:53. That
  run had one minute at 191 ms and a mean latency of 19.4 ms.

Both runs were repeated from 18:22 to 18:43 on an idle host. The table above
shows the reruns. During the reruns, `raw/host-activity-rerun.log` recorded the
load and the top processes every 10 s. No other process used more than 12% of
one CPU thread. The XMQ latency at 100K was the same in the rerun, so it does
not come from the CT runs.

No CT runs or compiles overlapped the other runs. The EMQX runs started at
23:46, and the last CT run of that night ended at 23:28.

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
| `raw/environment-scale.json`, `raw/runner-scale.log` | The same for the XMQ and FlashMQ runs |
| `raw/environment-rerun.json`, `raw/runner-rerun.log` | The same for the 100K reruns; their output replaces `raw/xmq-100000` and `raw/flashmq-100000` |
| `raw/host-activity-rerun.log` | Load average and top processes every 10 s during the reruns |
| `raw/xmq-<rate>/xmq_server.conf`, `raw/flashmq-<rate>/flashmq.conf` | Broker configuration |
| `raw/<mode>-<rate>/scenario.json` | Scenario passed to `xmq_scn` |
| `raw/<mode>-<rate>/client.log` | `xmq_scn` output: connect phase and publish intervals |
| `raw/<mode>-<rate>/summary.json` | Summary of the rate |
| `raw/<mode>-<rate>/resources.json` | `[time, cpu seconds, rss bytes]` every second |
| `raw/<mode>-<rate>/memory.csv` | `time ms, total, processes, binary, ets, system, process count, port count` every second |
| `raw/<mode>-<rate>/broker.log` | Broker console output |
