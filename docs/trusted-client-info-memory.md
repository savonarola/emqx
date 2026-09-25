# Trusted client information memory evaluation

This report compares the retained MQTT channel memory before and after trusted client information
integration.

## Revisions

| Stage | Commit |
| --- | --- |
| Pre-feature baseline | `37c9942c7f` |
| Core API only | `218a6a97fd` |
| Integrated implementation | `8e3f5672ac` |

The core API checkpoint had no runtime integration. Its retained terms matched the baseline byte for
byte: 896 bytes for `ClientInfo`, 3,496 bytes for the channel record, and 4,072 bytes for the complete
connection state. Its channel-process median was also unchanged at 6,104 bytes.

## Method

The benchmark is `apps/emqx/test/emqx_clientinfo_memory_SUITE.erl`. It does not run during normal CT
runs unless `EMQX_CLIENTINFO_MEMORY_SCENARIO` is set.

Each measurement used:

- Erlang/OTP 28 with an 8-byte word.
- The `emqx-enterprise-test` build profile.
- Three runs of 1,000 concurrent MQTT 5 TCP channels.
- A 13-byte client ID and a 31-byte untrusted username.
- `client_attrs.tns` with a 16-byte value.
- Two additional trusted-candidate attributes with 96-byte values.
- One additional untrusted attribute with a 96-byte value.
- The same input values and `is_superuser = false` authentication result on each revision. The
  integrated revision also received the scenario's trust mask.
- One subscription authorization and one acknowledged QoS 1 publish authorization per channel.
- Two forced channel-process garbage collections separated by 500 milliseconds.

The publish operation populated the authorization cache. The multi-tenancy switch was set, but the
multi-tenancy hook was not started because hardened namespace handling removes an untrusted `tns`
attribute. This kept the retained input identical in every comparison.

The suite measured the following values:

- `erts_debug:size/1` for sharing-aware heap words.
- `erts_debug:flat_size/1` for heap words without sharing.
- Referenced binary payload sizes for `ClientInfo`, the channel record, and the complete connection
  state.
- `process_info/2` memory, heap, message queue, and referenced binaries after garbage collection.
- The process dictionary term that contains the authorization cache.

Run one case with:

```sh
EMQX_CLIENTINFO_MEMORY_SCENARIO=disabled \
EMQX_CLIENTINFO_MEMORY_COUNT=1000 \
EMQX_CLIENTINFO_MEMORY_RUNS=3 \
EMQX_CLIENTINFO_MEMORY_OUTPUT=/tmp/clientinfo-memory.eterm \
TERM=dumb \
SUITES=emqx_clientinfo_memory_SUITE \
make apps/emqx-ct
```

Use `clientid_only` or `several_attrs` for the other cases. Copy the suite unchanged into a detached
baseline worktree to repeat the comparison. The suite detects whether the worktree contains the core
API or the integrated implementation.

## Retained term sizes

All values are bytes. Sharing-aware and flat sizes were equal for these extracted terms.

| Case | Term | Baseline | Integrated | Increase | Percent |
| --- | --- | ---: | ---: | ---: | ---: |
| Disabled | `ClientInfo` | 896 | 1,096 | +200 | +22.32% |
| Disabled | Channel record | 3,496 | 3,696 | +200 | +5.72% |
| Disabled | Connection state | 4,072 | 4,272 | +200 | +4.91% |
| Enabled, client ID only | `ClientInfo` | 896 | 1,096 | +200 | +22.32% |
| Enabled, client ID only | Channel record | 3,496 | 3,696 | +200 | +5.72% |
| Enabled, client ID only | Connection state | 4,072 | 4,272 | +200 | +4.91% |
| Enabled, several attributes | `ClientInfo` | 896 | 1,264 | +368 | +41.07% |
| Enabled, several attributes | Channel record | 3,496 | 3,864 | +368 | +10.53% |
| Enabled, several attributes | Connection state | 4,072 | 4,440 | +368 | +9.04% |

The referenced binary payload did not increase. It remained 1,317 bytes for `ClientInfo` and 1,348
bytes for the channel and connection-state terms in every case. The process dictionary remained
1,128 heap bytes plus 88 referenced binary bytes. This shows that authorization did not retain a
trusted projection in its cache.

## Channel-process memory

The process allocator stayed in the same heap-size step in every case.

| Case | Baseline median / p95 | Integrated median / p95 | Measured increase |
| --- | ---: | ---: | ---: |
| Disabled | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |
| Enabled, client ID only | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |
| Enabled, several attributes | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |

The per-run process-memory means ranged from 6,104.0 to 6,143.3 bytes across all baseline and
integrated runs. Fewer than 5% of channels entered a larger heap allocation step, so the median and
p95 remained stable. Referenced off-heap binary payload was 411 bytes per process in every run.

The retained term increase fits into existing process heap slack for this workload. It can still
cause an earlier heap growth step with larger sessions or other channel state.

## Fleet projections

The retained channel-record increase gives the stable payload projection. The measured process
allocation projection is zero while channels remain in the same heap step.

| Case | Retained increase per channel | 100,000 channels | 1,000,000 channels |
| --- | ---: | ---: | ---: |
| Disabled | 200 bytes | 19.07 MiB | 190.73 MiB |
| Enabled, client ID only | 200 bytes | 19.07 MiB | 190.73 MiB |
| Enabled, several attributes | 368 bytes | 35.10 MiB | 350.95 MiB |

## Overhead source

The common 200-byte increase comes from the retained `trusted_attrs` metadata. It contains the
sanitized authentication result, the client-information trust mask, and the exclusion mask. The
nested mask for `client_attrs.tns` and the two additional attributes adds 168 bytes.

The channel retains one `ClientInfo` term. The exact `ClientInfo` increase therefore propagates to
the channel record and complete connection state without another retained copy. Attribute values are
not copied into the mask, and referenced binary payload remains unchanged. Trusted projections used
by authorization and other consumers are temporary and are not retained in the measured channel or
authorization cache.

Disabled enforcement does not build a projection or traverse the mask in consumers. It still retains
the 200-byte provenance metadata created at authentication. This metadata lets a later authorization
configuration change enforce trust on an existing connection. Removing it would require clients to
reauthenticate or reconnect before enforcement could be enabled safely.

This benchmark measures retained memory, not execution time. It does not include session takeover,
which remains outside this work.
