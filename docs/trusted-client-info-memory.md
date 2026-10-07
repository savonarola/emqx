# Trusted client information memory evaluation

This report compares the retained MQTT channel memory before and after trusted client information
integration.

## Revisions

| Stage | Commit |
| --- | --- |
| Pre-feature baseline | `37c9942c7f` |
| Core API only | `218a6a97fd` |
| Integrated implementation | `8e3f5672ac` |
| Unconditional-trust cleanup | `e00a7c2f0b` plus removal of exclusion metadata |
| Optimized authentication layout | `4317637eef` |
| Direct trust mask and separate authn namespace before rebase | `1e914c57df` |
| Three-scenario matrix after rebase | `1b175a0223` plus local shortcut and matrix changes |

The results below use the direct trust mask and separate authn namespace. Known authentication
outputs remain at the top level. Unknown outputs stay in top-level `authn`, which is absent when
empty. `trusted_attrs` contains the input mask directly. Disabled connections retain no trust mask.

The baseline comparison below was measured before the rebase onto updated `dev-70`. The
post-rebase matrix results appear in the final section.

The unconditional-trust cleanup retained 160 extra bytes per disabled or client-ID-only channel and
328 bytes per several-attributes channel. The optimized layout removes the disabled overhead and
saves 48 bytes in each enabled case. The direct mask saves another 48 bytes per enabled channel by
removing the singleton wrapper map. The original integrated implementation retained another 40 bytes
per channel for the empty exclusion map and its metadata entry.

The core API checkpoint had no runtime integration. Its retained terms matched the baseline byte for
byte: 896 bytes for `ClientInfo`, 3,496 bytes for the channel record, and 4,072 bytes for the complete
connection state. Its channel-process median was also unchanged at 6,104 bytes.

## Method

The benchmark is `apps/emqx/test/emqx_clientinfo_memory_SUITE.erl`. It does not run during normal CT
runs unless `EMQX_CLIENTINFO_MEMORY=1` is set. It runs a Common Test matrix with `disabled`,
`clientid_only`, and `several_attrs` scenarios.

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

Run all three scenarios with:

```sh
EMQX_CLIENTINFO_MEMORY=1 \
EMQX_CLIENTINFO_MEMORY_COUNT=1000 \
EMQX_CLIENTINFO_MEMORY_RUNS=3 \
TERM=dumb \
SUITES=emqx_clientinfo_memory_SUITE \
make apps/emqx-ct
```

The suite prints each scenario's measurements with `ct:print/2`. It does not write a separate
results file. Use `GROUPS=disabled`, `GROUPS=clientid_only`, or `GROUPS=several_attrs` to run one
scenario. Copy the suite unchanged into a detached baseline worktree to repeat the comparison.
The suite detects whether the worktree contains the core API or the integrated implementation.

## Retained term sizes

All values are bytes. Sharing-aware and flat sizes were equal for these extracted terms.

| Case | Term | Baseline | Direct mask | Increase | Percent |
| --- | --- | ---: | ---: | ---: | ---: |
| Disabled | `ClientInfo` | 896 | 896 | 0 | 0% |
| Disabled | Channel record | 3,496 | 3,496 | 0 | 0% |
| Disabled | Connection state | 4,072 | 4,072 | 0 | 0% |
| Enabled, client ID only | `ClientInfo` | 896 | 960 | +64 | +7.14% |
| Enabled, client ID only | Channel record | 3,496 | 3,560 | +64 | +1.83% |
| Enabled, client ID only | Connection state | 4,072 | 4,136 | +64 | +1.57% |
| Enabled, several attributes | `ClientInfo` | 896 | 1,128 | +232 | +25.89% |
| Enabled, several attributes | Channel record | 3,496 | 3,728 | +232 | +6.64% |
| Enabled, several attributes | Connection state | 4,072 | 4,304 | +232 | +5.70% |

The referenced binary payload did not increase. It remained 1,317 bytes for `ClientInfo` and 1,348
bytes for the channel and connection-state terms in every case. The process dictionary remained
1,128 heap bytes plus 88 referenced binary bytes. This shows that authorization did not retain a
trusted projection in its cache.

## Channel-process memory

All three direct-mask cases stayed in the same median and p95 heap-size step as the baseline.

| Case | Baseline median / p95 | Direct mask median / p95 | Measured increase |
| --- | ---: | ---: | ---: |
| Disabled | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |
| Enabled, client ID only | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |
| Enabled, several attributes | 6,104 / 6,104 | 6,104 / 6,104 | 0 bytes, 0% |

The direct-mask process-memory means ranged from 6,119.1 to 6,191.6 bytes in disabled mode, from
6,146.2 to 6,179.4 bytes for client-ID-only trust, and from 6,104.0 to 6,140.3 bytes for several
attributes. Referenced off-heap binary payload was 411 bytes per process in every run.

Earlier unconditional-trust cleanup measurements showed heap-allocation variation in the
several-attributes case. One execution used a 987-word heap for most channels, with a 9,120-byte
median. A repeat used a 610-word heap, with a 6,104-byte median. Both retained the same term sizes.

The retained increase can fit into existing process heap slack. Forced garbage collection does not
guarantee the same heap allocation step across executions.

## Fleet projections

The retained channel-record increase gives the stable payload projection. Actual process allocation
depends on the heap step. The earlier 3,016-byte step adds 287.63 MiB for 100,000 channels or
2,876.28 MiB for 1,000,000 channels when all channels enter that larger step.

| Case | Retained increase per channel | 100,000 channels | 1,000,000 channels |
| --- | ---: | ---: | ---: |
| Disabled | 0 bytes | 0 MiB | 0 MiB |
| Enabled, client ID only | 64 bytes | 6.10 MiB | 61.04 MiB |
| Enabled, several attributes | 232 bytes | 22.13 MiB | 221.25 MiB |

## Overhead source

The enabled client-ID-only case retains 64 extra bytes. The top-level `trusted_attrs` entry costs
16 bytes, and its client-ID mask costs 48 bytes. There is no singleton wrapper map. The nested mask
for `client_attrs.tns` and the two additional attributes adds 168 bytes. Attribute values are not
copied.

There is no exclusion mask. Known authentication outputs remain at the top level, so the benchmark
does not retain an `authn` map. Custom authentication outputs add a separate top-level map only when
present. This map remains separate from the input trust mask in every enforcement mode.

The channel retains one `ClientInfo` term. The exact `ClientInfo` increase therefore propagates to
the channel record and complete connection state without another retained copy. Attribute values are
not copied into the mask, and referenced binary payload remains unchanged. Trusted projections used
by authorization and other consumers are temporary and are not retained in the measured channel or
authorization cache.

Disabled authentication skips input trust composition and retains no trust mask. Disabled consumers
do not build projections or traverse masks. Enabling enforcement on these existing connections
treats discarded input trust as absent until reauthentication or reconnect. Known authentication
outputs remain statically trusted.

This benchmark measures retained memory, not execution time. It does not include session takeover,
which remains outside this work.

## Matrix after the dev-70 rebase

The matrix ran all three scenarios in one suite invocation on the branch rebased onto `dev-70`
at `5e835e904c`. Each scenario used three runs of 1,000 channels. All three cases passed.

All values below are bytes. Retained term sizes were identical across the three runs per scenario.
The overhead column compares each channel record with the disabled case in this matrix. It does
not compare the new runtime with the historical baseline above.

| Scenario | `ClientInfo` | Channel record | Connection state | Overhead | Process median / p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 728 | 2,936 | 3,520 | 0 | 6,104 / 6,104 |
| Client ID only | 792 | 3,000 | 3,584 | 64 | 6,104 / 6,104 |
| Several attributes | 960 | 3,168 | 3,752 | 232 | 6,104 / 6,104 |

The process dictionary remained 1,128 heap bytes in every scenario. Referenced off-heap binary
payload remained 411 bytes per process. The trust mask overhead remains 64 and 232 bytes for
the enabled scenarios.
