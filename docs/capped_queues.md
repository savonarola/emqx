# Capped message queues: implementation plan

## Scope and baseline

Implement a per-queue full policy on top of `20261012-r70-queue-cleanup`.
The investigated baseline is commit `e7fe958380`.
Integrate that branch before implementing this plan.

The baseline implements the purge and cutoff work described in
[#19209](https://github.com/emqx/emqx/issues/19209).
This plan adds the reject policy described in
[#18142](https://github.com/emqx/emqx/issues/18142).

Support limited regular queues and limited last-value queues.
Keep the existing `drop_oldest` policy as the default.
Add `reject` as the other policy.
Do not implement `block`.
Keep limits per shard.

Storage timestamps and internal cleanup boundaries use microseconds.

The objective is to release quota as consumption advances without waiting for
periodic garbage collection.
Publishers check the quota index and reject writes that do not fit.
Publishers must not initiate cleanup or wait for cleanup to complete.

### Storage layout of limited queues

Every limited queue stores its messages and quota index in
`?MQ_MESSAGE_LASTVALUE_DB`. This includes limited regular queues.
Writes to that database always use generation 1
(`emqx_mq_message_db:insert_generation/1`).
A queue with a quota index therefore never changes generation and never
reaches end-of-stream.

Identify all cleanup state and work by `{QueueId, Shard}`.
Do not model generations, generation changes, or completed-slab events.

`?MQ_MESSAGE_DB_LTS_SETTINGS` configures LTS so that each shard holds a single
stream for a queue topic. Consumption cleanup relies on this property.
A consumed boundary from the shard's single stream buffer covers every queue
record that a ranged deletion in that shard can remove.

## Agreed decisions

1. Admission accounting is approximate. Stale reads may reject a write that
   could fit. Concurrent writes and buffered index updates may allow overshoot.
2. Keep buffered quota updates. Do not add transactional quota reservations or
   cross-shard admission coordination.
3. Release quota by retiring whole index buckets at existing bucket boundaries.
4. Retain the newest index bucket. Do not force rotation, split it during
   cleanup, or reset it when consumption catches up.
5. Accept rejection when a new message cannot fit alongside the consumed but
   still charged newest bucket.
6. Report consumption only when a whole consumer buffer has been acknowledged
   and is retired. Do not report partial progress on each acknowledgment.
7. Inject an optional progress callback into stream-buffer consumption. Keep
   quota-index and cleanup decisions outside the buffer implementation.
8. Place each consumer and its optional cleaner under a per-queue supervisor.
   Create one cleaner per active reject-queue consumer, covering all its shards.
9. Commit index truncation before deleting data. Release quota after the index
   commit, without waiting for data deletion or RocksDB compaction.
10. Reject queues must not discard index buckets or stored messages merely
    because usage exceeds a capacity limit. Retention and explicit purge still
    apply.
11. Do not persist a cleanup cutoff. Each cleanup deletes the range `[0, C)`.
    A later cleanup or retention removes records left by a failed deletion.
12. The full policy cannot change after queue creation.
13. The reject policy requires at least one finite limit.
14. Allow reject queues only after every cluster node runs a release that
    supports them.
15. Admission does not credit the previous value of a last-value queue.
16. Publication to ordinary topics gives no feedback about rejection by a
    matched queue. Only direct `$queue/` publication reports `queue_full`.
17. Reject last-value queues hold only unconsumed values. Cleanup deletes
    consumed values. Queues are consumed once and are not streams.

## Semantics and invariants

### Quota occupancy

Occupancy is the count and serialized message bytes charged to the quota index
for one queue in one shard.
It is an estimate of retained backlog.
It is not an exact count of unacknowledged messages or a physical disk-size cap.

A publish adds quota through the existing quota buffer after its data write.
A last-value replacement subtracts the previous value and adds the new value
through the existing quota buffer path.
Cleanup releases quota in whole buckets.
Buffered updates, cache lag, retained buckets, and concurrent publication affect
the observed occupancy.
Do not promise a fixed maximum overshoot.

A stale occupancy estimate is acceptable.
A stale estimate must not authorize deletion of unacknowledged data.
Consumption-based deletion must always stay within a confirmed consumed prefix.

Reject queues must not drop accepted messages because of capacity pressure.
Configured expiration, explicit purge, and last-value replacement retain their
existing meanings.

### Per-shard limits

Limits apply to each shard separately.
Regular queues select the shard by publisher client ID.
Last-value queues select the shard by message key.
A queue can reject one publisher while it accepts messages from other
publishers. The total queue capacity is the limit multiplied by the number of
shards. Document this behavior for users.

### Consumer buffers and index buckets

Use these terms consistently:

- **Consumer buffer:** the lower or upper delivery buffer in
  `emqx_mq_consumer_stream_buffer`. It tracks DS iterators and unacknowledged
  message IDs.
- **Index bucket:** a timestamp range with count and byte totals in the quota
  index. The next newer bucket supplies its exclusive upper boundary.

Consumer-buffer retirement and index-bucket retirement are separate events.
A consumer buffer can retire without crossing an index boundary.
One consumer-buffer retirement can make several index buckets eligible.

### Consumed boundary

A consumed boundary is an exclusive DS timestamp for a particular queue and
shard. Every message in the covered prefix has been acknowledged or explicitly
skipped by existing consumption rules.
The boundary must come from a completely acknowledged consumer buffer.

Consumer message IDs are DS timestamps.
For a retired buffer that contains records, the boundary is the last record's
message ID plus one.

Do not derive notifications from the earliest remaining unacknowledged message
inside a partially acknowledged buffer.
Do not use the stream-wide `last_message_id` as the retired lower buffer's
boundary. The upper buffer may already contain newer unacknowledged messages.

Distinguish consumer acknowledgments from DS subscription acknowledgments.
DS subscription acknowledgments control fetching and buffering.
They do not prove that subscribers have consumed messages.

### Retained newest index bucket

The index stores buckets newest first, so the newest bucket is its head.
That bucket has no newer boundary.
Consumption cleanup must retain it even when all its currently known messages
have been acknowledged.

An insertion that makes the head bucket reach a threshold also closes it.
The index then prepends a new empty head bucket.
The charge left in the head bucket therefore stays below one threshold, which
is ten percent of the limit by default.

For example, a queue with a byte limit of 1,000 has a threshold of 100 bytes.
Its head bucket may hold 90 consumed bytes. A new 950-byte message is then
rejected. This is accepted behavior.
Do not introduce a tail-close operation to admit that message.

Normal index updates create newer bucket boundaries.
Cleanup can then retire the former head after a consumer-buffer retirement has
confirmed sufficient progress.
Creation of a boundary alone does not prove consumption.
Do not require an idle, fully consumed queue to reach zero index occupancy.

### First bucket

The baseline creates the first bucket of an empty index without the threshold
check (`maybe_advance([], ...)` in `emqx_mq_quota_index_common`).
With a count limit of one, the first message fills the head bucket.
Admission then rejects every later message, so no insertion creates a newer
boundary. Consumption cannot release the bucket, and the queue rejects
publication until retention expires.

Apply the threshold check when the first bucket is created.
The first insertion must close its bucket when it reaches a threshold, in the
same way as later insertions.
This change affects the shared index code. Run the stream index regression
tests.

## Existing implementation to reuse

Paths in this section refer to the baseline branch.

- `apps/emqx_mq/src/emqx_mq_message_db.erl` performs synchronous writes for
  limited queues. It submits quota updates after the message transaction.
- `apps/emqx_mq/src/emqx_mq_quota/emqx_mq_quota_buffer.erl` batches updates and
  flushes them in separate index transactions.
- `apps/emqx_mq/src/emqx_mq_quota/emqx_mq_quota_index_common.erl` stores count and
  bytes in shared timestamp buckets. It currently trims older buckets when
  accumulated usage reaches a limit.
- `emqx_mq_message_db:purge_indexed_slab/5` commits index truncation before
  deleting data in a separate transaction.
- `emqx_mq_consumer_stream_buffer` already detects fully acknowledged buffers
  when rotating the upper buffer or compacting the lower buffer.
- `emqx_mq_consumer` persists consumption progress periodically. The default
  interval is ten seconds, so runtime progress can be newer than its checkpoint.
- Queue configuration updates already restart consumers on the baseline branch.
- Direct publication already carries persistence results to PUBACK and PUBREC
  through `mq_publish_result`.

Reuse the shard-scoped storage operations from purge.
Do not call the registry purge API for consumption cleanup.
That API changes a queue-wide delivery cutoff and restarts its consumer.

## Architecture

### Supervision

Use this supervision structure, alongside the existing service children:

```text
emqx_mq_sup
├── consumer_sup
│   └── queue_sup [one per active queue]
│       ├── consumer
│       └── cleaner [reject queues only]
└── gc_sup
```

Configure the per-queue supervisor as follows:

- Use the `rest_for_one` strategy. Start the consumer first and the cleaner
  second.
- Set `auto_shutdown => any_significant`.
- Make the consumer a `transient`, significant child.
- Make the cleaner a `permanent` child.
- Keep the per-queue supervisor temporary under `consumer_sup` to preserve
  on-demand consumer startup.

The consumer keeps its existing exit paths. When the consumer exits normally,
for example after its idle shutdown timer or `stop/1`, the per-queue supervisor
stops and terminates the cleaner.
When the consumer crashes, the supervisor restarts the consumer and then the
cleaner.
A cleaner failure restarts only the cleaner.

The child list depends on the full policy. Only reject queues get a cleaner.
Drop-oldest queues and unlimited queues start only the consumer.

The consumer registers its global name in `init/1`.
When a concurrent startup loses the registration, the consumer `init/1` fails.
The per-queue supervisor then fails to start, and no cleaner starts.
Return the registered consumer reference to the caller, as the baseline does.
Do not return the supervisor PID as the consumer reference.

The cleaner finds the consumer with `emqx_mq_consumer:find/1` in its `init/1`
and sends it an attach message.
The consumer replies with its latest reported boundaries.
A restarted cleaner uses the same path.

When a queue update restarts consumption, stop the old per-queue supervisor.
Queue deletion and application shutdown must also terminate the paired children.
Do not wait for data deletion during consumer shutdown.

Proposed modules are `emqx_mq_queue_sup` for the per-queue supervisor,
`emqx_mq_consumer_cleanup` for callback state, and `emqx_mq_cleanup_worker` for
storage work.

### Responsibilities

| Component | Responsibility |
| --- | --- |
| Stream buffer | Report completely acknowledged buffer retirement. |
| Cleanup callback handler | Keep the latest reported boundary and compare it with cached index boundaries. |
| Cleaner | Read the index, truncate eligible buckets, delete data, and retry failures. |
| Publisher | Check estimated occupancy and return the admission result. |
| Periodic GC | Apply retention to data and, for reject queues, to index buckets. |

## Consumption callback design

### Callback contract

Introduce a narrow optional callback with a contract equivalent to:

```erlang
on_consumed(Shard, BoundaryUs, CallbackState0) -> CallbackState.
```

`BoundaryUs` is the exclusive DS timestamp boundary of a retired consumer
buffer.
Do not decode opaque iterator representations in MQ code.

Inject the callback and runtime state when constructing or restoring a stream
buffer. Queues using `drop_oldest` use no cleanup callback.
Keep callback state out of the serialized consumer progress.

Keep the generic injection point small.
Do not introduce a general event framework or unrelated callbacks.

### Notification points

Route these transitions through one buffer-retirement helper:

1. `try_rotate_upper_buffer/1`: the lower buffer has no unacknowledged messages
   and the upper buffer replaces it.
2. `try_compact_lower_buffer/1`: the lower buffer has no unacknowledged messages
   and its starting iterator advances to its ending iterator.

Report the retired lower buffer's ending boundary before replacing its metadata.
Suppress notifications when an already-empty buffer makes no progress.
If rotation exposes another fully acknowledged buffer, allow its retirement to
report the next boundary in order.

`try_compact_lower_buffer/1` runs on every acknowledgment and DS reply.
With a fast consumer, the callback runs on almost every acknowledgment.
The callback must stay cheap: compare against the cached boundary and return.

Track the last record's message ID separately for each consumer buffer.
Handle empty DS batches conservatively. An empty batch does not advance the
boundary.
Preserve or reconstruct the necessary boundary during restoration.
Do not emit new progress merely because restoration began or a subscription
failed.
An unrecoverable subscription error is not evidence of acknowledgment.

### Callback state and index caching

Keep per-shard callback state for:

- The latest completely acknowledged boundary.
- The attached cleaner.
- The cached next eligible index-bucket boundary.
- Any outstanding refresh or cleanup notification.

The callback records monotonically advancing progress.
When progress crosses the cached boundary, it notifies the cleaner.
When the boundary is unknown, it requests an asynchronous index refresh.
Coalesce notifications while work is pending.

The callback runs in the consumer process.
It must not perform synchronous DS reads, wait for cleanup, or retry storage
transactions. Keep index reads and storage retries in the cleaner.

After a refresh, compare the returned boundary against the latest saved
consumption boundary immediately.
Do not wait for another acknowledgment or buffer retirement.
This is necessary when consumption preceded the quota-buffer flush.
Retain pending progress and refresh it on a bounded retry schedule when the
index has not caught up.

A cached boundary is a scheduling hint.
The cleaner must select the actual deletion boundary from a fresh transactional
index read and the supplied consumed boundary.
Invalidate or replace cache state after cleanup, purge, and cleaner
replacement.

## Quota-index changes

### Reject-mode retention

Pass the full policy into quota-index options.
The policy cannot change after queue creation, so options captured in buffered
updates always carry the queue's current policy.
Keep the existing drop-oldest behavior for existing callers and queues.

For reject queues:

- Keep ordinary bucket creation thresholds and bucket merging.
- Disable capacity-driven trimming of older buckets.
- Retain all charged buckets until consumption, retention, or purge removes them.
- Do not use `deadline/1` to authorize capacity-driven data deletion.

Apply these rules consistently to any legacy representation that is still
maintained. The common-index migration layer must not reintroduce capacity
trimming for reject queues.
Keep stream users of the shared quota-index code on their existing behavior,
except for the first-bucket fix.

Do not change the index storage format.

### New index operations

Add dedicated helpers for:

- Reading estimated count and byte occupancy.
- Finding the next boundary that can retire at least one older bucket.
- Truncating eligible whole buckets at a supplied boundary.

Do not use `inspect/1` for admission or cleanup decisions.

For an exclusive consumed boundary `P`, cleanup may use an existing bucket
boundary `C` when `C =< P`.
Remove buckets before `C` and retain the bucket beginning at `C`.
Never remove the newest bucket through consumption cleanup.

Keep this boundary rule separate from bounded purge's existing strictly-earlier
cutoff rule. Do not silently change the public purge contract.

Index read-modify-write operations must remain transactional.
Approximate publisher admission does not justify overwriting concurrent quota
updates during truncation.
Retry index conflicts in the cleaner.

### Timestamp domains

The current quota buffer assigns buckets using the embedded message timestamp.
DS stores messages with a monotonic storage timestamp.
Consumer message IDs and ranged deletion use the DS timestamp.

Preserve approximate accounting for this implementation.
Do not assume that an index timestamp identifies exactly the same message set
as a DS consumption position.
Delayed writes and clock differences can make occupancy less accurate than
quota-buffer lag alone.

Data deletion stays safe because it compares DS timestamps with the DS consumed
boundary: every record below `C` is below `P`.
An index bucket is not proof that its corresponding messages were acknowledged.
Test delayed writes and timestamp skew for accounting effects.
Do not add exact storage-time accounting as an implicit prerequisite for the
reject policy.

## Cleanup protocol

### Per-shard operation

Identify work by immutable queue ID and shard.
Do not identify deletion work only by a reusable queue name.

For each reported consumed boundary:

1. Coalesce it with the highest pending boundary for that shard.
2. Read the current index in a transaction.
3. Select an existing eligible bucket boundary `C` no later than the consumed
   boundary. If none exists, retain progress for a later refresh.
4. Truncate index buckets older than `C`.
5. Commit the index transaction.
6. Delete queue records in `[0, C)` in a separate transaction.
7. Refresh the next index boundary.

Truncation only removes buckets, so it never lowers the oldest retained
boundary. Delayed requests select a boundary from the current index and cannot
restore removed buckets.

Serialize work within a cleaner and coalesce repeated requests.
Process shard work in bounded steps so one busy shard does not indefinitely
delay the others.
Concurrent periodic GC or purge may still touch the same index.
Use transactions and idempotent ranged deletion to handle those races.

Publication can use the reduced index after step 5.
It must not wait for step 6.
RocksDB compaction may reclaim disk space later.

### Failed deletion

Do not persist the cleanup boundary.
When step 6 fails after the index commit, the remaining records are consumed and
no longer charged.
Retry the deletion in the cleaner while it runs.
Otherwise, the next successful cleanup removes them, because every deletion
range starts at zero. Retention removes them when no consumer is active.

A consumer restored from an older checkpoint may redeliver such records.
Delivery is already at-least-once, and the checkpoint lag already causes the
same redelivery. Do not add a cutoff filter to consumer restoration.

### Restoration after cleanup

`emqx_mq_consumer_stream_buffer` leaves the restoring state when it sees a
record with an ID at or above the checkpoint's `last_message_id`, or at
end-of-stream. Reject queues never reach end-of-stream.
When cleanup has deleted the checkpoint's last record and no newer record
exists, the buffer stays in the restoring state until a new message arrives.
No messages wait for delivery in that state, and the buffer reports no
progress. Document this behavior and cover it with a test.

## Publish admission

### Regular and last-value queues

In `emqx_mq_message_db:insert/2`, check the policy before the message
transaction:

1. Determine the destination shard and serialized message size.
2. Read its estimated quota-index occupancy with a dirty read.
3. Add the new message's count and bytes and check the result against the
   configured finite limits.
4. Return `{error, queue_full}` when the write does not fit.
5. Otherwise use the existing message transaction and buffered quota update.

Apply the same check to last-value queues.
Do not credit the previous value of the same key.
Admission may reject a replacement that would fit after the previous value is
removed. A rejected replacement leaves the previous value intact.

Do not read the quota index inside the message transaction.
Do not force a quota-buffer flush or contact the consumer on publication.
Do not turn read/check/write into an atomic quota reservation.
Add a bounded-lifetime admission cache only if measurements justify it.

Handle an absent index according to the existing new-queue semantics.
Do not confuse an unavailable index read with a successfully read empty index.
Return a storage error when the occupancy read fails.

### Publisher results

Map `queue_full` to MQTT 5 Quota exceeded (`0x97`) through the existing direct
publish PUBACK and PUBREC hooks.
Keep storage failures distinguishable from capacity rejection.
Count capacity rejections separately from storage errors.

QoS 0 has no acknowledgment channel. Reject its queue insertion and record the
result in metrics.
MQTT 3.x acknowledgments cannot carry the MQTT 5 rejection reason.
Document the supported feedback behavior without promising an error reason to
those clients.

### Ordinary topic publication

`emqx_mq:on_message_publish/1` inserts a message into every matching queue,
logs errors, and continues regular routing.
Keep this behavior for reject queues:

- A full reject queue skips the message.
- Count a `queue_full` rejection metric and log a throttled warning.
- Continue insertion into other matched queues and regular routing.
- Do not change the PUBACK or PUBREC reason code.

Only direct `$queue/` publication reports `0x97`.
Document that publishers to ordinary topics receive no rejection feedback.

## Retention, purge, and configuration

### Periodic GC

For `drop_oldest`, keep the existing shared per-shard transaction and its
capacity-driven deletion.

For `reject`, omit the quota-derived capacity deadline.
Process each reject queue separately in two transactions:

1. Truncate index buckets older than the retention deadline. Retain the newest
   bucket.
2. Delete records below the retention deadline.

Retry or skip index conflicts until the next GC run.
A conflict on one reject queue must not affect expiration of other queues.
Without index truncation, an expired queue without subscribers would remain
charged indefinitely.

Explicit expiration and purge may remove data without consumer acknowledgment.
Keep those operations distinct from consumption-based cleanup.

### Purge

Reuse the baseline full and bounded purge operations.
Invalidate cleaner caches after purge.
Preserve their documented cutoff and partial-failure behavior.

Full purge remains an explicit operation that may reset the head bucket.
It is not an automatic response to an empty consumer.
Bounded purge retains its bucket-boundary accounting approximation.

Test purge racing index flushes, publication, consumption cleanup, and retry of
an earlier failed deletion.
Neither path may restore removed buckets through stale whole-index writes.

### Configuration and compatibility

Use a per-queue field `full_policy` with `drop_oldest` as its default.
Carry it through:

- Queue schemas, API representations, and automatic queue creation settings.
- Queue types, ASN.1 queue state, and encode/decode defaults for older records.
- Registry records and lightweight publishing handles.
- Quota-buffer and quota-index options.
- Consumer startup and periodic GC selection.

Validate the policy on create and update:

- Reject updates that change `full_policy`, in the same way as updates to
  `is_lastvalue`.
- Require at least one finite limit when the policy is `reject`.
- Reject updates that remove all finite limits from a reject queue. This
  applies to last-value queues, which can otherwise change from limited to
  unlimited.
- Apply the same rules to automatic queue creation settings.

Old nodes do not know `full_policy` and treat every queue as drop-oldest.
They trim index buckets for capacity during quota flushes, delete data below
the capacity deadline during GC, and publish without admission checks.
Refuse to create a reject queue until every cluster node runs a release that
supports the policy. Use the existing cluster version or BPAPI check.

## Implementation sequence

### 1. Integrate the baseline

- Integrate `20261012-r70-queue-cleanup` into the implementation branch.
- Confirm the public policy name.

### 2. Add policy plumbing and index operations

- Update schema, types, registry handles, persistence, and configuration docs.
- Add policy validation and the cluster version check.
- Apply the threshold check to first-bucket creation.
- Add reject-mode index retention without changing existing callers' defaults.
- Add occupancy, next-boundary, and truncation helpers.
- Add focused index tests, including legacy/common representations and stream
  index regressions.

### 3. Add generic buffer-retirement callbacks

- Track the last record's message ID for each buffer.
- Add the optional callback to new and restored buffers.
- Share notification logic between lower-buffer compaction and upper-buffer
  rotation.
- Test the callback independently with a recording handler.
- Keep quota logic out of the buffer module.

### 4. Add supervision and the cleanup handler

- Add the per-queue supervisor and the reject-only cleaner.
- Preserve consumer lookup, connection, and stop behavior.
- Implement attachment, boundary caching, refresh, and notification coalescing.
- Restore callback runtime state without serializing process references.

### 5. Implement cleanup

- Extract or reuse shard-scoped index truncation and ranged deletion primitives.
- Add retries for index conflicts and failed deletions.
- Test failure between index commit and data deletion.

### 6. Add admission and publisher feedback

- Measure the publication latency of the dirty index read before continuing.
- Add dirty-read admission for reject queues.
- Keep buffered accounting and existing data transactions.
- Add `queue_full` result handling and rejection metrics.
- Implement the ordinary-publish behavior.

### 7. Integrate GC and purge

- Disable capacity deletion for reject queues.
- Add per-queue retention for reject queues, including index truncation.
- Invalidate cleaner caches after purge.
- Verify queue deletion, recreation, application shutdown, and cluster behavior.

### 8. Validate and document

- Run focused index, consumer, purge, GC, publication, and cluster tests.
- Measure publication overhead and index conflict rates under concurrent load.
- Add user documentation for approximate occupancy, per-shard limits, the
  retained newest bucket, and last-value cleanup.
- Add the release note after the implementation scope is final.

## Test plan

### Index and admission

- Count-only, byte-only, and combined limits.
- A count limit of one accepts a new message after the previous one is consumed.
- The first insertion closes its bucket when it reaches a threshold.
- Reject-mode updates retain older buckets after overshoot.
- Drop-oldest and stream index behavior remain covered by their existing tests.
- Stale low reads allow overshoot without triggering capacity deletion.
- Stale high reads may reject until the estimate refreshes.
- Rejection leaves existing data and last-value values intact.
- Last-value admission does not credit the previous value.
- Missing indices and failed reads have distinct outcomes.

### Consumer callbacks

- Partial acknowledgments do not notify.
- A completely acknowledged lower buffer notifies when compacted or swapped out.
- Out-of-order acknowledgments do not advance through an unacknowledged lower
  buffer.
- Upper-buffer progress does not leak into the retired lower-buffer boundary.
- An already-empty buffer does not emit duplicate progress.
- Empty DS batches do not advance the boundary.
- Skipped messages and restoration follow the same retirement rules.
- Subscription failure does not report false acknowledgment progress.
- Callback state is recreated after restart and is not persisted as runtime
  references.

### Cleanup and retained head

- Crossing a cached index boundary schedules cleanup.
- One notification can retire several eligible index buckets.
- An exact exclusive boundary can retire the preceding bucket.
- No newer boundary means no consumption-based head removal.
- A large message can be rejected because a consumed head remains charged.
- A naturally created newer boundary allows later cleanup of its predecessor.
- Delayed quota flush followed by refresh triggers cleanup without another
  buffer retirement.
- Cached stale boundaries cannot authorize deletion beyond confirmed DS progress.
- Concurrent publication and index truncation preserve transactional updates.
- Delayed message timestamps and clock skew do not cause deletion of
  unacknowledged records.
- Records left by a failed deletion are removed by the next cleanup.

### Recovery and lifecycle

- Cleaner failure does not restart or block the consumer.
- A replacement cleaner receives the latest reported progress.
- Consumer normal exit terminates its per-queue supervisor and cleaner.
- Consumer crash restarts the consumer and then the cleaner.
- Concurrent consumer startup does not leave orphan supervisors or cleaners.
- A consumer restored after cleanup deleted its checkpoint's last record stays
  restoring until a new message arrives, then delivers it.
- Queue recreation does not reuse deletion work from the previous queue ID.

### Configuration

- Changing `full_policy` on update is rejected.
- A reject queue without a finite limit is rejected on create and update.
- Creating a reject queue fails while a cluster node runs an older release.

### Integration

- Periodic GC never drops unconsumed reject-queue data due to capacity.
- Expiration releases accounting without an active consumer.
- An index conflict on one reject queue does not block expiration of others.
- Full and bounded purge coordinate with cleanup and concurrent writes.
- MQTT 5 QoS 1 and QoS 2 report `0x97` for direct queue rejection.
- QoS 0 rejection records metrics without an acknowledgment.
- Ordinary publication to a full reject queue records the metric, keeps the
  PUBACK reason code, and still reaches other queues and subscribers.

Use the existing suites under `apps/emqx_mq/test/` where their scope fits.
Add focused callback and cleaner suites when necessary.
Use trace events to control index-commit, deletion, and refresh races.
Do not assert an exact cap or zero occupancy after draining the newest bucket.

For compilation, run `make test-compile`.
Run focused suites with `TERM=dumb SUITES=<suite> make apps/emqx_mq-ct`.
Run the relevant MQ integration suites after the focused checks pass.
Include shared-index stream regression tests when changing common quota code.

## Remaining decisions

1. **Public policy name:** confirm `full_policy` and its values `drop_oldest`
   and `reject`.

These decisions must preserve approximate admission, buffer-retirement-only
notifications, asynchronous cleanup, and the retained newest bucket.
