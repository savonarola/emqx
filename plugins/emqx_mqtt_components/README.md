# MQTT components PoC

This plugin demonstrates state and service dependencies and automatic cleanup from
the MQTT Component Model proposal. One coordinator holds all metadata in
memory on one broker node. Hooks call the coordinator synchronously.
Components use MQTT 5 and clean sessions.

## Protocol

Declare resources in one SUBSCRIBE packet:

```text
crouter:  $provide/service/reg-route
chandler: $consume/service/reg-route
```

Include ordinary application and response subscriptions in that packet as
needed. The plugin adds a lifecycle subscription at
`$component/<clientid>/events`. Lifecycle payloads are JSON objects with an
`event` field. Declarations remain fixed until disconnect.

1. When all providers are active, the plugin sends `{"event":"initialize"}`.
2. The component performs initialization requests. Publish to the declared
   `$service/...` topic with MQTT 5 Response Topic and optional Correlation Data.
3. The plugin assigns an effect ID and forwards the payload to
   `$service/.../apply/<effect-id>`. It replaces Response Topic with an internal
   response topic. The provider replies there and echoes Correlation Data.
   The plugin forwards the response to the caller's original Response Topic.
   Its MQTT User Property `component-effect-id` contains the assigned ID.
4. After initialization responses arrive, publish to `$component/ready`.
   The plugin sends the provider subscription commands and checks the installed
   subscriptions before returning `{"event":"activated"}`. Without a Response
   Topic, control responses go to the component's lifecycle topic.
5. When a provider disconnects or is disabled, the plugin sends
   `{"event":"deactivated"}` to affected components. They stop new application
   work and keep serving accepted applies and retractions.
6. After dependents finish cleanup and outstanding applies complete, the plugin
   sends `{"event":"cleanup_requested"}`. The component performs local teardown
   and publishes a non-retained message to `$component/cleanup_complete`.
7. The plugin retracts the component's managed effects, releases its bindings,
   and sends `{"event":"stopped"}`. The component may then disconnect or wait
   for another `initialize`. Initialization abort returns `aborted` instead.

During activation, each service request adds a cleanup action. Cleanup sends
an empty payload to `$service/.../retract/<effect-id>` in reverse request
order. It waits for outstanding apply responses from reachable providers first.
The provider uses the effect ID to remove the corresponding operation.
Each retract includes an internal MQTT Response Topic. The provider responds
there with `{"status":"retracted"}` after removing the effect. A provider that
cannot confirm removal responds with `{"status":"failed"}` or
`{"status":"unknown"}`. The plugin waits for this response before starting
the next inverse. Invalid responses do not complete retraction.
The plugin also retracts requests when their caller disconnects, including
requests whose responses have not arrived. Reconnecting creates a new
component connection. Dependents register again during their next activation.

Stopping withdraws the affected components before the coordinator accepts more
work. Cleanup runs from dependents toward providers. For `A -> B -> C` and
`D -> C`, where each arrow points to a provider, A completes local teardown and
retracts its managed effects before B receives `cleanup_requested`. D can finish
independently. Each component retracts its effects in reverse acceptance order.
B receives `cleanup_requested` only after A finishes. Before that notification,
B must preserve resources needed to complete accepted applies and retractions.
The plugin rejects an early `cleanup_complete`. Local teardown completes before
B's managed effects are retracted, so those effects remain available during
local cleanup. A disconnected component has unknown local cleanup and requires
no acknowledgement. Providers keep their subscriptions until cleanup ends.

When C has disconnected, its retractions have an unknown outcome. Its resource
keys stay reserved until dependent cleanup and C's own cleanup finish.
An earlier replacement declaration receives `provider_conflict`. The client
must retry after cleanup. Cleanup keeps the old activation's bindings and never
sends old retractions to the replacement.

`emqx_mqtt_components:cleanup_results/0` returns completed cleanup reports,
newest first. Each report contains the client ID, activation ID, local cleanup
outcome, and effect outcomes in reverse acceptance order. A disconnected client
has unknown local cleanup unless it already confirmed completion. Failed and
unknown retractions remain in these reports. They count as finished attempts,
not confirmed rollback. Reports remain in memory until the coordinator stops.

Invalid declarations and operations return `{"event":"error","reason":"..."}`.
Declaration errors use the lifecycle topic. SUBACK and PUBACK acknowledge MQTT
transport only. They do not confirm component admission or service completion.
Any provider response completes the request from the coordinator's perspective.
The caller interprets the opaque response and decides whether it can send `ready`.

## Initialization abort and retry

During Starting, publish a non-retained request to `$component/abort` to abandon
initialization. The optional payload records an opaque failure reason. The plugin
sends `deactivated`, rejects new work, and runs normal cleanup. The component
waits for `cleanup_requested`, finishes its local teardown, and sends
`cleanup_complete`. Outstanding service requests remain recorded and their
responses still lead to retraction.

Activation has one block: `none`, `disabled`, or `aborted`. Abort sets it to
`aborted`. After cleanup finishes, the plugin returns `{"event":"aborted"}`.
The component remains Inactive until retry or administrative enable clears the
block. Dependency changes do not clear it. Cleanup reports preserve the abort
reason as `initialization_failure`; that history does not block activation.

Publish a non-retained request to `$component/retry` from the same component
connection to clear an `aborted` block. The plugin accepts retry only in Inactive
and returns `{"event":"retry_accepted"}` before any new `initialize` notification.
If the block is `none` and all dependencies are available, activation starts
immediately. Otherwise it remains Inactive. Missing dependencies trigger normal
activation when they become available; no second retry is needed. Retry also
works when there is no block. It leaves a `disabled` block unchanged. A disabled
component still needs administrative enablement.

Abort and retry use MQTT Response Topic and Correlation Data when supplied.
Otherwise their responses use the component's lifecycle topic. Abort is rejected
outside Starting. Retry is rejected in Starting, Active, or Stopping, including
while abort cleanup remains unfinished. Both return `invalid_state_or_operation`
when used in the wrong state or with RETAIN set. Reconnecting creates a fresh
component without the previous connection's failure.

## State and subscriptions

State resources require the EMQX retainer to be enabled. A provider declares
`$provide/state/X` and publishes retained values to `$state/X` while Starting or
Active. The plugin writes retained state synchronously and records a delete
action. Repeated writes keep that action. An empty payload deletes the value.
Cleanup deletes the final retained value after dependent cleanup finishes.
It does not retract messages already delivered to subscribers.

A consumer declares `$consume/state/X`. This records a dependency and grants
access through its committed provider binding. No `$consume/...` declaration
installs a resource subscription. After `initialize`, the consumer may send
SUBSCRIBE for the exact `$state/X` topic. The plugin records an unsubscribe
action in the activation's cleanup order. An explicit UNSUBSCRIBE completes
that action only after the `session.unsubscribed` hook confirms removal.
Each new activation subscribes again if it needs state updates.

The plugin permits managed subscriptions only to declared state topics,
the active provider's service filters, and the client's own lifecycle topic.
It rejects shared subscriptions to managed topics, undeclared topics, and
wildcards that exceed the declaration. Ordinary MQTT subscriptions are unchanged.
When a packet contains a forbidden filter, none of its filters are installed.
The plugin sends `subscription_not_allowed`; SUBACK alone does not report this
hook-level rejection. Virtual declaration filters remain installed.

## Explicit release

Publish an empty, non-retained message to `$component/release/<effect-id>` to
release a service effect during Starting or Active. Only its owning activation
may release it. The plugin waits for apply completion, sends the recorded
retract request, and responds after the provider reports its outcome:

```json
{"event":"released","effect_id":"123","status":"retracted"}
```

The status may also be `failed` or `unknown`. Use MQTT Response Topic and
Correlation Data for the response, or receive it on the lifecycle topic.
Repeating a completed release returns the recorded outcome without another
retract. Automatic cleanup skips that effect. The record expires when activation
cleanup finishes.

## Administrative control

Publish non-retained JSON to `$component-admin/disable` or
`$component-admin/enable`:

```json
{"clientid":"crouter"}
```

The target is the currently connected component with that MQTT Client ID.
Disable sets `activation_block = disabled`, replacing any `aborted` block. It
withdraws the component's provisions and stops its dependents through normal
cleanup. The client remains connected to handle retractions. Enable clears
either block and permits a new activation once cleanup finishes and dependencies
resolve. Enable during Stopping does not interrupt cleanup. A reconnect starts
with no block; the setting belongs to the connection.

Responses contain `event` (`disabled` or `enabled`) and `clientid`. They confirm
the command was accepted; they do not confirm cleanup or activation completion.
The caller need not be a component. Restrict these topics through normal broker
publish authorization. The PoC adds no separate administrator identity store.

## Web server example

`test/emqx_mqtt_components_demo.erl` implements both component roles:

- `crouter` runs an HTTP listener and provides `$service/reg-route`.
- Each `chandler` registers JSON containing `handle_topic` and `path_prefix`.
- The router stores each route under its registration effect ID.
- For each HTTP request, the router selects the longest matching prefix and
  publishes JSON containing `verb`, `path`, and `body` to `handle_topic`.
- MQTT Response Topic and Correlation Data connect the response to the waiting
  HTTP request. The handler returns JSON containing `status` and `body`.
- Retracting the registration removes its route. Other routes remain available.

The suite sends real HTTP requests through real MQTT clients. It covers multiple
handlers, prefix selection, handler disconnect and reconnect, waiting for the
router, router disconnect and reconnect, declaration errors, dependency chains,
pending initialization, interrupted initialization, stale responses, reverse
cleanup order, rejected service calls, and ordinary MQTT traffic. Transitive
tests cover graceful and abrupt provider loss while two branches initialize,
early reconnect rejection, premature cleanup acknowledgements, and a diamond
dependency graph. Controlled providers store actual effects and delay apply
and retract completion. Tests check those effects, independent branch cleanup,
failed and unknown outcomes, provider loss during retraction, and stale replies.
State tests cover retained replay, replacement and deletion, explicit subscription
cleanup, confinement, and transitive disable during initialization. Release tests
check ownership, pending applies, repeated releases, and later automatic cleanup.
The web example also tests disabling and re-enabling the router while connected.
Local cleanup tests check that dependents drain before provider teardown, that
managed effects remain during local teardown, and that `stopped` precedes a new
activation. An explicit unsubscribe test delays removal and checks that its
cleanup record remains pending until the subscription disappears.
Abort and retry tests cover delayed cleanup, state deletion and unsubscription,
fresh activations, missing dependencies, administrative disablement, invalid
states, and opaque application error responses.

See [CONFORMANCE.md](CONFORMANCE.md) for the comparison with both the EIP and
the original paper.

```sh
TERM=dumb make plugins/emqx_mqtt_components-ct
make plugin-emqx_mqtt_components
```

## PoC limits

The PoC uses one global coordinator without tenant isolation. Administrative
disable runs cleanup while the component stays connected. There is no separate
permanent retirement command. The component decides whether an application
response permits readiness or requires an abort.
Normal handler request topics use ordinary MQTT request/response.

The plugin does not retry requests, deduplicate requests, persist effects, or
recover coordinator state after a restart. It has no apply or retract timeout.
A reachable provider that never replies blocks cleanup. A disconnected provider
loses its local effects in the example, but the coordinator records their
rollback as unknown. Connected dependents must report `cleanup_complete`
before restarting. Cleanup history has no retention limit in this PoC.

The plugin uses existing channel subscription commands and direct delivery.
Before activating a provider, a local helper polls the broker's subscriptions
every 10 ms for up to 100 retries. If required subscriptions remain missing,
the plugin removes partial provider subscriptions and reports
`subscription_installation_failed`. The component stays Starting and may retry
`ready`. Polling uses timer messages so the coordinator can process channel hooks.
Consumed-state cleanup receives removal confirmations through
`session.unsubscribed`. It also polls with the same interval to handle absent
subscriptions and disconnected clients.
A TODO calls for completion checks without fixed-interval polling. Lifecycle
subscription commands remain asynchronous. Another TODO calls for enforcing
declarations in the first SUBSCRIBE packet; currently only later declarations
are rejected. The plugin does not isolate ordinary MQTT topics, change broker
internals, or coordinate nodes. Retained writes and coordinator records are not
a shared crash-safe transaction.

## Deferred fixes

- TODO: Validate all subscription filters before committing declarations or
  sending lifecycle notifications. A rejected packet that mixes a declaration
  with a forbidden filter currently leaves the component declared and may
  start initialization. Add a test that checks unchanged metadata after rejection.
- TODO: Confirm retained-state mutations before reporting success. The Mnesia
  backend returns `ok` without storing a new topic when the table is full.
  Retainer deletion returns `ok` without deleting stored data when the backend
  is disabled. Add tests for capacity exhaustion and disabling the retainer
  between a state write and cleanup.
