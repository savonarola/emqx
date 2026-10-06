# Component lifecycle conformance

This comparison covers the PoC's state and service lifecycle. It does not claim full
conformance with the EIP or prove arbitrary service inverses correct.

Sources:

- [MQTT Component Model EIP](https://github.com/savonarola/eip/blob/20260909-mqtt-component-model/active/0046-mqtt-component-model.md):
  component transitions, service resources, withdrawal and cleanup, and invariants.
- [A Programming Paradigm for Spatiotemporal Composability](https://arxiv.org/pdf/2608.25512):
  sections 4.2.2 and 4.4, and Algorithm 5 in section 5.1.3.

## Implemented rules

| Paper rule | EIP rule | Implementation | E2E coverage |
|---|---|---|---|
| O-Insert requires disjoint provided keys. | Reserve keys until the old declaration is removed. | `declare/4` includes disconnected components still stopping. | `t_dependency_chain`, `t_delayed_transitive_retractions` reject replacements during cleanup. |
| L-Begin fixes the committed view. | Bind dependencies to provider activations. | `activate_waiting/1` stores connection and activation IDs. Effects keep both owner and provider activation IDs. | `t_interrupted_initialization`, `t_delayed_transitive_retractions` check replacement and stale responses. |
| L-Finish requires completed initialization. | Do not activate while initialization requests are pending. | `operation/4` checks recorded apply completion before accepting `ready`. | `t_service_initialization_and_reverse_cleanup`, `t_transitive_initialization_disconnect`. |
| L-Divert retains effects from interrupted initialization. | Keep pending effects after withdrawal. | `stop_component/2` preserves effects. `service_reply/4` accepts late completion without forwarding it to stopped work. | `t_delayed_transitive_retractions` delays an apply until after withdrawal and checks its removal. |
| Algorithm 5 withdraws availability before recovery. | Reject new work while Stopping. | `stop_component/2` withdraws the transitive closure within one coordinator call. | Both `t_transitive_initialization_*` cases reject calls and readiness after withdrawal. |
| L-Unload waits for committed dependents. | Complete dependent cleanup before provider cleanup. | `maybe_cleanup/2` waits for bindings to be released before `cleanup_local/4` requests local teardown. Early completion is rejected. | `t_delayed_transitive_retractions` checks actual provider effects at each delayed inverse. `t_diamond_initialization_disconnect` covers shared dependencies. `t_local_cleanup_waits_for_dependents` checks the local teardown permission and completion notifications. |
| Recovery runs inverses in LIFO order. | Run cleanup in reverse acceptance order. | `cleanup_effects/4` sends one inverse per activation and waits for its result. | `t_service_initialization_and_reverse_cleanup`, `t_transitive_initialization_disconnect` check order across providers. |
| Section 4.4 awaits asynchronous work. | Confirm retractions through provider responses. | `cleanup/3` waits for reachable pending applies. `retract_reply/4` validates the responder and result. | `t_delayed_transitive_retractions`, `t_provider_disconnect_during_retraction`. |
| Independent branches can progress independently. | Sibling cleanup can interleave. | Each activation has its own cleanup progress. | D finishes while A's inverse remains blocked in `t_delayed_transitive_retractions`. |
| Context operations record inverses. | Delete retained state owned by a stopping activation. | `write_state/5` uses the synchronous retainer API and records one delete action per state key. | `t_state_initialization_and_empty_write`, `t_state_subscription_cleanup` cover initial writes, replacement, deletion, and reconnect. |
| Dependency access follows the committed view. | Subscribe to consumed state only during Starting or Active. | Declarations create no consumer subscriptions. `record_subscription/3` records an unsubscribe action for an explicit state subscription. | `t_state_subscription_cleanup` checks unsubscribe, resubscribe, cleanup, and reactivation without automatic subscriptions. `t_explicit_unsubscribe_waits_for_removal` checks that an unfinished explicit unsubscribe keeps its inverse pending. |
| Confinement limits access to declared resources. | Reject managed subscriptions outside declarations. | `subscription_allowed/4` checks lifecycle state, exact state topics, provider service filters, and the client's lifecycle topic. | `t_subscription_confinement` checks undeclared, wildcard, shared, and other-client lifecycle subscriptions. |
| Completed inverses need not run again. | Allow explicit release of a service effect. | `release/5` checks ownership and waits for the recorded retract outcome. | `t_explicit_effect_release`, `t_release_during_pending_apply` check actual effects, duplicate release, and release during withdrawal. |
| Availability changes trigger withdrawal and recovery. | Administrative disable stops a component and its dependents. | `admin/4` sets or clears the connection's activation block. Enable waits for cleanup and dependencies. | `t_admin_web_router`, `t_state_transitive_disable` check HTTP routes and state cleanup through multiple dependency levels. |

## Local cleanup protocol

`deactivated` withdraws the activation and stops new work. It does not permit
provider resource destruction. After dependents finish and reachable pending
applies complete, `cleanup_requested` permits local teardown. The component
sends `cleanup_complete` when that teardown finishes. Managed inverses then run.
The coordinator sends `stopped`, or the `aborted` response for initialization
abort, after cleanup finishes. A later `initialize` follows that completion.

This places the wait for dependents before both local teardown and managed
inverses, as required by Algorithm 5. It also keeps the component's own managed
effects available until local teardown completes.

## MQTT failure outcomes

An explicit initialization abort enters Stopping and preserves accepted work for
cleanup. The component remains Inactive after cleanup until retry or
administrative enable clears its `aborted` block.
`t_abort_retry_cleanup` checks delayed applies and inverses, retained-state
deletion, unsubscription, and a fresh activation. `t_abort_waits_for_retry` checks
that provider replacement does not clear the block. `t_admin_enable_after_abort`
checks that enable clears it while Inactive or after pending cleanup finishes.
`t_disable_replaces_abort` checks that disable replaces the abort block without
requiring a separate retry after enable.
`t_retry_without_dependencies` checks that an accepted retry permits activation
when dependencies later appear. `t_retry_respects_disable` checks that retry
does not enable a disabled component. `t_retry_and_abort_invalid_states` checks
state restrictions and that application error responses do not trigger abort.

This is the MQTT adaptation of the paper's explicit initialization failure and
retry. Retry keeps the MQTT connection and declarations and permits a fresh
activation. The paper describes retry through revision of the runtime instance.

The EIP specifies unknown local cleanup after unconfirmed client disconnect.
It also requires failed or unknown retractions to remain recorded when their
provider cannot confirm rollback. `cleanup/3` records unreachable effects as
unknown. `finish_cleanup/3` retains all outcomes in `cleanup_results/0`.
Responses carry outcomes in the `component-status` MQTT User Property.
Framework response payloads are empty. Apply response payloads remain opaque.
`t_response_properties` checks status and payload forwarding, correlation,
coordinator-assigned effect IDs, and a retract outcome with an opaque payload.

`t_failed_and_unknown_retractions` verifies that failed inverses leave real
provider effects present and reports those outcomes. It also verifies that the
remaining inverses still run. `t_provider_disconnect_during_retraction` covers
loss after a retract was sent, invalid acknowledgements, and a forged response.
The two transitive initialization cases cover MQTT DISCONNECT and connection loss.
Physical disconnect marks the component unreachable before withdrawing its
dependents. It sends no deactivation or local cleanup request to that component.
Connected dependents still finish ordered cleanup. Administrative disable keeps
the target connected so it can serve retractions and acknowledge local cleanup.
`t_physical_disconnect_debug` and `t_physical_connection_loss_debug` check this
distinction after a disable/enable cycle. A prior activation's cleanup completion
does not confirm cleanup for the disconnected activation.

`t_activation_waits_for_subscriptions` checks that a provider becomes available
only after its subscriptions appear. `t_subscription_installation_failure`
checks partial installation, timeout, and retry after freeing subscription space.
`t_dependency_loss_during_subscription_installation` checks that a pending
installation cannot activate a component after its dependency disappears.

A terminal failed or unknown attempt lets lifecycle cleanup finish. It does not
mean that the external effect disappeared. The PoC does not retry that attempt
against a replacement provider.

## Scope and deviations

- The implementation has one global coordinator on one broker node without
  tenant isolation. Administrative disable runs cleanup while the component
  stays connected. There is no separate permanent retirement command.
- The hook recognizes the first declaration packet, not necessarily the first
  SUBSCRIBE packet on the connection. Enforcement has a TODO. Virtual
  declaration subscriptions remain installed.
- Provider subscriptions use queued channel commands followed by a local poll of
  installed topics before activation. State unsubscription completes through
  `session.unsubscribed`. Polling handles absent subscriptions and disconnected clients.
  Replacing fixed-interval checks has a TODO. Lifecycle subscriptions have no
  completion check. Declaration rejection uses SUBACK failure codes and an MQTT 5
  Reason String. It prevents the packet's installations and sends no lifecycle error.
  Other managed-subscription rejection still uses `subscription_not_allowed`
  on the lifecycle topic.
- State writes and deletes use synchronous retainer APIs. They are not atomic
  with in-memory coordinator metadata across crashes. State support requires
  an enabled retainer. An accepted client subscription records its inverse
  before the channel installs it; a failed installation leaves a harmless
  unsubscribe action.
- TODO: Validate a whole subscription packet before committing declarations or
  sending initialization notifications. A rejected mixed packet currently
  retains the declaration and can start activation. See [deferred fixes](readme.md#deferred-fixes).
- TODO: Check retained-state mutations rather than treating every retainer `ok`
  as confirmation. A full table can skip a write, and a disabled backend can
  skip deletion. These cases do not yet satisfy the EIP's state completion rule.
- Apply responses are opaque. The client decides whether their application result
  permits `ready` or whether to send `abort`. The plugin does not infer failure
  from a service response.
- Each accepted request creates an effect. Correlation Data identifies a response;
  it does not deduplicate retries. Service responses expose `component-effect-id`
  as an MQTT User Property for explicit release.
- Administrative topics rely on broker publish authorization. The activation block
  belongs to the connection and does not survive reconnect.
- Effect IDs are unique within the broker VM. There is no journal, crash recovery,
  apply/retract timeout, or persistence. Cleanup reports also disappear on
  coordinator restart.
- Ordinary handler request topics stay outside the managed service lifecycle.
- Providers must implement serial apply/retract processing per effect and valid
  inverses. The web example removes routes by effect ID. These tests check selected
  executions; they do not establish commutativity for arbitrary provider code.
