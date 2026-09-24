# Trusted Client Info Implementation Plan

This plan implements [EIP-0040](https://github.com/emqx/eip/blob/main/active/0040-trusted-client-info.md) using the [recorded decisions](trusted-client-info-decisions.md).

The implementation centers on `emqx_clientinfo`, explicit trust metadata from authenticators, and consumer-specific enforcement. Address session takeover separately in [issue #188](https://github.com/emqx/emqx-dev-team-tasks/issues/188#issuecomment-5205219111).

## Agreed design

- Successful authn results can return `trusted_attrs`, a mask of trusted input fields.
- Built-in authenticators mark all actually used input variables as trusted. Operators remain responsible for backend validation.
- Returned `client_attrs` and applied client ID and zone overrides contribute to the final trusted attributes.
- Custom authn hooks must explicitly mark other input fields as trusted.
- `emqx_clientinfo` calculates trusted attributes and exposes `get_trusted(ClientInfo, Key)` and `set_trusted(ClientInfo, Key, Value)`.
- `emqx_clientinfo:set(ClientInfo, Key, Value)` updates a field and clears its trust.
- Do not retain compatibility field duplicates.
- Security-sensitive consumers should use the trusted access API.
- Evaluate the post-authentication namespace expression using trusted attributes when multi-tenancy enforcement is enabled.

## Implementation guidance: disabled-mode performance

Design the implementation so disabling trusted attributes causes no noticeable performance degradation compared with the implementation before this change.

When a consumer's trust enforcement is disabled, pass `ClientInfo` through as-is. Check the enforcement switch before doing trust-specific work. Do not recalculate trusted attributes, traverse a trust mask, or allocate a projection for that consumer. Preserve this fast path when other consumers have enforcement enabled.

## 1. Introduce emqx_clientinfo

Files:

- New `apps/emqx/src/emqx_clientinfo.erl`.
- Types in `apps/emqx/src/emqx_types.erl` and `apps/emqx/src/emqx_access_control.erl`.

Responsibilities:

1. Compose the final trusted attributes from:
   - EMQX-controlled fields.
   - Authenticator-returned values.
   - Input fields selected by the returned `trusted_attrs` mask.
   - Applied zone and client ID overrides.
   - Explicitly configured `mqtt.trusted_client_attributes`.
2. Provide `get_trusted(ClientInfo, Key)`.
3. Provide a trusted projection for consumers that need a complete map, such as template evaluation.
4. Provide `set_trusted(ClientInfo, Key, Value) -> NewClientInfo` to set or replace a trusted value. Use the same key format as `get_trusted/2`. Store the value as trusted and return the updated `ClientInfo`.
5. Provide `set(ClientInfo, Key, Value) -> NewClientInfo` to update a field and clear its trust. Use the same key format as `get_trusted/2`. The new value must not inherit trust from the previous value.

Keep the distinction between the mask returned by authn and the resulting trusted data in `ClientInfo` explicit.

Before migrating callers, define nested-key access and missing-value behavior. Missing trusted values must remain distinguishable from legitimate values such as `false`.

Test nested masks, returned attributes, overrides, missing values, configured trust, explicit anonymous access, and absent authentication metadata. Test that values set through `set_trusted/3` are returned by `get_trusted/2` and included in the trusted projection.

Test that updating a trusted field through `set/3` clears its trust and excludes the field from the trusted projection. Cover nested fields and verify that unrelated fields retain their trust.

## 2. Extend the authentication result contract

Files: `emqx_access_control.erl`, `emqx_authn_provider.erl`, and `emqx_authn_chains.erl`.

- Add the optional `trusted_attrs` mask to successful authn results.
- Preserve it through ordinary and enhanced authentication.
- Compose trust only after successful authentication.
- Do not accumulate trust from ignored or failed authenticators.
- Handle explicit anonymous access using the EIP's universal input mask.
- Ensure reauthentication replaces obsolete authentication results and trust.

Custom hooks can continue returning authentication output, but they must return a mask to trust input fields such as `username`. Document this breaking change.

## 3. Add masks to built-in authenticators

Update each provider to return the variables it actually used.

| Provider | Work |
| --- | --- |
| Built-in database | Report the identity field and namespace inputs used by the successful lookup path. |
| SQL, Redis, MongoDB | Derive masks from variables used in the executed query or command. |
| HTTP | Derive masks from variables used to construct the authentication request. |
| LDAP | Report variables used by the selected authentication method. |
| JWT | Report client variables used during token authentication and claim checks. |
| SCRAM and Kerberos | Report the client fields used by the completed authentication exchange. |

Reuse parsed template-variable information where available. Compute static masks during provider initialization when appropriate. Adjust them for runtime-dependent paths.

Do not add backend-validation heuristics. The agreed contract trusts actually used variables.

Review these details explicitly:

- Authentication-chain preconditions and their used variables.
- Namespace fallback paths.
- Multi-step authentication.
- Cached backend responses.

Test exact masks for successful paths, including fallback, cache hits, and completed multi-step authentication.

## 4. Integrate trust composition into MQTT and gateways

Files: `apps/emqx/src/emqx_channel.erl` and `apps/emqx_gateway/src/emqx_gateway_ctx.erl`.

Replace their independent authentication-result handling with shared `emqx_clientinfo` operations where the behavior is common.

### MQTT

- Compose trusted data after authentication succeeds.
- Apply client ID and zone overrides through the shared module.
- Make trusted authentication output available before `client.post_authn`.
- Update expiry handling and other direct authn-field consumers to use the new representation.
- Retain no top-level compatibility copies of relocated authentication fields.

### Gateways

- Preserve the current unsupported-client-ID-override behavior.
- Integrate trusted result composition before mountpoint rendering.
- Account for the current difference in `client_attrs` merging: MQTT merges individual keys; gateways replace the map.
- Resolve how the zone-overridable settings apply, given that gateway authentication currently forces the default zone.

Use `set_trusted/3` to set trusted values after authentication. This includes namespace derivation from trusted attributes and identity overrides.

Use `set/3` for updates that do not establish trust. Replace direct map updates where a new value could otherwise inherit the previous value's trust.

Test MQTT and gateway result composition, expiry, overrides, reauthentication, and post-authentication updates.

## 5. Migrate authorization

Files: `emqx_authz_context.erl`, `emqx_access_control.erl`, authorization sources, `emqx_message_ingress.erl`, and `emqx_delayed.erl`.

- Make authorization-context construction honor `authorization.require_trusted_attributes` in production.
- Build the enforced context through `emqx_clientinfo`.
- Migrate superuser and client ACL reads to the canonical representation.
- Remove assumptions that optional identity fields always exist. For example, `emqx_authz:authorize/5` currently requires a `username` key.
- Ensure required missing placeholders fail closed in authorization templates and external backend requests.
- Audit direct authorization callers.
- Preserve trusted data through delayed-message context persistence.
- Refresh cached authorization decisions when their trust inputs change.

Provide legacy behavior through the consumer's disabled-enforcement path without introducing duplicate fields.

Tests:

- Client-ID authentication cannot use a forged username in an ACL.
- Authn-returned superuser and ACL data work through the new representation.
- Missing trusted placeholders do not produce an unintended allow.
- Deferred authorization and reauthentication preserve the intended trust boundary.
- Disabling authorization enforcement restores the full-input behavior.

## 6. Migrate namespace, mountpoint, and limiter consumers

### Multi-tenancy

Files: `emqx_mt_hookcb.erl`, `emqx_mt_config.erl`, and related multi-tenancy consumers.

When `multi_tenancy.require_trusted_attributes` is enabled:

- Pass trusted attributes to `post_auth_tns_expression`.
- Evaluate it after authentication and trust composition.
- Record the resulting namespace through `emqx_clientinfo:set_trusted/3`.
- Use trusted data for authoritative namespace, managed-namespace, and quota checks.
- Review the no-expression path, whose main checks currently run before authentication.
- Ensure namespace registration and accounting agree with the namespace that passed checks.

The decision is to restrict expression input. No new expression-failure policy has been agreed.

### Mountpoints and limiters

Files: `emqx_channel.erl`, `emqx_mountpoint.erl`, `emqx_gateway_ctx.erl`, and `emqx_mt_limiter.erl`.

When `mqtt.require_trusted_attributes` is enabled:

- Use trusted namespace data for `namespace_as_mountpoint`.
- Render gateway mountpoints from trusted attributes.
- Add strict mountpoint rendering so missing fields cannot remain as literal placeholders.
- Pass trusted context to limiter adjustment.

Disabling one consumer's enforcement must not make its output trusted for another consumer.

Test forged tenant attributes, trusted namespace derivation, missing mountpoint variables, quota checks, and tenant limiter selection.

## 7. Add configuration and migration documentation

Files: `emqx_schema.erl`, `emqx_security_profile.erl`, `emqx_mt_schema.erl`, and relevant i18n files.

Add:

| Configuration | Applies to |
| --- | --- |
| `authorization.require_trusted_attributes` | Authorization rules, backend templates, superuser and client ACL handling |
| `multi_tenancy.require_trusted_attributes` | Namespace resolution, managed-namespace checks, quotas |
| `mqtt.require_trusted_attributes` | Namespace mountpoint, gateway mountpoint, limiter adjustment; session enforcement in the separate task |
| `mqtt.trusted_client_attributes` | Explicit additional trusted input paths |

Use `false` defaults for the legacy profile and `true` for hardened enforcement. Support MQTT zone overrides.

Document:

- The authn result mask.
- The `emqx_clientinfo` access API.
- Custom-hook migration.
- Built-in providers' used-variable contract.
- Compatibility settings.
- Behavior for existing connections when configuration changes.

## 8. Validate the complete flow

Run focused tests as each stage lands, then broader integration checks:

1. Core trust-composition tests.
2. Provider-specific authentication tests.
3. Authorization and channel suites.
4. Multi-tenancy and gateway suites.
5. Delayed-message authorization tests.
6. Legacy and hardened profiles and independent switch combinations.
7. `make test-compile` for broad compilation coverage.

Use Docker CT environments for external authentication backends.

## 9. Evaluate channel memory footprint

Measure the per-channel memory increase against the implementation before this change. Cover these cases:

| Case | Configuration and trusted inputs |
| --- | --- |
| Disabled | All `require_trusted_attributes` switches are disabled. |
| Enabled, client ID only | Trust enforcement is enabled. Only `clientid` is trusted among client input fields. |
| Enabled, several attributes | Trust enforcement is enabled. Trust `clientid`, `client_attrs.tns`, and two additional `client_attrs` entries. |

Include the normal EMQX-controlled fields and authentication output in the enabled cases. Use the same authentication output, input values, and attribute lengths in each before-and-after comparison. Keep untrusted input attributes present so the comparison measures trust overhead rather than differences in client data.

Measure both:

- Retained `ClientInfo` and channel-state term sizes, accounting for shared terms and referenced binaries.
- Actual channel-process memory after connections reach a stable state and garbage collection completes.

Use the same Erlang/OTP version, build profile, connection settings, and workload for the baseline and changed implementation. Measure enough channels to account for process heap allocation steps. Include idle connected channels and channels that have performed authorization so retained contexts and caches are represented.

Report:

- Baseline and changed memory usage for each case.
- Absolute increase in bytes per channel and percentage increase.
- Projected additional memory for 100,000 and 1,000,000 channels.
- Attribute sizes, measurement method, and variation across runs.
- The source of the overhead, including maps, masks, copied values, and retained projections.

Evaluate the representation once the core module is available. Repeat the measurements after consumer integration. Use the results to identify unnecessary allocations, especially when trust enforcement is disabled.

## Remaining implementation details

Define these details during implementation:

- Nested-key syntax and missing-value result for `get_trusted/2`.
- The exact static trusted-field list.
- Gateway zone resolution.
- Persistence and live-configuration handling for existing trusted contexts.

## Implementation order

1. Core module and authentication contract.
2. Built-in providers.
3. MQTT and gateway trust composition.
4. Authorization.
5. Multi-tenancy, mountpoints, and limiters.
6. Configuration and full integration validation.
7. Final channel-memory comparison against the pre-change baseline.
