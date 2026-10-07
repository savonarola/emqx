# Trusted Client Info Decisions

This document records implementation decisions for [EIP-0040](https://github.com/emqx/eip/blob/main/active/0040-trusted-client-info.md).

## Authentication result contract

Authentication results can include an additional `trusted_attrs` field. This field contains a mask of client input fields that the authenticator treats as trusted.

For example, an authenticator that uses `username` and `client_attrs.tns` returns:

```erlang
{ok, #{
    trusted_attrs => #{
        username => true,
        client_attrs => #{<<"tns">> => true}
    }
}}.
```

Use this mask to compose the final trusted attributes. Extend the final trust set with:

- Client attributes returned in the authentication result's `client_attrs` map.
- The resulting `zone` and `clientid` values when authentication overrides are applied.

Trusting a returned client attribute does not trust other attributes already present in the runtime `client_attrs` map.

An authentication result can return `trusted_attrs => true` to trust all client input. When tracking is enabled, store this mask directly as `ClientInfo.trusted_attrs => true`. Do not expand it into a map or retain exclusions. Explicit anonymous access uses this mask.

## Retained client information layout

Keep `is_superuser`, `auth_expire_at`, and `acl` at the top level. Include them in the static trusted keys. Normalize authentication's `expire_at` to `auth_expire_at`. Reset these known outputs on each successful authentication.

Store the input trust mask directly in `ClientInfo.trusted_attrs`. Store additional authentication outputs in `ClientInfo.authn`. Omit the `authn` map when empty. Reauthentication replaces `authn` in every mode and replaces the input trust mask when tracking is enabled.

Apply only known authentication outputs to client information. Use `client_attrs`, `clientid_override`, and `zone_override` for supported updates. Keep every unknown output in `authn`, including keys that match client information fields. Client information access must not implicitly read the `authn` namespace. This separation prevents collisions without lists of reserved client information keys.

When all consumer enforcement switches are disabled, omit `trusted_attrs` entirely. Apply zone overrides before checking the switches. Keep nonempty custom output in the separate `authn` map. Do not merge unknown outputs into client information in any mode.

## Built-in authenticators

Built-in authenticators treat all actually used input variables as trusted. They report these variables in the authentication result's `trusted_attrs` mask.

EMQX does not prove that a customer-controlled backend validates every variable it uses. Operators must ensure that their authentication configuration and backend validate all attributes reported as trusted. An overpermissive backend can violate this contract.

## Custom authentication hooks

The trusted-input contract is a breaking change for authentication hooks.

A successful authentication result does not implicitly trust client input fields. When a hook wants to trust an input field such as `username`, it must explicitly include that field in the returned `trusted_attrs` mask.

Returned `client_attrs` and applied zone or client ID overrides contribute trust as described above. Hooks do not need to list those values separately in the mask.

## Trusted attribute calculation and access

Introduce `emqx_clientinfo` to centralize trusted attribute calculation and access. Compose the final trusted attributes through this module.

Access a trusted attribute through:

```erlang
emqx_clientinfo:get_trusted(ClientInfo, Key)
```

Set or replace a trusted attribute through:

```erlang
NewClientInfo = emqx_clientinfo:set_trusted(ClientInfo, Key, Value)
```

`set_trusted/3` stores the value as trusted and returns the updated `ClientInfo`. Use it for trusted updates after authentication, including a namespace derived from trusted attributes.

Set or replace an attribute without adding trust through:

```erlang
NewClientInfo = emqx_clientinfo:set(ClientInfo, Key, Value)
```

When the input trust mask is a map, `set/3` removes the updated path from that map. When `trusted_attrs => true`, both setters only update the value and preserve the mask. EMQX-controlled fields remain statically trusted. Use the same key format for all three methods. Route attribute updates through these setters instead of modifying the map directly.

Do not retain compatibility field duplicates. Update consumers to use the trusted attribute accessor.

Avoid direct access to client attributes in contexts that require a trusted value. Use `emqx_clientinfo:get_trusted/2` in those contexts so consumers do not implement their own trust checks.

Access custom authentication output through `authn.<key>`, or use an explicit path such as `[authn, custom_field]` with `get_trusted/2`. Preserve the `authn` namespace in trusted projections and persisted authorization contexts.

Use trusted accessors for known `is_superuser`, `acl`, and `auth_expire_at` outputs. Keep the channel's `trusted_value/2` helper to express that requirement. Use `get_trusted/3` when a consumer needs a default for a missing or untrusted field. These known top-level outputs are always trusted. Keep their fast path inside `emqx_clientinfo` so callers do not bypass trust checks for arbitrary fields. This fast path must not check enforcement, traverse masks, or allocate a projection.

## Session takeover

Address session takeover policy separately in [issue #188](https://github.com/emqx/emqx-dev-team-tasks/issues/188#issuecomment-5205219111).

## Consumer enforcement switches

Use the following mapping:

| Switch | Consumers |
| --- | --- |
| `authorization.require_trusted_attributes` | Authorization rules, backend request templates, superuser and client ACL handling |
| `multi_tenancy.require_trusted_attributes` | Namespace resolution, managed-namespace checks, quota decisions |
| `mqtt.require_trusted_attributes` | Session takeover and discard, `namespace_as_mountpoint`, gateway mountpoint rendering, limiter adjustment |

Session takeover and discard enforcement will be addressed in the separate task linked above.

Disabling enforcement for one consumer must not add trust to its results for another consumer. An existing unconditional trust mask remains unconditional after updates.

## Disabled-mode performance

Disabling trusted attributes must not cause noticeable performance degradation compared with the implementation before this change.

When a consumer's trust enforcement is disabled, pass `ClientInfo` through as-is. Do not recalculate trusted attributes, traverse a trust mask, or build a projection for that consumer. Keep this fast path independent of enforcement enabled for other consumers.

When all consumer enforcement switches are disabled, skip input trust composition at authentication. Setters must not create trust metadata in this mode. If enforcement is later enabled, treat discarded input trust as absent until reauthentication or reconnect. Do not infer unconditional trust from missing metadata.

## Post-authentication namespace expression

When multi-tenancy trust enforcement is enabled, pass only trusted attributes to `multi_tenancy.post_auth_tns_expression`. Evaluate it after authentication. This prevents untrusted attributes from selecting a tenant namespace.
