# Trusted Client Attributes

EMQX tracks which client information fields authentication has validated. Security-sensitive
consumers can use a trusted projection instead of the complete runtime `ClientInfo` map.

## Configuration

The default for each enforcement switch depends on `EMQX_SECURITY_PROFILE`.

| Configuration | Legacy default | Hardened default | Consumer |
| --- | --- | --- | --- |
| `authorization.require_trusted_attributes` | `false` | `true` | Authorization rules, backend templates, superuser checks, and client ACL checks |
| `multi_tenancy.require_trusted_attributes` | `false` | `true` | Namespace resolution, managed namespace checks, and quota checks |
| `mqtt.require_trusted_attributes` | `false` | `true` | Namespace mountpoints, gateway mountpoint templates, and limiter adjustment |

`mqtt.require_trusted_attributes` supports zone overrides through
`zones.<zone>.mqtt.require_trusted_attributes`.

Use `mqtt.trusted_client_attributes` to trust an additional client information path. Use dotted
paths for nested values.

```hocon
mqtt.trusted_client_attributes = ["clientid", "client_attrs.tns"]
```

This setting also supports zone overrides. Configure a path only when an external control validates
its value.

## Authentication result contract

A successful authentication result can contain a `trusted_attrs` mask.

```erlang
{ok, #{
    is_superuser => false,
    trusted_attrs => #{
        username => true,
        client_attrs => #{<<"tns">> => true}
    }
}}.
```

Custom authentication hooks must return this mask for client input fields that they validate. A
successful result does not trust other client input fields automatically.

Built-in authenticators report the non-secret template variables that they use. `${password}` does
not enter the trusted projection. Client attributes returned by authentication are trusted. Applied
client ID and zone overrides are also trusted.

Explicit anonymous access returns `trusted_attrs => true`. When tracking is enabled, EMQX retains
this as `clientinfo => true`.
This mask trusts all client input, including later updates. EMQX does not expand it or track exclusions.

## Client information layout

Known authentication outputs remain at the top level. `is_superuser`, `auth_expire_at`, and `acl`
are statically trusted, like `zone`. Authentication normalizes `expire_at` to `auth_expire_at`.
It resets `is_superuser` and authentication expiry on each successful authentication. It removes
the previous ACL when the new result does not return one.

```erlang
#{
    zone => default,
    clientid => <<"client">>,
    is_superuser => false,
    auth_expire_at => undefined,
    trusted_attrs => #{clientinfo => #{clientid => true}}
}.
```

`trusted_attrs.authn` contains only additional authentication outputs. EMQX omits it when empty.
Reauthentication replaces this map and the input trust mask.

When all three enforcement switches are disabled for the effective zone, EMQX skips input trust
composition and omits `trusted_attrs` entirely. It applies zone overrides before checking the
switches. Custom authentication outputs use the legacy top-level merge behavior in this mode.
Without metadata, EMQX cannot identify and remove arbitrary custom outputs from an earlier result.

## Access API

Use `emqx_clientinfo:get_trusted/2` when a consumer needs one trusted value.

```erlang
case emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"tns">>]) of
    {ok, Namespace} -> Namespace;
    error -> undefined
end.
```

Use `emqx_clientinfo:trusted/1` when a template or hook needs the complete trusted projection.

Use `emqx_clientinfo:set_trusted/3` for a value derived from trusted input. Use
`emqx_clientinfo:set/3` for other updates. When the input trust mask is a map, `set/3` removes the
updated path from that map. When `clientinfo => true`, both setters only update the value and
preserve trust. EMQX-controlled fields remain statically trusted.

Authentication output such as `is_superuser`, `acl`, and authentication expiry is available through
`emqx_clientinfo:get_trusted/2`. These top-level fields are authoritative, not compatibility copies.
Both setters avoid creating trust metadata when all enforcement switches are disabled.

## Migration

Review custom authentication hooks before enabling enforcement. Add a `trusted_attrs` mask for each
client field that the hook validates. Review authorization and mountpoint templates for variables
that the configured authentication method does not validate.

Use one of these options during migration:

1. Use the legacy security profile. All three enforcement switches default to `false`.
2. Keep the hardened profile and set one enforcement switch to `false` while migrating that
   consumer.
3. Add a path to `mqtt.trusted_client_attributes` when another external control validates it.

Disabling one enforcement switch does not add trust to its derived values for another consumer.
An existing unconditional trust mask remains unconditional after updates.

Configuration changes affect existing connections at different times:

- Authorization enforcement applies when the next authorization context is built.
- Multi-tenancy namespace derivation and checks apply after the next authentication.
- Mountpoint rendering applies when a channel authenticates and establishes its mountpoint.
- Limiter adjustment applies when EMQX creates or recreates a limiter container.
- Changes to `mqtt.trusted_client_attributes` apply after authentication composes new trust metadata.

When a connection authenticated with all enforcement switches disabled, enabling enforcement does
not recover its discarded input trust. Consumers treat those inputs as untrusted until the client
reauthenticates or reconnects. Known authentication outputs remain statically trusted.
Disabling all switches removes retained metadata after the next successful authentication.

Reconnect clients when a change must apply to every channel immediately.

Session takeover and discard policy are outside this change. Track that work in the separate session
takeover task.
