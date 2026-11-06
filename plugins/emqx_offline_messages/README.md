# Offline Messages

This plugin stores messages in an external database and replays them to subscribers.
MySQL and PostgreSQL store matching QoS 1/2 publishes and track independent progress for each client.
Redis stores selected QoS 1/2 messages for offline replay.

Supported backends:

- [MySQL](MYSQL.md), with ordered live delivery and replay, and independent progress for each client
- Redis
- [PostgreSQL](POSTGRESQL.md), with ordered live delivery and replay, and independent progress for each client

## Configuration

The plugin is configured through the standard EMQX plugin configuration API:

`PUT /api/v5/plugins/<name-vsn>/config`

The defaults are in `priv/config.hocon`. The plugin configuration schema is `priv/config_schema.avsc`.

The backends only persist messages when:

- the publish QoS is greater than `0`
- the topic matches one of the configured `topics` filters

MySQL and PostgreSQL can run simultaneously with disjoint topic filters.
The plugin rejects overlapping topic filters between these two backends.

## Build And Test

Build the plugin from the repository root:

```bash
make
```

Run this plugin's Common Test suite:

```bash
./scripts/ct/run.sh --app plugins/emqx_offline_messages
```

## Database Schema

### MySQL

Use `priv/mysql.sql` or enable `mysql.init_default_schema` to provision the tables at startup.
See [MySQL](MYSQL.md) for requirements and conversion from the legacy tables.

### Redis

Redis uses hashes and sorted sets under the `mqtt:sub:*` and `mqtt:msg:*` key spaces.

- Subscription state is stored in hashes keyed by client ID.
- Message payloads are stored in hashes keyed by message ID.
- Topic indexes are stored in sorted sets keyed by topic.
