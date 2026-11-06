# MySQL backend

The MySQL backend uses the same delivery runtime as PostgreSQL.
It stores QoS 1/2 publishes and delivers them at QoS 1 in per-topic offset order.
Each subscriber has independent progress. An acknowledgement does not delete another subscriber's messages.
See [PostgreSQL](POSTGRESQL.md) for the shared subscription, replay, batching and retention semantics.

## Requirements

- EMQX 7.0 or later on every node.
- MySQL 8.0 or later, or MariaDB 10.6 or later, with InnoDB.
  One `omp_append` stored procedure appends message batches on all these servers.
  It reads its JSON argument with `JSON_TABLE`. Tables use no JSON column type and no foreign keys.
  The test suite runs against MySQL 8.4. The schema, procedure, queries and legacy conversion
  were also checked manually on MariaDB 10.6 and 10.11. Other servers are not tested.
- The same plugin version, namespace and topic configuration on all nodes serving those topics.
- Topic names of at most 2048 bytes.
- Client IDs of at most 2048 bytes. Authorization denies a client with a longer client ID
  a subscription to a MySQL-managed topic. Such a client receives no MySQL-managed messages.

## Setup

Create a database and an account with access to it. Install the packaged schema:

```sh
mysql -h mysql.example.net -u owner -p mqtt < priv/mysql.sql
```

The schema owner needs `CREATE`, `INDEX`, `INSERT`, `CREATE ROUTINE` and `ALTER ROUTINE` privileges.
The plugin account needs `SELECT`, `INSERT`, `UPDATE`, `DELETE` and `EXECUTE` privileges:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON mqtt.* TO 'emqx'@'%';
```

Alternatively, set `init_default_schema = true`. The plugin installs the schema at startup when
the schema version or the `omp_append` procedure is missing. The plugin account then also needs
the schema owner privileges. Installation is serialized across plugin nodes.
A node waits at most `query_timeout`, rounded up to whole seconds, for another node's installation.
If the wait ends first, the start fails and the backend health check fails.
The plugin does not change a complete schema.
A failed installation, a missing procedure or an incompatible schema version fails the backend health check.
The plugin does not create the database or database account.

```hocon
mysql {
    enable = true
    server = "mysql.example.net:3306"
    database = "mqtt"
    username = "emqx"
    password = "secret"
    namespace = "production"
    topics = ["mysql-offline/#"]
    init_default_schema = false
    ssl { enable = true, verify = verify_peer, cacertfile = "/certs/ca.pem" }
}
```

Connection settings are `server`, `database`, `username`, `password`, `ssl` and `query_timeout`.
Delivery settings match PostgreSQL: `namespace`, `topics`, `async_batched`, `max_topic_length`,
`new_subscription_start`, `writer`, `read`, `checkpoint`, `retention` and `subscription_expiry`.
The three connection pool settings are `message_publish_pool_size`, `message_read_pool_size`
and `subscription_meta_pool_size`.
MySQL does not have the PostgreSQL `set_statement_timeout` setting.

MySQL has no server-side timeout for writes. The plugin limits row-lock waits instead:

- Each connection sets `innodb_lock_wait_timeout` to `query_timeout`, rounded up to whole seconds.
  A statement that waits longer for a row lock fails on the server and is rolled back.
- The plugin waits for a reply 5 seconds longer than the lock wait.
  With the default `query_timeout` of 5000 ms, it waits up to 10 seconds.

A statement that stays slow for other reasons, for example a disk stall, can still exceed this time.
The publisher is then disconnected, and the message may be stored anyway.

TLS verifies the server certificate and host name by default: `verify = verify_peer` and
`server_name_indication = auto`. Set `cacertfile`, or configure `node.trusted_certs`.
Without a trusted CA certificate, the connection fails.

MySQL limits the size of one statement with `max_allowed_packet`.
A batch of messages is sent in one `omp_append` call with base64-encoded payloads.
Set `max_allowed_packet` above 1.4 times `writer.flush_max_bytes` plus the largest expected payload.

PostgreSQL and MySQL can run simultaneously. Their topic filters must not overlap.
Do not assign Redis the topics managed by either SQL backend.

## Upgrade from plugin versions before 3.0.0

Version 3.0.0 is a major upgrade. Stop the old plugin version on all nodes before you start
version 3.0.0. Do not run both versions in one cluster.

The new backend uses fresh `mqtt_omp_*` tables. It does not read `mqtt_msg` or `mqtt_sub`.
Version 3.0.0 starts with its default configuration. Configure it again. Do not copy the old configuration.
The six custom SQL settings, `pool_size`, `batch_size` and `batch_time` no longer exist.
Use the three connection pool settings and `writer` settings instead.
Custom queries are no longer supported.

Subscription behavior changes for MySQL-managed topics:

- An acknowledgement advances the progress of one subscriber. It does not delete the message for others.
- Authorization denies exact-topic subscriptions to managed topics with QoS other than 1, and
  subscriptions from durable sessions or with client IDs longer than 2048 bytes.
- Exact-topic subscriptions receive messages at QoS 1.
- Wildcard filters that overlap managed topics are accepted at any QoS. They are stored and
  restored on connect, and they receive live messages only. Stored messages are not replayed to them.
  See [PostgreSQL](POSTGRESQL.md#subscribing).

To retain data from the default legacy schema:

1. Stop the old plugin version on all nodes, and all other writers to its tables.
2. Install `priv/mysql.sql` in the same database.
3. Ensure the target namespace contains no rows. Back up the source tables.
4. Review `priv/mysql-convert-legacy.sql`. By default, it reads legacy `arrived` values in the
   server's global time zone, which the legacy plugin used. Its comments describe how to set
   `@omp_source_time_zone` when the legacy connections used another time zone.
5. Run it with the target namespace:

   ```sh
   mysql -h mysql.example.net -u owner -p mqtt \
     -e "SET @omp_namespace = 'production'; SOURCE priv/mysql-convert-legacy.sql;"
   ```

6. Configure version 3.0.0 with the same namespace and enable the MySQL backend.

The conversion assigns offsets within each topic in legacy `id` order.
It converts exact subscriptions to QoS 1 and starts their progress at zero.
Converted exact subscribers replay the retained history.
Wildcard subscriptions keep their QoS. They are restored on connect and receive live messages only.
Invalid or missing message GUIDs receive new GUIDs during replay.
The legacy tables do not contain MQTT properties, message expiry or independent acknowledgement progress.
Messages already deleted by the legacy plugin cannot be recovered.
Customized legacy schemas require adapted conversion queries.
The conversion does not remove the source tables.
