## 3.0.0

- Use independent subscriber progress, ordered replay, batching and retention for MySQL.
- Replace custom MySQL queries with a fixed schema. Provide schema installation and legacy conversion SQL files.
- Allow PostgreSQL and MySQL to run simultaneously with disjoint topic filters.
- Accept wildcard subscriptions over SQL-managed topics at any QoS. They are stored and restored
  on connect, and they receive live messages without replay.
- Save subscriber progress only while acknowledgements are unsaved. Idle subscriptions run only
  a renewal timer at a third of `subscription_expiry`. The `checkpoint.interval` default changes
  from 1 to 5 seconds.
- Replace MySQL `pool_size`, `batch_size` and `batch_time` with separate connection pool and writer settings.

Breaking changes:

- Upgrade from earlier versions requires a full stop. Stop the old version on all nodes, then
  configure and start 3.0.0. The old configuration is not used.
- MySQL uses new `mqtt_omp_*` tables. Run `priv/mysql-convert-legacy.sql` to convert data from
  the default `mqtt_msg` and `mqtt_sub` tables.
- MySQL no longer accepts custom SQL settings.
- A MySQL acknowledgement advances the progress of one subscriber instead of deleting the message.
- Authorization denies exact-topic subscriptions to MySQL-managed topics with QoS other than 1,
  and subscriptions from durable sessions or with client IDs longer than 2048 bytes. Exact-topic
  subscriptions receive messages at QoS 1. Wildcard subscriptions receive live messages only.
- MySQL requires MySQL 8.0 or MariaDB 10.6 or later.
- MySQL TLS verifies the server certificate and host name by default. A TLS connection without
  `cacertfile` or `node.trusted_certs` fails.

## 2.0.0

- Add PostgreSQL storage for offline messages with per-client replay progress and optional asynchronous batching of QoS 1/2 publishes. QoS 0 uses regular broker dispatch.
- Add the `emqx_offline_messages` plugin to the monorepo.
