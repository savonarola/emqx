-- Stop the legacy plugin and all SQL writers before running this file.
-- Install mysql.sql first. The target namespace must contain no rows.
-- This conversion supports the default legacy mqtt_msg and mqtt_sub tables only.
-- It needs window functions: MySQL 8.0 or later, or MariaDB 10.2 or later.
-- Set @omp_namespace before sourcing this file. The default is the empty namespace.
-- Messages deleted by the legacy plugin cannot be recovered.
-- Legacy tables contain no MQTT properties or independent acknowledgement progress.
-- Every converted exact subscription uses QoS 1, starts at offset zero and replays retained history.
-- Converted wildcard subscriptions keep their QoS. They are restored and receive live messages only.
SET @omp_namespace = COALESCE(@omp_namespace, '');

-- The legacy plugin wrote `arrived` with FROM_UNIXTIME() in its connection's session time
-- zone. It did not set that zone, so the server's global time zone applies by default.
-- When legacy connections used another session time zone, for example through init_connect
-- or a proxy, set @omp_source_time_zone to that zone before sourcing this file:
--   SET @omp_source_time_zone = '+08:00'
-- Check the global zone with SELECT @@GLOBAL.time_zone. SYSTEM means the server host zone.
-- Named zones such as 'Asia/Shanghai' require the MySQL time zone tables.
SET @omp_source_time_zone = COALESCE(@omp_source_time_zone, @@GLOBAL.time_zone);
SET time_zone = @omp_source_time_zone;
START TRANSACTION;

INSERT INTO mqtt_omp_topic(namespace, topic)
SELECT CAST(@omp_namespace AS BINARY), topic FROM (
    SELECT CAST(topic AS BINARY) AS topic FROM mqtt_msg
    UNION
    SELECT CAST(topic AS BINARY) AS topic FROM mqtt_sub
) AS legacy_topics;

INSERT INTO mqtt_omp_message
    (topic_id, msg_offset, message_id, sender, qos, retain, payload, properties,
     published_at, expires_at)
SELECT t.topic_id,
       ROW_NUMBER() OVER (PARTITION BY t.topic_id ORDER BY m.id),
       CASE WHEN m.msgid REGEXP '^[0-9a-fA-F]{32}$' THEN UNHEX(m.msgid) ELSE X'' END,
       COALESCE(m.sender, ''), m.qos, COALESCE(m.retain, 0), COALESCE(m.payload, X''),
       '{"version": 1, "values": {}}',
       CAST(UNIX_TIMESTAMP(m.arrived) * 1000 AS SIGNED), NULL
FROM mqtt_msg m JOIN mqtt_omp_topic t
  ON t.namespace = CAST(@omp_namespace AS BINARY) AND t.topic = CAST(m.topic AS BINARY);

UPDATE mqtt_omp_topic t SET next_offset = (
    SELECT COALESCE(MAX(m.msg_offset), 0) + 1 FROM mqtt_omp_message m WHERE m.topic_id = t.topic_id
) WHERE t.namespace = CAST(@omp_namespace AS BINARY);

INSERT INTO mqtt_omp_subscription
    (namespace, client_id, topic_id, subopts, ack_offset)
SELECT CAST(@omp_namespace AS BINARY), CAST(s.clientid AS BINARY), t.topic_id,
       CASE WHEN s.topic LIKE '%+%' OR s.topic LIKE '%#%'
            THEN CONCAT('{"qos": ', COALESCE(s.qos, 0), '}')
            ELSE '{"qos": 1}' END,
       0
FROM mqtt_sub s JOIN mqtt_omp_topic t
  ON t.namespace = CAST(@omp_namespace AS BINARY) AND t.topic = CAST(s.topic AS BINARY);

COMMIT;
