-- MySQL 8.0 or later, or MariaDB 10.6 or later, with InnoDB.
-- Run in the selected database as its schema owner, for example:
--   mysql -h HOST -u OWNER -p DATABASE < mysql.sql
-- The owner needs CREATE, INDEX and INSERT privileges.
-- The plugin account needs SELECT, INSERT, UPDATE and DELETE privileges.
-- These tables are independent of the legacy mqtt_msg and mqtt_sub tables.
-- The plugin sends this file to the server as one multi-statement query.
-- CHECK constraints are enforced by MySQL 8.0.16 or later and MariaDB. Older servers ignore them.
-- Properties and subscription options are JSON text in BLOB columns. SQL does not read them.
CREATE TABLE IF NOT EXISTS mqtt_omp_schema (
    version INTEGER NOT NULL PRIMARY KEY CHECK (version = 1)
) ENGINE=InnoDB;

-- One row per exact topic with the next offset to allocate.
-- The row is kept after cleanup, so offsets are never reused.
CREATE TABLE IF NOT EXISTS mqtt_omp_topic (
    topic_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    namespace VARBINARY(256) NOT NULL,
    topic VARBINARY(2048) NOT NULL CHECK (OCTET_LENGTH(topic) > 0),
    next_offset BIGINT NOT NULL DEFAULT 1,
    UNIQUE KEY namespace_topic (namespace, topic)
) ENGINE=InnoDB;

-- Stored messages, ordered by msg_offset within a topic.
CREATE TABLE IF NOT EXISTS mqtt_omp_message (
    topic_id BIGINT NOT NULL,
    msg_offset BIGINT NOT NULL,
    message_id VARBINARY(16) NOT NULL,
    sender BLOB NOT NULL,
    qos TINYINT NOT NULL CHECK (qos BETWEEN 0 AND 2),
    retain TINYINT NOT NULL,
    payload LONGBLOB NOT NULL,
    properties LONGBLOB NOT NULL,
    -- Unix milliseconds
    published_at BIGINT NOT NULL,
    expires_at BIGINT,
    PRIMARY KEY (topic_id, msg_offset)
) ENGINE=InnoDB;

-- One row per client and topic with the last acknowledged offset.
-- updated_at is UTC. Subscriptions not renewed within subscription_expiry are deleted.
CREATE TABLE IF NOT EXISTS mqtt_omp_subscription (
    namespace VARBINARY(256) NOT NULL,
    client_id VARBINARY(2048) NOT NULL,
    topic_id BIGINT NOT NULL,
    subopts BLOB NOT NULL,
    ack_offset BIGINT NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (namespace, client_id, topic_id),
    KEY subscription_topic (topic_id)
) ENGINE=InnoDB;

-- Write the version only after every table exists.
-- DDL commits implicitly in MySQL.
INSERT IGNORE INTO mqtt_omp_schema VALUES (1);
