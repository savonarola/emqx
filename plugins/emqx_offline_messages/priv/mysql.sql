-- MySQL 8.0 or later, or MariaDB 10.6 or later, with InnoDB.
-- Run in the selected database as its schema owner, for example:
--   mysql -h HOST -u OWNER -p DATABASE < mysql.sql
-- The owner needs CREATE, INDEX, INSERT, CREATE ROUTINE and ALTER ROUTINE privileges.
-- The plugin account needs SELECT, INSERT, UPDATE, DELETE and EXECUTE privileges.
-- These tables are independent of the legacy mqtt_msg and mqtt_sub tables.
-- The plugin splits this file like the mysql client: a statement ends with the current
-- DELIMITER at the end of a line. Do not end a comment line with the delimiter.
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

-- Append a JSON array of messages and return the topic identity and first assigned offset.
-- Each message is an object with scalar values only, so that MySQL and MariaDB read it alike:
--   message_id and payload: base64 strings, sender: string, qos: 0 to 2, retain: 0 or 1,
--   properties: JSON text as a string, published_at: Unix milliseconds,
--   expires_at: Unix milliseconds, omitted when the message does not expire.
-- The topic row lock makes other writers to this topic wait until commit.
-- A rollback releases the reserved offsets.
DELIMITER $$
DROP PROCEDURE IF EXISTS omp_append$$
CREATE PROCEDURE omp_append(IN ns VARBINARY(256), IN t VARBINARY(2048), IN messages LONGTEXT)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE v_topic_id BIGINT DEFAULT NULL;
    DECLARE v_first BIGINT DEFAULT NULL;
    DECLARE v_count INT DEFAULT JSON_LENGTH(messages);
    -- A missing topic row leaves v_topic_id NULL without a warning.
    DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN END;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    IF v_count IS NULL OR v_count < 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'invalid append batch';
    END IF;
    START TRANSACTION;
    -- Lock an existing topic row. Create the row only for a new topic.
    SELECT topic_id, next_offset INTO v_topic_id, v_first FROM mqtt_omp_topic
        WHERE namespace = ns AND topic = t FOR UPDATE;
    IF v_topic_id IS NULL THEN
        INSERT INTO mqtt_omp_topic(namespace, topic) VALUES (ns, t)
            ON DUPLICATE KEY UPDATE next_offset = next_offset;
        SELECT topic_id, next_offset INTO v_topic_id, v_first FROM mqtt_omp_topic
            WHERE namespace = ns AND topic = t FOR UPDATE;
    END IF;
    UPDATE mqtt_omp_topic SET next_offset = next_offset + v_count WHERE topic_id = v_topic_id;
    -- Insert the messages at consecutive offsets in array order.
    INSERT INTO mqtt_omp_message
        (topic_id, msg_offset, message_id, sender, qos, retain, payload, properties,
         published_at, expires_at)
    SELECT v_topic_id, v_first + j.ord - 1, FROM_BASE64(j.message_id), j.sender, j.qos,
           j.retain, FROM_BASE64(j.payload), j.properties, j.published_at, j.expires_at
    FROM JSON_TABLE(messages, '$[*]' COLUMNS (
        ord FOR ORDINALITY,
        message_id VARCHAR(64) PATH '$.message_id',
        sender LONGTEXT PATH '$.sender',
        qos INT PATH '$.qos',
        retain INT PATH '$.retain',
        payload LONGTEXT PATH '$.payload',
        properties LONGTEXT PATH '$.properties',
        published_at BIGINT PATH '$.published_at',
        expires_at BIGINT PATH '$.expires_at'
    )) AS j;
    COMMIT;
    SELECT v_topic_id AS topic_id, v_first AS first_offset;
END$$
DELIMITER ;

-- Write the version only after every table and the procedure exist.
-- DDL commits implicitly in MySQL.
INSERT IGNORE INTO mqtt_omp_schema VALUES (1);
