A production-ready Change Data Capture (CDC) pipeline that enables real-time data synchronization across microservices using:

- MySQL (source)
- Debezium MySQL Connector (binlog reader via Kafka Connect)
- Kafka (event streaming)
- Spring Boot (CDC consumer / projector)
- Downstream MySQL (materialized read model)

## Table of contents
- [Overview](#overview)
- [Why CDC](#why-cdc)
- [Architecture](#architecture)
- [How it works](#how-it-works)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation & setup](#installation--setup)
- [Run & test](#run--test)
- [Operational notes](#operational-notes)
- [Contributing](#contributing)
- [License](#license)

## Overview
When data changes in the source MySQL, Debezium captures row-level changes from the binlog and publishes them to Kafka topics. A Spring Boot microservice consumes those events, enforces idempotency, and applies changes to a downstream MySQL instance to maintain a materialized read model in near real time.

## Why CDC
Traditional ETL/polling approaches cause:
- High DB load
- Delays and inconsistent data
- Complex or brittle code

CDC provides:
- Real-time updates (sub-second)
- Zero read load on source tables (binlog only)
- Event ordering, replayability, and multi-consumer independence

## Architecture
Source MySQL → Debezium (Kafka Connect) → Kafka topics → Spring Boot Consumer → Downstream MySQL (read model)

Typical topic naming: `dbserver1.cdc.customers`, `dbserver1.cdc.orders`, ...

## How it works (step-by-step)
1. Application performs CRUD on source MySQL. Binlog records every row change.
2. Debezium reads the binlog and emits JSON envelopes:
  - `"op": "c"` = insert
  - `"op": "u"` = update
  - `"op": "d"` = delete
  Example envelope:
  ```json
  {
    "payload": {
     "before": null,
     "after": { "id": 5001, "email": "test@example.com", "name": "Test User" },
     "op": "c"
    }
  }
  ```
3. Events are written to Kafka topics per table. Kafka provides durability, retention, and consumer independence.
4. Spring Boot consumer:
  - Reads events (e.g., `@KafkaListener`)
  - Parses Debezium envelope (before/after/op)
  - Generates a stable event ID (e.g., `binlog-file:position`)
  - Checks a `processed_events` table for idempotency
  - Applies INSERT/UPDATE/DELETE to downstream DB
  - Manually acknowledges offsets to achieve application-level exactly-once semantics
5. Downstream DB stores the projected data as a materialized read model; it can be rebuilt by replaying Kafka.

## Features / Benefits
- Real-time sync between services
- Event-driven architecture
- Rebuildable read models (replayability)
- No polling or cron jobs
- Scalable and fault-tolerant

## Prerequisites
- MySQL with ROW binlog enabled
- Kafka + Zookeeper
- Kafka Connect with Debezium MySQL connector plugin
- Java / Gradle for Spring Boot consumer

## Installation & setup (simple & local)
1. Enable MySQL binlog (edit `my.cnf`):
  ```
  log_bin=mysql-bin
  binlog_format=ROW
  binlog_row_image=FULL
  server_id=1
  ```
  Restart MySQL.

2. Create Debezium replication user:
  ```sql
  CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz';
  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
  ```

3. Start Kafka & Kafka Connect:
  ```
  bin/zookeeper-server-start.sh config/zookeeper.properties
  bin/kafka-server-start.sh config/server.properties
  bin/connect-distributed.sh config/connect-distributed.properties
  ```
  Copy Debezium MySQL connector JARs into:
  `kafka/connect/plugins/debezium-mysql/`

4. Start Spring Boot consumer:
  ```
  ./gradlew bootRun
  ```
  Expected output:
  - Connector REGISTERED successfully.
  - CDC consumer started...

## Run & test the end-to-end flow
On source MySQL:
```sql
INSERT INTO customers(id, name, email)
VALUES (6001, 'CDC Demo', 'demo@example.com');

UPDATE customers SET email='updated@example.com' WHERE id=6001;

DELETE FROM customers WHERE id=6001;
```
Verify downstream:
```sql
SELECT * FROM consumerdb.customers WHERE id = 6001;
```

## Operational notes
- Ensure Debezium connector config matches server/DB/table includes and snapshot settings.
- Use a stable event ID derived from binlog file and position for idempotency.
- Maintain a `processed_events` ledger (event id + processed timestamp) to ensure exactly-once at application level.
- To rebuild read models, reset consumer offsets or replay topics from the beginning.

## Contributing
PRs and issues welcome. Follow repository contribution guidelines (code style, tests, documentation).

## License
Specify your project license (e.g., MIT, Apache-2.0) in `LICENSE`.

--- 
Debezium + Kafka + Spring Boot forms a resilient CDC architecture for real-time replication, strong consistency across microservices, and replayable audit/history.

