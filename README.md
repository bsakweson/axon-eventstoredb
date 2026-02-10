# Axon EventStoreDB Spring Boot Starter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21+-brightgreen.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Axon Framework](https://img.shields.io/badge/Axon%20Framework-4.11-brightgreen.svg)](https://developer.axoniq.io/)

A Spring Boot starter that integrates [EventStoreDB](https://www.eventstore.com/) (KurrentDB) as the event store for [Axon Framework](https://developer.axoniq.io/) applications. Drop it in, configure, and your Axon aggregates persist to EventStoreDB instead of a relational database.

## Features

- **`EventStorageEngine`** — Full implementation backed by EventStoreDB's gRPC client
- **`TokenStore`** — Tracking processor tokens stored in EventStoreDB streams
- **`TrackingToken`** — Maps EventStoreDB's `$all` stream positions to Axon's tracking model
- **Spring Boot Auto-Configuration** — Zero-code setup via `application.yml`
- **Optimistic Concurrency** — Uses EventStoreDB's `ExpectedRevision` for aggregate consistency
- **Category Projections** — Follows EventStoreDB's `{Type}-{Id}` stream naming convention for built-in `$ce-{Type}` projections
- **Snapshot Support** — Dedicated snapshot streams per aggregate
- **Jackson Serialization** — Axon metadata preserved in EventStoreDB event metadata

## Requirements

| Dependency | Version |
| ------------ | --------- |
| Java | 21+ |
| Spring Boot | 3.2+ |
| Axon Framework | 4.9+ |
| EventStoreDB | 23.10+ (or KurrentDB) |

## Quick Start

### 1. Add the dependency

**Gradle:**

```groovy
implementation 'com.bakalr.axon:axon-eventstoredb-spring-boot-starter:0.1.0'
```

**Maven:**

```xml
<dependency>
    <groupId>com.bakalr.axon</groupId>
    <artifactId>axon-eventstoredb-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

### 2. Configure

```yaml
axon:
  eventstoredb:
    enabled: true
    connection-string: "esdb://localhost:2113?tls=false"
```

### 3. Use Axon as usual

No code changes needed — your aggregates, command handlers, and event handlers work exactly the same. Events are now stored in EventStoreDB instead of a relational database.

```java
@Aggregate
public class OrderAggregate {

    @AggregateIdentifier
    private String orderId;

    @CommandHandler
    public OrderAggregate(CreateOrderCommand cmd) {
        apply(new OrderCreatedEvent(cmd.getOrderId(), cmd.getCustomerId()));
    }

    @EventSourcingHandler
    public void on(OrderCreatedEvent event) {
        this.orderId = event.getOrderId();
    }
}
```

Events are stored in stream `Order-{orderId}` in EventStoreDB.

## Configuration Reference

| Property | Default | Description |
| -------- | ------- | ----------- |
| `axon.eventstoredb.enabled` | `false` | Enable EventStoreDB storage engine |
| `axon.eventstoredb.connection-string` | — | Full EventStoreDB connection string (`esdb://...`) |
| `axon.eventstoredb.host` | `localhost` | EventStoreDB host (when connection-string is not set) |
| `axon.eventstoredb.port` | `2113` | EventStoreDB gRPC port |
| `axon.eventstoredb.tls` | `false` | Enable TLS |
| `axon.eventstoredb.tls-verify-cert` | `true` | Verify TLS certificates |
| `axon.eventstoredb.username` | `admin` | Authentication username |
| `axon.eventstoredb.password` | — | Authentication password |
| `axon.eventstoredb.batch-size` | `256` | Events per batch when reading `$all` stream |
| `axon.eventstoredb.stream-prefix` | (empty) | Prefix for aggregate event streams |
| `axon.eventstoredb.snapshot-stream-prefix` | `__snapshot` | Prefix for snapshot streams |
| `axon.eventstoredb.token-stream-prefix` | `__axon-tokens` | Prefix for token streams |
| `axon.eventstoredb.node-id` | (random UUID) | Node identifier for token claim management |

## Architecture

### Component Overview

```mermaid
graph TD
    subgraph app["Spring Boot Application"]
        style app fill:#e8f4fd,stroke:#4a90d9,rx:15,ry:15,color:#1a1a1a

        agg["Aggregate<br/><i>Command Side</i>"]
        style agg fill:#dceefb,stroke:#4a90d9,rx:10,ry:10,color:#1a1a1a

        axon["Axon Framework 4.11"]
        style axon fill:#d4e9f7,stroke:#3a7fc1,rx:10,ry:10,color:#1a1a1a

        subgraph autoconfig["AxonEventStoreDBAutoConfiguration"]
            style autoconfig fill:#fef9e7,stroke:#f0c040,rx:12,ry:12,color:#1a1a1a

            engine["EventStoreDB<br/>EventStorageEngine"]
            style engine fill:#e8f8e8,stroke:#5cb85c,rx:10,ry:10,color:#1a1a1a

            tokenstore["EventStoreDB<br/>TokenStore"]
            style tokenstore fill:#e8f8e8,stroke:#5cb85c,rx:10,ry:10,color:#1a1a1a

            serializer["EventStoreDB<br/>Serializer"]
            style serializer fill:#e8f8e8,stroke:#5cb85c,rx:10,ry:10,color:#1a1a1a
        end

        client["EventStoreDB Client<br/><i>db-client-java 5.x</i>"]
        style client fill:#f5eef8,stroke:#8e44ad,rx:10,ry:10,color:#1a1a1a
    end

    esdb[("EventStoreDB 24.x<br/><i>KurrentDB</i>")]
    style esdb fill:#fdecea,stroke:#e74c3c,rx:10,ry:10,color:#1a1a1a

    agg --> axon
    axon --> engine
    axon --> tokenstore
    engine --> client
    tokenstore --> client
    serializer -.-> engine
    client -- "gRPC" --> esdb
```

### Stream Layout

```mermaid
graph LR
    subgraph esdb["EventStoreDB Streams"]
        style esdb fill:#f0f7ff,stroke:#4a90d9,rx:15,ry:15,color:#1a1a1a

        subgraph agg_streams["Aggregate Event Streams"]
            style agg_streams fill:#e8f8e8,stroke:#5cb85c,rx:12,ry:12,color:#1a1a1a
            s1["Order-abc123"]
            s2["Product-xyz789"]
        end

        subgraph snap_streams["Snapshot Streams"]
            style snap_streams fill:#fef9e7,stroke:#f0c040,rx:12,ry:12,color:#1a1a1a
            s3["__snapshot-Order-abc123"]
        end

        subgraph token_streams["Token Streams"]
            style token_streams fill:#f5eef8,stroke:#8e44ad,rx:12,ry:12,color:#1a1a1a
            s4["__axon-tokens-OrderProj-0"]
        end

        subgraph system_streams["System Streams (automatic)"]
            style system_streams fill:#fdecea,stroke:#e74c3c,rx:12,ry:12,color:#1a1a1a
            s5["$all"]
            s6["$ce-Order"]
        end
    end
```

### Event Metadata Schema

Each Axon event is stored in EventStoreDB with the following metadata:

```json
{
  "axon-message-id": "uuid",
  "axon-message-type": "com.example.OrderCreatedEvent",
  "axon-message-revision": "1.0",
  "axon-message-timestamp": "2026-02-09T10:30:00Z",
  "axon-message-aggregate-type": "Order",
  "axon-message-aggregate-seq": 0,
  "axon-metadata": { }
}
```

### Key Method Mappings

| Axon Method | EventStoreDB Operation |
| ------------- | ---------------------- |
| `appendEvents(events)` | `client.appendToStream(streamName, options, eventData...)` |
| `readEvents(aggregateId, firstSeqNum)` | `client.readStream(streamName, forwards().fromRevision(n))` |
| `readEvents(trackingToken, mayBlock)` | `client.readAll(forwards().fromPosition(pos))` |
| `storeSnapshot(snapshot)` | `client.appendToStream("__snapshot-{type}-{id}", data)` |
| `readSnapshot(aggregateId)` | `client.readStream("__snapshot-...", backwards().maxCount(1))` |
| `createTailToken()` | `Position.START` wrapped in `EventStoreDBTrackingToken` |
| `createHeadToken()` | Read last event from `$all`, extract position |
| `createTokenAt(instant)` | Binary search through `$all` for position at timestamp |

### Optimistic Concurrency

- **First append:** `AppendToStreamOptions.expectedRevision(StreamState.noStream())`
- **Subsequent appends:** `AppendToStreamOptions.expectedRevision(lastKnownRevision)`
- EventStoreDB's `WrongExpectedVersionException` is mapped to Axon's `EventStoreException`

## Migrating from JPA Event Store

If you have an existing Axon application using JPA (`axon-spring-boot-starter` with a relational DB), migration can be done in stages:

```mermaid
graph LR
    subgraph migration["Migration Path"]
        style migration fill:#f0f7ff,stroke:#4a90d9,rx:15,ry:15,color:#1a1a1a

        step1["1. Dual-Write<br/><i>Write to both stores,<br/>read from JPA</i>"]
        style step1 fill:#fef9e7,stroke:#f0c040,rx:10,ry:10,color:#1a1a1a

        step2["2. Replay Events<br/><i>Export JPA events<br/>into EventStoreDB</i>"]
        style step2 fill:#e8f8e8,stroke:#5cb85c,rx:10,ry:10,color:#1a1a1a

        step3["3. Switch Read Path<br/><i>Set enabled: true,<br/>JPA becomes backup</i>"]
        style step3 fill:#dceefb,stroke:#4a90d9,rx:10,ry:10,color:#1a1a1a

        step4["4. Remove JPA<br/><i>Drop tables,<br/>remove dependency</i>"]
        style step4 fill:#f5eef8,stroke:#8e44ad,rx:10,ry:10,color:#1a1a1a
    end

    step1 --> step2 --> step3 --> step4
```

**Step 2 — Replay example:**

```bash
# Export from PostgreSQL event store
psql -c "COPY (SELECT * FROM domain_event_entry ORDER BY global_index) TO STDOUT WITH CSV"

# Import into EventStoreDB using a replay script
```

## Deploying EventStoreDB

```yaml
# docker-compose.yaml
eventstore:
  image: eventstore/eventstore:24.10
  ports:
    - "2113:2113"
  environment:
    EVENTSTORE_INSECURE: "true"
    EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP: "true"
    EVENTSTORE_MEM_DB: "false"
```

## Project Structure

```mermaid
graph TD
    subgraph project["axon-eventstoredb"]
        style project fill:#f0f7ff,stroke:#4a90d9,rx:15,ry:15,color:#1a1a1a

        subgraph core["Core Components"]
            style core fill:#e8f8e8,stroke:#5cb85c,rx:12,ry:12,color:#1a1a1a
            engine2["EventStoreDBEventStorageEngine"]
            token2["EventStoreDBTokenStore"]
            tracking["EventStoreDBTrackingToken"]
        end

        subgraph config["Auto-Configuration"]
            style config fill:#fef9e7,stroke:#f0c040,rx:12,ry:12,color:#1a1a1a
            autoconf["AxonEventStoreDBAutoConfiguration"]
            props["EventStoreDBProperties"]
        end

        subgraph util["Utilities"]
            style util fill:#f5eef8,stroke:#8e44ad,rx:12,ry:12,color:#1a1a1a
            ser["EventStoreDBSerializer"]
            naming["EventStoreDBStreamNaming"]
        end
    end

    autoconf --> engine2
    autoconf --> token2
    engine2 --> tracking
    engine2 --> ser
    engine2 --> naming
```

## Dependencies

| Dependency | Purpose |
| ---------- | ------- |
| `axon-eventsourcing:4.11.2` | `EventStorageEngine`, `DomainEventData`, `TrackingToken` |
| `axon-messaging:4.11.2` | `EventMessage`, `MetaData`, `Serializer` |
| `axon-configuration:4.11.2` | `EventProcessingConfigurer` |
| `db-client-java:5.4.5` | EventStoreDB gRPC client |
| `spring-boot-autoconfigure` | `@AutoConfiguration`, `@ConditionalOnClass` |
| `jackson-databind` | JSON serialization for event payloads |

## Development

### Prerequisites

- Java 21+
- Docker (for integration tests with EventStoreDB)

### Build

```bash
./gradlew build
```

### Run tests

```bash
./gradlew test
```

### Run integration tests (requires Docker)

```bash
./gradlew integrationTest
```

### Testing Strategy

| Level | Tool | Scope |
| ------- | ------ | ------ |
| Unit | JUnit 5 + Mockito | Token math, stream naming, serialization |
| Integration | Testcontainers + EventStoreDB Docker | Full append/read/track lifecycle |
| Service-level | Axon Test Fixtures | Aggregate behavior with EventStoreDB backend |

EventStoreDB Docker image: `eventstore/eventstore:24.10` (or `kurrentio/kurrentdb:latest`)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Roadmap

```mermaid
graph LR
    subgraph roadmap["Roadmap"]
        style roadmap fill:#f0f7ff,stroke:#4a90d9,rx:15,ry:15,color:#1a1a1a

        subgraph phase1["Phase 1 — Core (Done)"]
            style phase1 fill:#e8f8e8,stroke:#5cb85c,rx:12,ry:12,color:#1a1a1a
            p1a["EventStorageEngine"]
            p1b["TokenStore"]
            p1c["TrackingToken"]
            p1d["Auto-Configuration"]
        end

        subgraph phase2["Phase 2 — Hardening"]
            style phase2 fill:#fef9e7,stroke:#f0c040,rx:12,ry:12,color:#1a1a1a
            p2a["Event Upcasting"]
            p2b["Connection Resilience"]
            p2c["Micrometer Metrics"]
        end

        subgraph phase3["Phase 3 — Advanced"]
            style phase3 fill:#f5eef8,stroke:#8e44ad,rx:12,ry:12,color:#1a1a1a
            p3a["Persistent Subscriptions"]
            p3b["Distributed Token Claims"]
            p3c["Maven Central Publish"]
        end
    end

    phase1 --> phase2 --> phase3
```

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgments

- [Axon Framework](https://developer.axoniq.io/) by AxonIQ
- [EventStoreDB](https://www.eventstore.com/) (now [KurrentDB](https://kurrent.io/))
- Inspired by the need for a modern, maintained Axon ↔ EventStoreDB adapter
