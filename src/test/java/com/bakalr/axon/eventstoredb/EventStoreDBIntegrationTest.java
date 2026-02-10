package com.bakalr.axon.eventstoredb;

import com.bakalr.axon.eventstoredb.util.EventStoreDBSerializer;
import com.bakalr.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests using Testcontainers with a real EventStoreDB instance.
 *
 * <p>These tests verify the full lifecycle: append events → read events →
 * store snapshot → read snapshot → token management.
 */
@Testcontainers
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventStoreDBIntegrationTest {

    private static final String AGGREGATE_TYPE = "Order";
    private static final String AGGREGATE_ID = UUID.randomUUID().toString();
    private static final int ESDB_HTTP_PORT = 2113;

    @Container
    private static final GenericContainer<?> EVENT_STORE_DB =
            new GenericContainer<>("eventstore/eventstore:lts")
                    .withExposedPorts(ESDB_HTTP_PORT)
                    .withEnv("EVENTSTORE_INSECURE", "true")
                    .withEnv("EVENTSTORE_MEM_DB", "true")
                    .withEnv("EVENTSTORE_CLUSTER_SIZE", "1")
                    .withEnv("EVENTSTORE_RUN_PROJECTIONS", "All")
                    .withEnv("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "true")
                    .waitingFor(Wait.forHttp("/health/live")
                            .forPort(ESDB_HTTP_PORT)
                            .forStatusCode(204)
                            .withStartupTimeout(Duration.ofSeconds(60)));

    private EventStoreDBClient client;
    private EventStoreDBEventStorageEngine engine;
    private EventStoreDBTokenStore tokenStore;
    private Serializer axonSerializer;
    private EventStoreDBStreamNaming naming;

    @BeforeAll
    void setUp() {
        String host = EVENT_STORE_DB.getHost();
        Integer port = EVENT_STORE_DB.getMappedPort(ESDB_HTTP_PORT);
        String connectionString = "esdb://" + host + ":" + port + "?tls=false";

        EventStoreDBClientSettings settings =
                EventStoreDBConnectionString.parseOrThrow(connectionString);
        client = EventStoreDBClient.create(settings);

        axonSerializer = JacksonSerializer.defaultSerializer();
        naming = new EventStoreDBStreamNaming();
        engine = new EventStoreDBEventStorageEngine(client, axonSerializer, naming, 256);
        tokenStore = new EventStoreDBTokenStore(
                client, naming, "integration-test-node");
    }

    @AfterAll
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
    }

    // ── Payload types for testing ────────────────────────────────────────

    public record OrderCreated(String orderId, String customer) {
    }

    public record ItemAdded(String orderId, String item, int quantity) {
    }

    public record OrderSnapshot(String orderId, String customer, List<String> items) {
    }

    // ── Event Storage Engine tests ───────────────────────────────────────

    @Test
    @Order(1)
    void shouldAppendFirstEvent() {
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 0,
                new OrderCreated(AGGREGATE_ID, "Alice"));

        assertThatCode(() -> engine.appendEvents(List.of(event)))
                .doesNotThrowAnyException();
    }

    @Test
    @Order(2)
    void shouldAppendSubsequentEvent() {
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 1,
                new ItemAdded(AGGREGATE_ID, "Widget", 3));

        assertThatCode(() -> engine.appendEvents(List.of(event)))
                .doesNotThrowAnyException();
    }

    @Test
    @Order(3)
    void shouldReadEventsForAggregate() {
        DomainEventStream stream = engine.readEvents(AGGREGATE_ID, 0);
        assertThat(stream.hasNext()).isTrue();

        DomainEventMessage<?> first = stream.next();
        assertThat(first.getAggregateIdentifier()).isEqualTo(AGGREGATE_ID);
        assertThat(first.getSequenceNumber()).isZero();
        assertThat(first.getType()).isEqualTo(AGGREGATE_TYPE);
        assertThat(first.getPayloadType()).isEqualTo(OrderCreated.class);

        assertThat(stream.hasNext()).isTrue();
        DomainEventMessage<?> second = stream.next();
        assertThat(second.getSequenceNumber()).isEqualTo(1L);
        assertThat(second.getPayloadType()).isEqualTo(ItemAdded.class);

        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    @Order(4)
    void shouldReadEventsFromOffset() {
        DomainEventStream stream = engine.readEvents(AGGREGATE_ID, 1);
        assertThat(stream.hasNext()).isTrue();

        DomainEventMessage<?> event = stream.next();
        assertThat(event.getSequenceNumber()).isEqualTo(1L);
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    @Order(5)
    void shouldReturnEmptyForNonexistentAggregate() {
        DomainEventStream stream = engine.readEvents("nonexistent-" + UUID.randomUUID(), 0);
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    @Order(6)
    void shouldDetectConcurrencyConflict() {
        // Try to append an event with sequence 0 again (stream already exists)
        DomainEventMessage<?> conflicting = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 0,
                new OrderCreated(AGGREGATE_ID, "Bob"));

        assertThatThrownBy(() -> engine.appendEvents(List.of(conflicting)))
                .hasMessageContaining("Concurrent modification");
    }

    @Test
    @Order(7)
    void shouldReportLastSequenceNumber() {
        Optional<Long> seq = engine.lastSequenceNumberFor(AGGREGATE_ID);
        assertThat(seq).isPresent().hasValue(1L);
    }

    @Test
    @Order(8)
    void shouldReturnEmptySequenceForUnknownAggregate() {
        Optional<Long> seq = engine.lastSequenceNumberFor("unknown-" + UUID.randomUUID());
        assertThat(seq).isEmpty();
    }

    // ── Snapshot tests ───────────────────────────────────────────────────

    @Test
    @Order(10)
    void shouldStoreAndReadSnapshot() {
        DomainEventMessage<?> snapshot = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 1,
                new OrderSnapshot(AGGREGATE_ID, "Alice", List.of("Widget")));

        engine.storeSnapshot(snapshot);

        Optional<DomainEventMessage<?>> loaded = engine.readSnapshot(AGGREGATE_ID);
        assertThat(loaded).isPresent();
        assertThat(loaded.get().getSequenceNumber()).isEqualTo(1L);
        assertThat(loaded.get().getPayloadType()).isEqualTo(OrderSnapshot.class);
    }

    @Test
    @Order(11)
    void shouldReturnEmptySnapshotForUnknownAggregate() {
        Optional<DomainEventMessage<?>> snapshot =
                engine.readSnapshot("no-snap-" + UUID.randomUUID());
        assertThat(snapshot).isEmpty();
    }

    // ── Tracking (global stream) tests ───────────────────────────────────

    @Test
    @Order(20)
    void shouldCreateTailToken() {
        TrackingToken token = engine.createTailToken();
        assertThat(token).isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    @Order(21)
    void shouldCreateHeadToken() {
        TrackingToken token = engine.createHeadToken();
        assertThat(token).isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    @Order(22)
    void shouldReadTrackedEventsFromTail() {
        TrackingToken tail = engine.createTailToken();
        Stream<? extends TrackedEventMessage<?>> tracked = engine.readEvents(tail, false);

        List<? extends TrackedEventMessage<?>> events = tracked.toList();
        // We appended 2 domain events + 1 snapshot = at least 2 user events visible
        // (snapshots go to a separate stream, so they may or may not show in $all)
        assertThat(events).isNotEmpty();
        assertThat(events.getFirst().trackingToken())
                .isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    @Order(23)
    void shouldCreateTokenAtSpecificPosition() {
        TrackingToken token = engine.createTokenAt(Instant.now().minusSeconds(3600));
        assertThat(token).isNotNull();
    }

    // ── Token Store tests ────────────────────────────────────────────────

    @Test
    @Order(30)
    void shouldInitializeTokenSegments() {
        tokenStore.initializeTokenSegments("test-processor", 2);
        int[] segments = tokenStore.fetchSegments("test-processor");
        assertThat(segments).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    @Order(31)
    void shouldStoreAndFetchToken() {
        TrackingToken token = engine.createTailToken();
        tokenStore.storeToken(token, "test-processor", 0);

        TrackingToken fetched = tokenStore.fetchToken("test-processor", 0);
        assertThat(fetched).isNotNull();
        assertThat(fetched).isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    @Order(32)
    void shouldUpdateExistingToken() {
        TrackingToken head = engine.createHeadToken();
        tokenStore.storeToken(head, "test-processor", 0);

        TrackingToken fetched = tokenStore.fetchToken("test-processor", 0);
        assertThat(fetched).isEqualTo(head);
    }

    @Test
    @Order(33)
    void shouldExtendAndReleaseClaim() {
        tokenStore.extendClaim("test-processor", 0);
        assertThatCode(() -> tokenStore.releaseClaim("test-processor", 0))
                .doesNotThrowAnyException();
    }

    @Test
    @Order(34)
    void shouldDeleteToken() {
        tokenStore.deleteToken("test-processor", 1);
        TrackingToken fetched = tokenStore.fetchToken("test-processor", 1);
        assertThat(fetched).isNull();
    }

    @Test
    @Order(35)
    void shouldReturnNullForNonexistentToken() {
        TrackingToken token = tokenStore.fetchToken("nonexistent-processor", 99);
        assertThat(token).isNull();
    }

    @Test
    @Order(36)
    void shouldReturnStorageIdentifier() {
        Optional<String> id = tokenStore.retrieveStorageIdentifier();
        assertThat(id).isPresent().hasValue("integration-test-node");
    }

    @Test
    @Order(37)
    void shouldRequireExplicitSegmentInitialization() {
        assertThat(tokenStore.requiresExplicitSegmentInitialization()).isTrue();
    }

    // ── End-to-end round-trip ────────────────────────────────────────────

    @Test
    @Order(50)
    void shouldPerformFullRoundTrip() {
        String aggId = UUID.randomUUID().toString();

        // 1. Append events
        DomainEventMessage<?> created = new GenericDomainEventMessage<>(
                "Product", aggId, 0, new OrderCreated(aggId, "Bob"));
        DomainEventMessage<?> modified = new GenericDomainEventMessage<>(
                "Product", aggId, 1, new ItemAdded(aggId, "Gadget", 5));
        engine.appendEvents(List.of(created, modified));

        // 2. Read back
        DomainEventStream events = engine.readEvents(aggId, 0);
        List<? extends DomainEventMessage<?>> eventList = events.asStream().toList();
        assertThat(eventList).hasSize(2);

        // 3. Verify serialization round-trip preserves payload
        OrderCreated payload = (OrderCreated) eventList.get(0).getPayload();
        assertThat(payload.orderId()).isEqualTo(aggId);
        assertThat(payload.customer()).isEqualTo("Bob");

        ItemAdded itemPayload = (ItemAdded) eventList.get(1).getPayload();
        assertThat(itemPayload.item()).isEqualTo("Gadget");
        assertThat(itemPayload.quantity()).isEqualTo(5);

        // 4. Store and read snapshot
        DomainEventMessage<?> snap = new GenericDomainEventMessage<>(
                "Product", aggId, 1,
                new OrderSnapshot(aggId, "Bob", List.of("Gadget")));
        engine.storeSnapshot(snap);

        Optional<DomainEventMessage<?>> loadedSnap = engine.readSnapshot(aggId);
        assertThat(loadedSnap).isPresent();
        OrderSnapshot snapPayload = (OrderSnapshot) loadedSnap.get().getPayload();
        assertThat(snapPayload.customer()).isEqualTo("Bob");
        assertThat(snapPayload.items()).containsExactly("Gadget");

        // 5. Last sequence number
        Optional<Long> seq = engine.lastSequenceNumberFor(aggId);
        assertThat(seq).isPresent().hasValue(1L);

        // 6. Track from global stream
        TrackingToken tail = engine.createTailToken();
        Stream<? extends TrackedEventMessage<?>> tracked = engine.readEvents(tail, false);
        long productEvents = tracked
                .filter(e -> e instanceof DomainEventMessage<?> de
                        && aggId.equals(de.getAggregateIdentifier()))
                .count();
        assertThat(productEvents).isEqualTo(2);
    }
}
