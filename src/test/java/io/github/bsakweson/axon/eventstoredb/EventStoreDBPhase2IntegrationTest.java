package io.github.bsakweson.axon.eventstoredb;

import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.resilience.RetryPolicy;
import io.github.bsakweson.axon.eventstoredb.upcasting.EventStoreDBDomainEventData;
import io.github.bsakweson.axon.eventstoredb.upcasting.EventStoreDBTrackedDomainEventData;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import com.eventstore.dbclient.ReadAllOptions;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.ResolvedEvent;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
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
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
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
 * Integration tests for Phase 2 features (metrics, resilience, upcasting) using
 * Testcontainers with a real EventStoreDB instance.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Micrometer metrics are correctly recorded during real EventStoreDB operations</li>
 *   <li>Retry/resilience executor works end-to-end with a live database</li>
 *   <li>Event upcasting pipeline works with events written to and read from EventStoreDB</li>
 *   <li>All three features integrate correctly when used together</li>
 * </ul>
 */
@Testcontainers
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventStoreDBPhase2IntegrationTest {

    private static final String AGGREGATE_TYPE = "Invoice";
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
    private EventStoreDBEventStorageEngine engineWithAllFeatures;
    private EventStoreDBEventStorageEngine engineWithRetryOnly;
    private EventStoreDBEventStorageEngine engineWithUpcasting;
    private EventStoreDBTokenStore tokenStore;
    private Serializer axonSerializer;
    private EventStoreDBStreamNaming naming;
    private MeterRegistry meterRegistry;
    private EventStoreDBMetrics metrics;

    // ── Payload types ────────────────────────────────────────────────────

    public record InvoiceCreated(String invoiceId, String customer, double amount) {
    }

    public record LineItemAdded(String invoiceId, String product, int quantity, double unitPrice) {
    }

    // V1 → V2 upcasting: InvoiceCreatedV1 has no "currency" field
    public record InvoiceCreatedV1(String invoiceId, String customer, double amount) {
    }

    /**
     * Simple pass-through upcaster that logs its invocation to verify the pipeline runs.
     * In a real scenario this would modify the serialized data (e.g. add a field).
     */
    private static class NoOpLoggingUpcaster implements EventUpcaster {
        private int invocationCount;

        @Override
        public Stream<IntermediateEventRepresentation> upcast(
                Stream<IntermediateEventRepresentation> intermediateRepresentations) {
            return intermediateRepresentations.peek(rep -> invocationCount++);
        }

        int getInvocationCount() {
            return invocationCount;
        }
    }

    private NoOpLoggingUpcaster upcaster;

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
        meterRegistry = new SimpleMeterRegistry();
        metrics = new EventStoreDBMetrics(meterRegistry);
        upcaster = new NoOpLoggingUpcaster();

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(3)
                .initialBackoffMs(50)
                .maxBackoffMs(1000)
                .multiplier(2.0)
                .build();
        EventStoreDBRetryExecutor retryExecutor = new EventStoreDBRetryExecutor(retryPolicy);

        // Engine with ALL Phase 2 features
        engineWithAllFeatures = new EventStoreDBEventStorageEngine(
                client, axonSerializer, naming, 256,
                upcaster, retryExecutor, metrics);

        // Engine with retry only (no upcasting, no metrics)
        engineWithRetryOnly = new EventStoreDBEventStorageEngine(
                client, axonSerializer, naming, 256,
                null, retryExecutor, null);

        // Engine with upcasting only (no retry, no metrics)
        engineWithUpcasting = new EventStoreDBEventStorageEngine(
                client, axonSerializer, naming, 256,
                upcaster, null, null);

        tokenStore = new EventStoreDBTokenStore(
                client, naming, "phase2-integration-node");
    }

    @AfterAll
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // METRICS INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(1)
    void metricsShouldRecordEventsAppended() {
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 0,
                new InvoiceCreated(AGGREGATE_ID, "Alice", 100.0));

        engineWithAllFeatures.appendEvents(List.of(event));

        double appended = meterRegistry.get("axon.eventstoredb.events.appended").counter().count();
        assertThat(appended).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    @Order(2)
    void metricsShouldRecordMultipleEventsAppended() {
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 1,
                new LineItemAdded(AGGREGATE_ID, "Widget", 5, 19.99));

        double before = meterRegistry.get("axon.eventstoredb.events.appended").counter().count();
        engineWithAllFeatures.appendEvents(List.of(event));
        double after = meterRegistry.get("axon.eventstoredb.events.appended").counter().count();

        assertThat(after).isGreaterThan(before);
    }

    @Test
    @Order(3)
    void metricsShouldRecordEventsRead() {
        DomainEventStream stream = engineWithAllFeatures.readEvents(AGGREGATE_ID, 0);
        // consume the stream
        while (stream.hasNext()) {
            stream.next();
        }

        double read = meterRegistry.get("axon.eventstoredb.events.read").counter().count();
        assertThat(read).isGreaterThanOrEqualTo(2.0);
    }

    @Test
    @Order(4)
    void metricsShouldRecordSnapshotStored() {
        DomainEventMessage<?> snapshot = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 1,
                new InvoiceCreated(AGGREGATE_ID, "Alice", 100.0));

        engineWithAllFeatures.storeSnapshot(snapshot);

        double stored = meterRegistry.get("axon.eventstoredb.snapshots.stored").counter().count();
        assertThat(stored).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    @Order(5)
    void metricsShouldRecordSnapshotRead() {
        Optional<DomainEventMessage<?>> snapshot =
                engineWithAllFeatures.readSnapshot(AGGREGATE_ID);
        assertThat(snapshot).isPresent();

        double read = meterRegistry.get("axon.eventstoredb.snapshots.read").counter().count();
        assertThat(read).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    @Order(6)
    void metricsShouldRecordTrackedEventsRead() {
        TrackingToken tail = engineWithAllFeatures.createTailToken();
        Stream<? extends TrackedEventMessage<?>> tracked =
                engineWithAllFeatures.readEvents(tail, false);
        long count = tracked.count();

        double read = meterRegistry.get("axon.eventstoredb.events.read").counter().count();
        assertThat(read).isGreaterThanOrEqualTo((double) count);
    }

    @Test
    @Order(7)
    void metricsShouldRecordErrorOnConcurrencyConflict() {
        // Try to append with wrong sequence → should record error metric
        DomainEventMessage<?> conflicting = new GenericDomainEventMessage<>(
                AGGREGATE_TYPE, AGGREGATE_ID, 0,
                new InvoiceCreated(AGGREGATE_ID, "Bob", 50.0));

        assertThatThrownBy(() -> engineWithAllFeatures.appendEvents(List.of(conflicting)))
                .hasMessageContaining("Concurrent modification");

        double errors = meterRegistry.get("axon.eventstoredb.errors")
                .tag("operation", "append")
                .counter().count();
        assertThat(errors).isGreaterThanOrEqualTo(1.0);
    }

    // ════════════════════════════════════════════════════════════════════════
    // RESILIENCE / RETRY INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(10)
    void retryExecutorShouldSucceedOnFirstAttemptForValidOps() {
        String aggId = UUID.randomUUID().toString();

        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "RetryTest", aggId, 0,
                new InvoiceCreated(aggId, "RetryCustomer", 200.0));

        // Should succeed immediately — no retry needed
        assertThatCode(() -> engineWithRetryOnly.appendEvents(List.of(event)))
                .doesNotThrowAnyException();

        DomainEventStream stream = engineWithRetryOnly.readEvents(aggId, 0);
        assertThat(stream.hasNext()).isTrue();
        assertThat(stream.next().getPayloadType()).isEqualTo(InvoiceCreated.class);
    }

    @Test
    @Order(11)
    void retryExecutorShouldNotRetryConcurrencyErrors() {
        // Concurrency errors (WrongExpectedVersionException) are NOT retryable.
        // The engine should fail immediately without retrying.
        String aggId = UUID.randomUUID().toString();

        DomainEventMessage<?> first = new GenericDomainEventMessage<>(
                "RetryTest2", aggId, 0,
                new InvoiceCreated(aggId, "First", 10.0));
        engineWithRetryOnly.appendEvents(List.of(first));

        DomainEventMessage<?> duplicate = new GenericDomainEventMessage<>(
                "RetryTest2", aggId, 0,
                new InvoiceCreated(aggId, "Duplicate", 20.0));

        assertThatThrownBy(() -> engineWithRetryOnly.appendEvents(List.of(duplicate)))
                .hasMessageContaining("Concurrent modification");
    }

    @Test
    @Order(12)
    void retryExecutorShouldWorkWithTokenStore() {
        tokenStore.initializeTokenSegments("retry-test-processor", 1);
        int[] segments = tokenStore.fetchSegments("retry-test-processor");
        assertThat(segments).containsExactly(0);

        TrackingToken head = engineWithRetryOnly.createHeadToken();
        tokenStore.storeToken(head, "retry-test-processor", 0);

        TrackingToken fetched = tokenStore.fetchToken("retry-test-processor", 0);
        assertThat(fetched).isNotNull();
        assertThat(fetched).isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    @Order(13)
    void retryExecutorShouldHandleSnapshotsCorrectly() {
        String aggId = UUID.randomUUID().toString();

        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "RetrySnap", aggId, 0,
                new InvoiceCreated(aggId, "SnapCustomer", 300.0));
        engineWithRetryOnly.appendEvents(List.of(event));

        DomainEventMessage<?> snapshot = new GenericDomainEventMessage<>(
                "RetrySnap", aggId, 0,
                new InvoiceCreated(aggId, "SnapCustomer", 300.0));

        assertThatCode(() -> engineWithRetryOnly.storeSnapshot(snapshot))
                .doesNotThrowAnyException();

        Optional<DomainEventMessage<?>> loaded =
                engineWithRetryOnly.readSnapshot(aggId);
        assertThat(loaded).isPresent();
    }

    // ════════════════════════════════════════════════════════════════════════
    // UPCASTING INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(20)
    void upcasterShouldBeInvokedWhenReadingDomainEvents() {
        String aggId = UUID.randomUUID().toString();

        DomainEventMessage<?> event1 = new GenericDomainEventMessage<>(
                "UpcastAggregate", aggId, 0,
                new InvoiceCreatedV1(aggId, "UpcastCustomer", 500.0));
        DomainEventMessage<?> event2 = new GenericDomainEventMessage<>(
                "UpcastAggregate", aggId, 1,
                new LineItemAdded(aggId, "Gizmo", 2, 49.99));

        // Write with a plain engine (no upcasting on write)
        engineWithRetryOnly.appendEvents(List.of(event1, event2));

        // Read with upcasting engine
        int beforeCount = upcaster.getInvocationCount();
        DomainEventStream stream = engineWithUpcasting.readEvents(aggId, 0);
        List<? extends DomainEventMessage<?>> events = stream.asStream().toList();

        assertThat(events).hasSize(2);
        assertThat(upcaster.getInvocationCount()).isGreaterThan(beforeCount);
    }

    @Test
    @Order(21)
    void upcasterShouldBeInvokedWhenReadingTrackedEvents() {
        int beforeCount = upcaster.getInvocationCount();
        TrackingToken tail = engineWithUpcasting.createTailToken();
        Stream<? extends TrackedEventMessage<?>> tracked =
                engineWithUpcasting.readEvents(tail, false);
        List<? extends TrackedEventMessage<?>> events = tracked.toList();

        assertThat(events).isNotEmpty();
        assertThat(upcaster.getInvocationCount()).isGreaterThan(beforeCount);

        // Each tracked event MUST have an EventStoreDBTrackingToken
        for (TrackedEventMessage<?> event : events) {
            assertThat(event.trackingToken())
                    .as("Tracking token must not be null for tracked events")
                    .isNotNull()
                    .isInstanceOf(EventStoreDBTrackingToken.class);
        }
    }

    @Test
    @Order(22)
    void upcastedDomainEventsShouldPreservePayloadAndMetadata() {
        String aggId = UUID.randomUUID().toString();
        InvoiceCreated payload = new InvoiceCreated(aggId, "PayloadTest", 999.99);

        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "PayloadAggregate", aggId, 0, payload);
        engineWithRetryOnly.appendEvents(List.of(event));

        DomainEventStream stream = engineWithUpcasting.readEvents(aggId, 0);
        assertThat(stream.hasNext()).isTrue();

        DomainEventMessage<?> read = stream.next();
        assertThat(read.getAggregateIdentifier()).isEqualTo(aggId);
        assertThat(read.getType()).isEqualTo("PayloadAggregate");
        assertThat(read.getSequenceNumber()).isZero();
        assertThat(read.getPayloadType()).isEqualTo(InvoiceCreated.class);

        InvoiceCreated readPayload = (InvoiceCreated) read.getPayload();
        assertThat(readPayload.invoiceId()).isEqualTo(aggId);
        assertThat(readPayload.customer()).isEqualTo("PayloadTest");
        assertThat(readPayload.amount()).isEqualTo(999.99);
    }

    // ════════════════════════════════════════════════════════════════════════
    // DOMAIN EVENT DATA ADAPTER (UPCASTING FOUNDATION) INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(30)
    void eventStoreDBDomainEventDataShouldWrapRealEvents() throws Exception {
        String aggId = UUID.randomUUID().toString();

        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "DataAdapterAgg", aggId, 0,
                new InvoiceCreated(aggId, "WrapTest", 42.0));
        engineWithRetryOnly.appendEvents(List.of(event));

        // Read raw from EventStoreDB
        String streamName = naming.aggregateStream("DataAdapterAgg", aggId);
        ReadResult result = client.readStream(streamName,
                ReadStreamOptions.get().forwards().fromStart().maxCount(10)).get();

        List<ResolvedEvent> resolvedEvents = result.getEvents();
        assertThat(resolvedEvents).isNotEmpty();

        EventStoreDBDomainEventData data =
                new EventStoreDBDomainEventData(resolvedEvents.getFirst());
        assertThat(data.getAggregateIdentifier()).isEqualTo(aggId);
        assertThat(data.getType()).isEqualTo("DataAdapterAgg");
        assertThat(data.getSequenceNumber()).isZero();
        assertThat(data.getEventIdentifier()).isNotBlank();
        assertThat(data.getTimestamp()).isNotNull();
        assertThat(data.getPayload()).isNotNull();
        assertThat(data.getMetaData()).isNotNull();
    }

    @Test
    @Order(31)
    void eventStoreDBTrackedDomainEventDataShouldProvideTrackingToken() throws Exception {
        // Read from $all to get events with positions
        ReadResult allResult = client.readAll(
                ReadAllOptions.get().forwards().fromStart().maxCount(100)).get();

        List<ResolvedEvent> userEvents = allResult.getEvents().stream()
                .filter(e -> !e.getOriginalEvent().getStreamId().startsWith("$"))
                .filter(e -> !naming.isSystemStream(e.getOriginalEvent().getStreamId()))
                .toList();

        assertThat(userEvents).isNotEmpty();

        EventStoreDBTrackedDomainEventData trackedData =
                new EventStoreDBTrackedDomainEventData(userEvents.getFirst());

        assertThat(trackedData.trackingToken()).isNotNull();
        assertThat(trackedData.trackingToken())
                .isInstanceOf(EventStoreDBTrackingToken.class);
        assertThat(trackedData.getEventIdentifier()).isNotBlank();
        assertThat(trackedData.getAggregateIdentifier()).isNotBlank();
    }

    // ════════════════════════════════════════════════════════════════════════
    // ALL FEATURES COMBINED — END-TO-END
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(50)
    void shouldPerformFullRoundTripWithAllPhase2Features() {
        String aggId = UUID.randomUUID().toString();

        // 1. Append events (with metrics + retry)
        double appendBefore =
                meterRegistry.get("axon.eventstoredb.events.appended").counter().count();
        DomainEventMessage<?> created = new GenericDomainEventMessage<>(
                "FullTest", aggId, 0,
                new InvoiceCreated(aggId, "FullTestCustomer", 750.0));
        DomainEventMessage<?> lineItem = new GenericDomainEventMessage<>(
                "FullTest", aggId, 1,
                new LineItemAdded(aggId, "Premium Widget", 10, 75.0));
        engineWithAllFeatures.appendEvents(List.of(created));
        engineWithAllFeatures.appendEvents(List.of(lineItem));
        double appendAfter =
                meterRegistry.get("axon.eventstoredb.events.appended").counter().count();
        assertThat(appendAfter).isGreaterThanOrEqualTo(appendBefore + 2);

        // 2. Read domain events (with upcasting + metrics)
        double readBefore =
                meterRegistry.get("axon.eventstoredb.events.read").counter().count();
        int upcastBefore = upcaster.getInvocationCount();
        DomainEventStream stream = engineWithAllFeatures.readEvents(aggId, 0);
        List<? extends DomainEventMessage<?>> events =
                stream.asStream().toList();
        assertThat(events).hasSize(2);
        double readAfter =
                meterRegistry.get("axon.eventstoredb.events.read").counter().count();
        assertThat(readAfter).isGreaterThan(readBefore);
        assertThat(upcaster.getInvocationCount()).isGreaterThan(upcastBefore);

        // 3. Verify payload deserialization through upcaster
        InvoiceCreated createdPayload = (InvoiceCreated) events.get(0).getPayload();
        assertThat(createdPayload.customer()).isEqualTo("FullTestCustomer");
        assertThat(createdPayload.amount()).isEqualTo(750.0);
        LineItemAdded linePayload = (LineItemAdded) events.get(1).getPayload();
        assertThat(linePayload.product()).isEqualTo("Premium Widget");
        assertThat(linePayload.quantity()).isEqualTo(10);

        // 4. Store snapshot (with metrics)
        DomainEventMessage<?> snap = new GenericDomainEventMessage<>(
                "FullTest", aggId, 1,
                new InvoiceCreated(aggId, "FullTestCustomer", 750.0));
        engineWithAllFeatures.storeSnapshot(snap);
        double snapStored =
                meterRegistry.get("axon.eventstoredb.snapshots.stored").counter().count();
        assertThat(snapStored).isGreaterThanOrEqualTo(1.0);

        // 5. Read snapshot
        Optional<DomainEventMessage<?>> loadedSnap =
                engineWithAllFeatures.readSnapshot(aggId);
        assertThat(loadedSnap).isPresent();

        // 6. Track from global stream (with upcasting + metrics)
        upcastBefore = upcaster.getInvocationCount();
        Stream<? extends TrackedEventMessage<?>> tracked =
                engineWithAllFeatures.readEvents(
                        engineWithAllFeatures.createTailToken(), false);
        long fullTestEvents = tracked
                .filter(e -> e instanceof DomainEventMessage<?> de
                        && aggId.equals(de.getAggregateIdentifier()))
                .count();
        assertThat(fullTestEvents).isEqualTo(2);
        assertThat(upcaster.getInvocationCount()).isGreaterThan(upcastBefore);

        // 7. Last sequence number (with retry)
        Optional<Long> seq = engineWithAllFeatures.lastSequenceNumberFor(aggId);
        assertThat(seq).isPresent().hasValue(1L);
    }

    @Test
    @Order(51)
    void metricsShouldHaveAccumulatedCorrectlyAcrossAllTests() {
        // After all tests, verify metrics are sensible
        double totalAppended =
                meterRegistry.get("axon.eventstoredb.events.appended").counter().count();
        double totalRead =
                meterRegistry.get("axon.eventstoredb.events.read").counter().count();
        double totalSnapshotsStored =
                meterRegistry.get("axon.eventstoredb.snapshots.stored").counter().count();
        double totalSnapshotsRead =
                meterRegistry.get("axon.eventstoredb.snapshots.read").counter().count();

        assertThat(totalAppended)
                .as("Total events appended across all tests")
                .isGreaterThanOrEqualTo(4.0);
        assertThat(totalRead)
                .as("Total events read across all tests")
                .isGreaterThanOrEqualTo(2.0);
        assertThat(totalSnapshotsStored)
                .as("Total snapshots stored")
                .isGreaterThanOrEqualTo(2.0);
        assertThat(totalSnapshotsRead)
                .as("Total snapshots read")
                .isGreaterThanOrEqualTo(1.0);
    }
}
