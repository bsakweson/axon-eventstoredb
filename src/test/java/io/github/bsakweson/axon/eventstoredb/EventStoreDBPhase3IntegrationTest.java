package io.github.bsakweson.axon.eventstoredb;

import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.resilience.RetryPolicy;
import io.github.bsakweson.axon.eventstoredb.subscriptions.EventStoreDBPersistentSubscriptionMessageSource;
import io.github.bsakweson.axon.eventstoredb.tokenstore.DistributedTokenClaimManager;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
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
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Phase 3 features (persistent subscriptions, distributed
 * token claims) using Testcontainers with a real EventStoreDB instance.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Persistent subscriptions receive pushed events from EventStoreDB</li>
 *   <li>Distributed token claims enforce single-owner semantics across nodes</li>
 *   <li>Claim expiry allows failover when a node crashes</li>
 *   <li>TokenStore delegates to DistributedTokenClaimManager when configured</li>
 *   <li>All Phase 3 features work end-to-end with a live database</li>
 * </ul>
 */
@Testcontainers
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventStoreDBPhase3IntegrationTest {

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
    private EventStoreDBPersistentSubscriptionsClient persistentSubClient;
    private EventStoreDBEventStorageEngine engine;
    private Serializer axonSerializer;
    private EventStoreDBStreamNaming naming;
    private MeterRegistry meterRegistry;
    private EventStoreDBMetrics metrics;
    private EventStoreDBRetryExecutor retryExecutor;

    // ── Payload types ────────────────────────────────────────────────────

    public record OrderPlaced(String orderId, String customer, double total) {
    }

    public record OrderShipped(String orderId, String trackingNumber) {
    }

    @BeforeAll
    void setUp() {
        String host = EVENT_STORE_DB.getHost();
        Integer port = EVENT_STORE_DB.getMappedPort(ESDB_HTTP_PORT);
        String connectionString = "esdb://" + host + ":" + port + "?tls=false";

        EventStoreDBClientSettings settings =
                EventStoreDBConnectionString.parseOrThrow(connectionString);
        client = EventStoreDBClient.create(settings);
        persistentSubClient = EventStoreDBPersistentSubscriptionsClient.create(settings);

        axonSerializer = JacksonSerializer.defaultSerializer();
        naming = new EventStoreDBStreamNaming();
        meterRegistry = new SimpleMeterRegistry();
        metrics = new EventStoreDBMetrics(meterRegistry);

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(3)
                .initialBackoffMs(50)
                .maxBackoffMs(1000)
                .multiplier(2.0)
                .build();
        retryExecutor = new EventStoreDBRetryExecutor(retryPolicy);

        engine = new EventStoreDBEventStorageEngine(
                client, axonSerializer, naming, 256,
                null, retryExecutor, metrics);
    }

    @AfterAll
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        if (persistentSubClient != null) {
            persistentSubClient.shutdown();
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PERSISTENT SUBSCRIPTION INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(1)
    void shouldCreatePersistentSubscriptionGroup() {
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName("phase3-it-group")
                        .bufferSize(256)
                        .metrics(metrics)
                        .build();

        assertThatCode(source::createSubscriptionIfNotExists)
                .doesNotThrowAnyException();
    }

    @Test
    @Order(2)
    void shouldHandleIdempotentSubscriptionCreation() {
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName("phase3-it-group")
                        .bufferSize(256)
                        .build();

        // Second creation should be a no-op (group already exists)
        assertThatCode(source::createSubscriptionIfNotExists)
                .doesNotThrowAnyException();
    }

    @Test
    @Order(3)
    void shouldStartAndStopPersistentSubscription() {
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName("phase3-it-group")
                        .bufferSize(256)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();
        assertThat(source.isRunning()).isTrue();

        source.stop();
        assertThat(source.isRunning()).isFalse();
    }

    @Test
    @Order(4)
    void shouldNotStartTwice() {
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName("phase3-it-group")
                        .bufferSize(256)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();
        // Second start should warn but not throw
        assertThatCode(source::start).doesNotThrowAnyException();
        assertThat(source.isRunning()).isTrue();

        source.stop();
    }

    @Test
    @Order(10)
    void shouldReceiveEventsViaPersistentSubscription() throws Exception {
        // Create a fresh subscription group for this test
        String groupName = "phase3-receive-events";

        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName(groupName)
                        .bufferSize(256)
                        .metrics(metrics)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();

        // Wait a moment for subscription to stabilize
        Thread.sleep(500);

        // Append events AFTER subscription is started
        String aggId = UUID.randomUUID().toString();
        DomainEventMessage<?> event1 = new GenericDomainEventMessage<>(
                "Shipment", aggId, 0,
                new OrderPlaced(aggId, "Charlie", 199.99));
        DomainEventMessage<?> event2 = new GenericDomainEventMessage<>(
                "Shipment", aggId, 1,
                new OrderShipped(aggId, "TRK-" + aggId.substring(0, 8)));
        engine.appendEvents(List.of(event1));
        engine.appendEvents(List.of(event2));

        // Wait for events to arrive in the buffer (pushed by EventStoreDB)
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Stream<? extends TrackedEventMessage<?>> events =
                            source.readEvents(null, false);
                    List<? extends TrackedEventMessage<?>> batch = events.toList();
                    assertThat(batch).isNotEmpty();
                });

        source.stop();
    }

    @Test
    @Order(11)
    void shouldReturnGroupName() {
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName("my-group")
                        .bufferSize(128)
                        .build();

        assertThat(source.getGroupName()).isEqualTo("my-group");
    }

    @Test
    @Order(12)
    void shouldReadEventsWithBlocking() throws Exception {
        String groupName = "phase3-blocking-read";

        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName(groupName)
                        .bufferSize(256)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();

        Thread.sleep(500);

        String aggId = UUID.randomUUID().toString();
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "BlockTest", aggId, 0,
                new OrderPlaced(aggId, "BlockCustomer", 50.0));
        engine.appendEvents(List.of(event));

        // mayBlock=true should wait briefly for events
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Stream<? extends TrackedEventMessage<?>> events =
                            source.readEvents(null, true);
                    assertThat(events.toList()).isNotEmpty();
                });

        source.stop();
    }

    @Test
    @Order(13)
    void shouldReturnEmptyWhenNoEventsAvailable() throws Exception {
        String groupName = "phase3-empty-read";

        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName(groupName)
                        .bufferSize(256)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();

        // Read immediately — no events appended, should return empty
        Stream<? extends TrackedEventMessage<?>> events =
                source.readEvents(null, false);
        assertThat(events.toList()).isEmpty();

        source.stop();
    }

    @Test
    @Order(14)
    void shouldReceiveEventsWithTrackingTokens() throws Exception {
        String groupName = "phase3-tracking-tokens";

        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName(groupName)
                        .bufferSize(256)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();

        Thread.sleep(500);

        String aggId = UUID.randomUUID().toString();
        DomainEventMessage<?> event = new GenericDomainEventMessage<>(
                "TokenTest", aggId, 0,
                new OrderPlaced(aggId, "TokenCustomer", 75.0));
        engine.appendEvents(List.of(event));

        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Stream<? extends TrackedEventMessage<?>> events =
                            source.readEvents(null, false);
                    List<? extends TrackedEventMessage<?>> batch = events.toList();
                    assertThat(batch).isNotEmpty();
                    // Every event from persistent subscription must have a tracking token
                    for (TrackedEventMessage<?> msg : batch) {
                        assertThat(msg.trackingToken())
                                .isNotNull()
                                .isInstanceOf(EventStoreDBTrackingToken.class);
                    }
                });

        source.stop();
    }

    // ════════════════════════════════════════════════════════════════════════
    // DISTRIBUTED TOKEN CLAIM INTEGRATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(20)
    void shouldClaimAndReleaseSegment() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-A",
                Duration.ofSeconds(30), retryExecutor, metrics);

        TrackingToken token = engine.createHeadToken();

        // Claim
        assertThatCode(() -> claimManager.claimSegment("claim-proc", 0, token))
                .doesNotThrowAnyException();
        assertThat(claimManager.isClaimedByThisNode("claim-proc", 0)).isTrue();
        assertThat(claimManager.getClaimOwner("claim-proc", 0)).isEqualTo("node-A");

        // Release
        claimManager.releaseClaim("claim-proc", 0);
        assertThat(claimManager.isClaimedByThisNode("claim-proc", 0)).isFalse();
        assertThat(claimManager.getClaimOwner("claim-proc", 0)).isNull();
    }

    @Test
    @Order(21)
    void shouldPreventConcurrentClaimsByDifferentNodes() {
        DistributedTokenClaimManager nodeA = new DistributedTokenClaimManager(
                client, naming, "node-alpha",
                Duration.ofSeconds(30), retryExecutor, null);
        DistributedTokenClaimManager nodeB = new DistributedTokenClaimManager(
                client, naming, "node-beta",
                Duration.ofSeconds(30), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();

        // Node A claims first
        nodeA.claimSegment("contention-proc", 0, token);
        assertThat(nodeA.isClaimedByThisNode("contention-proc", 0)).isTrue();

        // Node B should fail to claim the same segment
        assertThatThrownBy(() -> nodeB.claimSegment("contention-proc", 0, token))
                .isInstanceOf(UnableToClaimTokenException.class)
                .hasMessageContaining("node-alpha");

        // Cleanup
        nodeA.releaseClaim("contention-proc", 0);
    }

    @Test
    @Order(22)
    void shouldAllowClaimAfterRelease() {
        DistributedTokenClaimManager nodeA = new DistributedTokenClaimManager(
                client, naming, "node-release-A",
                Duration.ofSeconds(30), retryExecutor, null);
        DistributedTokenClaimManager nodeB = new DistributedTokenClaimManager(
                client, naming, "node-release-B",
                Duration.ofSeconds(30), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();

        // Node A claims, then releases
        nodeA.claimSegment("release-proc", 0, token);
        nodeA.releaseClaim("release-proc", 0);

        // Node B should be able to claim now
        assertThatCode(() -> nodeB.claimSegment("release-proc", 0, token))
                .doesNotThrowAnyException();
        assertThat(nodeB.isClaimedByThisNode("release-proc", 0)).isTrue();

        // Cleanup
        nodeB.releaseClaim("release-proc", 0);
    }

    @Test
    @Order(23)
    void shouldExtendExistingClaim() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-extend",
                Duration.ofSeconds(30), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();
        claimManager.claimSegment("extend-proc", 0, token);

        // Extend should succeed when this node holds the claim
        assertThatCode(() -> claimManager.extendClaim("extend-proc", 0))
                .doesNotThrowAnyException();
        assertThat(claimManager.isClaimedByThisNode("extend-proc", 0)).isTrue();

        claimManager.releaseClaim("extend-proc", 0);
    }

    @Test
    @Order(24)
    void shouldRejectExtendFromDifferentNode() {
        DistributedTokenClaimManager nodeA = new DistributedTokenClaimManager(
                client, naming, "node-ext-A",
                Duration.ofSeconds(30), retryExecutor, null);
        DistributedTokenClaimManager nodeB = new DistributedTokenClaimManager(
                client, naming, "node-ext-B",
                Duration.ofSeconds(30), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();
        nodeA.claimSegment("ext-reject-proc", 0, token);

        // Node B should not be able to extend Node A's claim
        assertThatThrownBy(() -> nodeB.extendClaim("ext-reject-proc", 0))
                .isInstanceOf(UnableToClaimTokenException.class);

        nodeA.releaseClaim("ext-reject-proc", 0);
    }

    @Test
    @Order(25)
    void shouldRejectExtendWhenNoClaim() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-no-claim",
                Duration.ofSeconds(30), retryExecutor, null);

        assertThatThrownBy(() -> claimManager.extendClaim("unclaimed-proc", 99))
                .isInstanceOf(UnableToClaimTokenException.class)
                .hasMessageContaining("No active claim");
    }

    @Test
    @Order(26)
    void shouldAllowClaimWhenPreviousClaimExpired() {
        // Use very short timeout so the claim expires quickly
        DistributedTokenClaimManager nodeA = new DistributedTokenClaimManager(
                client, naming, "node-expire-A",
                Duration.ofMillis(100), retryExecutor, null);
        DistributedTokenClaimManager nodeB = new DistributedTokenClaimManager(
                client, naming, "node-expire-B",
                Duration.ofMillis(100), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();
        nodeA.claimSegment("expiry-proc", 0, token);

        // Wait for the claim to expire
        await().atMost(2, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(nodeA.isClaimedByThisNode("expiry-proc", 0)).isFalse();
                });

        // Node B should be able to claim now (expired)
        assertThatCode(() -> nodeB.claimSegment("expiry-proc", 0, token))
                .doesNotThrowAnyException();
        assertThat(nodeB.isClaimedByThisNode("expiry-proc", 0)).isTrue();

        nodeB.releaseClaim("expiry-proc", 0);
    }

    @Test
    @Order(27)
    void shouldAllowSameNodeToReclaimOwnSegment() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-reclaim",
                Duration.ofSeconds(30), retryExecutor, null);

        TrackingToken token = engine.createHeadToken();
        claimManager.claimSegment("reclaim-proc", 0, token);

        // Same node claiming again should succeed (idempotent)
        assertThatCode(() -> claimManager.claimSegment("reclaim-proc", 0, token))
                .doesNotThrowAnyException();

        claimManager.releaseClaim("reclaim-proc", 0);
    }

    @Test
    @Order(28)
    void shouldReturnCorrectClaimMetadata() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-meta",
                Duration.ofSeconds(45), retryExecutor, null);

        assertThat(claimManager.getNodeId()).isEqualTo("node-meta");
        assertThat(claimManager.getClaimTimeout()).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    @Order(29)
    void shouldReturnNullOwnerForUnclaimedSegment() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-null",
                Duration.ofSeconds(30), retryExecutor, null);

        assertThat(claimManager.getClaimOwner("nonexistent-proc", 0)).isNull();
        assertThat(claimManager.isClaimedByThisNode("nonexistent-proc", 0)).isFalse();
    }

    @Test
    @Order(30)
    void shouldRecordClaimMetrics() {
        MeterRegistry claimRegistry = new SimpleMeterRegistry();
        EventStoreDBMetrics claimMetrics = new EventStoreDBMetrics(claimRegistry);

        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "node-metrics",
                Duration.ofSeconds(30), retryExecutor, claimMetrics);

        TrackingToken token = engine.createHeadToken();
        claimManager.claimSegment("metrics-proc", 0, token);
        claimManager.extendClaim("metrics-proc", 0);
        claimManager.releaseClaim("metrics-proc", 0);

        double claimOps = claimRegistry.get("axon.eventstoredb.tokens.operations")
                .tag("type", "claim")
                .counter().count();
        double extendOps = claimRegistry.get("axon.eventstoredb.tokens.operations")
                .tag("type", "extendClaim")
                .counter().count();
        double releaseOps = claimRegistry.get("axon.eventstoredb.tokens.operations")
                .tag("type", "releaseClaim")
                .counter().count();

        assertThat(claimOps).isGreaterThanOrEqualTo(1.0);
        assertThat(extendOps).isGreaterThanOrEqualTo(1.0);
        assertThat(releaseOps).isGreaterThanOrEqualTo(1.0);
    }

    // ════════════════════════════════════════════════════════════════════════
    // TOKEN STORE WITH DISTRIBUTED CLAIMS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(40)
    void shouldStoreAndFetchTokenWithClaimManager() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "token-node-A",
                Duration.ofSeconds(30), retryExecutor, null);

        EventStoreDBTokenStore tokenStore = new EventStoreDBTokenStore(
                client, naming, "token-node-A", retryExecutor, null, claimManager);

        tokenStore.initializeTokenSegments("claim-token-proc", 1);

        TrackingToken token = engine.createHeadToken();
        tokenStore.storeToken(token, "claim-token-proc", 0);

        TrackingToken fetched = tokenStore.fetchToken("claim-token-proc", 0);
        assertThat(fetched).isNotNull();
        assertThat(fetched).isInstanceOf(EventStoreDBTrackingToken.class);

        tokenStore.releaseClaim("claim-token-proc", 0);
    }

    @Test
    @Order(41)
    void shouldDelegateClaimExtensionToClaimManager() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "token-ext-node",
                Duration.ofSeconds(30), retryExecutor, null);

        EventStoreDBTokenStore tokenStore = new EventStoreDBTokenStore(
                client, naming, "token-ext-node", retryExecutor, null, claimManager);

        tokenStore.initializeTokenSegments("delegate-proc", 1);

        TrackingToken token = engine.createHeadToken();
        tokenStore.storeToken(token, "delegate-proc", 0);

        // extendClaim should delegate to DistributedTokenClaimManager
        assertThatCode(() -> tokenStore.extendClaim("delegate-proc", 0))
                .doesNotThrowAnyException();

        tokenStore.releaseClaim("delegate-proc", 0);
    }

    @Test
    @Order(42)
    void shouldDelegateClaimReleaseToClaimManager() {
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "token-rel-node",
                Duration.ofSeconds(30), retryExecutor, null);

        EventStoreDBTokenStore tokenStore = new EventStoreDBTokenStore(
                client, naming, "token-rel-node", retryExecutor, null, claimManager);

        tokenStore.initializeTokenSegments("release-delegate-proc", 1);

        TrackingToken token = engine.createHeadToken();
        tokenStore.storeToken(token, "release-delegate-proc", 0);

        // releaseClaim should delegate to DistributedTokenClaimManager
        assertThatCode(() -> tokenStore.releaseClaim("release-delegate-proc", 0))
                .doesNotThrowAnyException();

        // After release, claim is gone
        assertThat(claimManager.isClaimedByThisNode("release-delegate-proc", 0))
                .isFalse();
    }

    @Test
    @Order(43)
    void shouldPreventTokenStoreAccessFromNonOwnerNode() {
        DistributedTokenClaimManager claimManagerA = new DistributedTokenClaimManager(
                client, naming, "ts-node-A",
                Duration.ofSeconds(30), retryExecutor, null);
        DistributedTokenClaimManager claimManagerB = new DistributedTokenClaimManager(
                client, naming, "ts-node-B",
                Duration.ofSeconds(30), retryExecutor, null);

        EventStoreDBTokenStore tokenStoreA = new EventStoreDBTokenStore(
                client, naming, "ts-node-A", retryExecutor, null, claimManagerA);
        EventStoreDBTokenStore tokenStoreB = new EventStoreDBTokenStore(
                client, naming, "ts-node-B", retryExecutor, null, claimManagerB);

        tokenStoreA.initializeTokenSegments("contention-token-proc", 1);

        TrackingToken token = engine.createHeadToken();
        tokenStoreA.storeToken(token, "contention-token-proc", 0);

        // Node B should fail to fetch (claim denied)
        assertThatThrownBy(() -> tokenStoreB.fetchToken("contention-token-proc", 0))
                .isInstanceOf(UnableToClaimTokenException.class);

        tokenStoreA.releaseClaim("contention-token-proc", 0);
    }

    @Test
    @Order(44)
    void shouldWorkWithoutClaimManager() {
        // TokenStore without claim manager should work normally (no contention checks)
        EventStoreDBTokenStore tokenStore = new EventStoreDBTokenStore(
                client, naming, "simple-node");

        tokenStore.initializeTokenSegments("no-claims-proc", 1);

        TrackingToken token = engine.createHeadToken();
        tokenStore.storeToken(token, "no-claims-proc", 0);

        TrackingToken fetched = tokenStore.fetchToken("no-claims-proc", 0);
        assertThat(fetched).isNotNull();
    }

    // ════════════════════════════════════════════════════════════════════════
    // MULTIPLE SEGMENTS WITH DISTRIBUTED CLAIMS
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(50)
    void shouldSupportMultipleSegmentsAcrossNodes() {
        DistributedTokenClaimManager claimManagerA = new DistributedTokenClaimManager(
                client, naming, "multi-A",
                Duration.ofSeconds(30), retryExecutor, null);
        DistributedTokenClaimManager claimManagerB = new DistributedTokenClaimManager(
                client, naming, "multi-B",
                Duration.ofSeconds(30), retryExecutor, null);

        EventStoreDBTokenStore tokenStoreA = new EventStoreDBTokenStore(
                client, naming, "multi-A", retryExecutor, null, claimManagerA);
        EventStoreDBTokenStore tokenStoreB = new EventStoreDBTokenStore(
                client, naming, "multi-B", retryExecutor, null, claimManagerB);

        tokenStoreA.initializeTokenSegments("multi-seg-proc", 3);

        int[] segments = tokenStoreA.fetchSegments("multi-seg-proc");
        assertThat(segments).containsExactlyInAnyOrder(0, 1, 2);

        TrackingToken token = engine.createHeadToken();

        // Node A claims segment 0 and 1
        tokenStoreA.storeToken(token, "multi-seg-proc", 0);
        tokenStoreA.storeToken(token, "multi-seg-proc", 1);

        // Node B claims segment 2
        tokenStoreB.storeToken(token, "multi-seg-proc", 2);

        // Verify ownership distribution
        assertThat(claimManagerA.getClaimOwner("multi-seg-proc", 0)).isEqualTo("multi-A");
        assertThat(claimManagerA.getClaimOwner("multi-seg-proc", 1)).isEqualTo("multi-A");
        assertThat(claimManagerB.getClaimOwner("multi-seg-proc", 2)).isEqualTo("multi-B");

        // Node B cannot steal segment 0
        assertThatThrownBy(() -> tokenStoreB.storeToken(token, "multi-seg-proc", 0))
                .isInstanceOf(UnableToClaimTokenException.class);

        // Cleanup
        tokenStoreA.releaseClaim("multi-seg-proc", 0);
        tokenStoreA.releaseClaim("multi-seg-proc", 1);
        tokenStoreB.releaseClaim("multi-seg-proc", 2);
    }

    // ════════════════════════════════════════════════════════════════════════
    // END-TO-END ROUND TRIP WITH ALL PHASE 3 FEATURES
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @Order(60)
    void shouldPerformFullPhase3RoundTrip() throws Exception {
        // 1. Set up distributed claims
        DistributedTokenClaimManager claimManager = new DistributedTokenClaimManager(
                client, naming, "e2e-node",
                Duration.ofSeconds(30), retryExecutor, metrics);

        EventStoreDBTokenStore tokenStore = new EventStoreDBTokenStore(
                client, naming, "e2e-node", retryExecutor, metrics, claimManager);

        tokenStore.initializeTokenSegments("e2e-proc", 1);

        // 2. Set up persistent subscription
        String groupName = "phase3-e2e-group";
        EventStoreDBPersistentSubscriptionMessageSource source =
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                        .subscriptionClient(persistentSubClient)
                        .eventSerializer(axonSerializer)
                        .naming(naming)
                        .groupName(groupName)
                        .bufferSize(256)
                        .metrics(metrics)
                        .build();

        source.createSubscriptionIfNotExists();
        source.start();
        assertThat(source.isRunning()).isTrue();

        Thread.sleep(500);

        // 3. Append events
        String aggId = UUID.randomUUID().toString();
        DomainEventMessage<?> placed = new GenericDomainEventMessage<>(
                "E2EOrder", aggId, 0,
                new OrderPlaced(aggId, "E2E-Customer", 999.99));
        engine.appendEvents(List.of(placed));

        // 4. Verify events arrive via persistent subscription
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Stream<? extends TrackedEventMessage<?>> events =
                            source.readEvents(null, false);
                    List<? extends TrackedEventMessage<?>> batch = events.toList();
                    assertThat(batch).isNotEmpty();
                });

        // 5. Store token with distributed claim
        TrackingToken head = engine.createHeadToken();
        tokenStore.storeToken(head, "e2e-proc", 0);
        assertThat(claimManager.isClaimedByThisNode("e2e-proc", 0)).isTrue();

        // 6. Fetch token back
        TrackingToken fetched = tokenStore.fetchToken("e2e-proc", 0);
        assertThat(fetched).isNotNull();
        assertThat(fetched).isInstanceOf(EventStoreDBTrackingToken.class);

        // 7. Extend claim
        tokenStore.extendClaim("e2e-proc", 0);
        assertThat(claimManager.isClaimedByThisNode("e2e-proc", 0)).isTrue();

        // 8. Release and stop
        tokenStore.releaseClaim("e2e-proc", 0);
        source.stop();

        assertThat(source.isRunning()).isFalse();
        assertThat(claimManager.isClaimedByThisNode("e2e-proc", 0)).isFalse();
    }
}
