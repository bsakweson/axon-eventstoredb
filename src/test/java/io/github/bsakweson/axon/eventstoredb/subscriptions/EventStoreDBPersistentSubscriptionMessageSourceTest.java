package io.github.bsakweson.axon.eventstoredb.subscriptions;

import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.CreatePersistentSubscriptionToAllOptions;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;
import com.eventstore.dbclient.NackAction;
import com.eventstore.dbclient.PersistentSubscription;
import com.eventstore.dbclient.PersistentSubscriptionListener;
import com.eventstore.dbclient.Position;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.SubscribePersistentSubscriptionOptions;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class EventStoreDBPersistentSubscriptionMessageSourceTest {

  @Mock private EventStoreDBPersistentSubscriptionsClient subscriptionClient;
  @Mock private EventStoreDBMetrics metrics;

  private Serializer eventSerializer;
  private EventStoreDBStreamNaming naming;

  @BeforeEach
  void setUp() {
    eventSerializer = JacksonSerializer.defaultSerializer();
    naming = new EventStoreDBStreamNaming();
  }

  // ── Builder / Construction ────────────────────────────────────────────

  @Test
  void shouldBuildWithRequiredParameters() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .groupName("test-group")
            .build();

    assertThat(source).isNotNull();
    assertThat(source.getGroupName()).isEqualTo("test-group");
    assertThat(source.isRunning()).isFalse();
  }

  @Test
  void shouldRejectNullSubscriptionClient() {
    assertThatThrownBy(
            () ->
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                    .subscriptionClient(null)
                    .eventSerializer(eventSerializer)
                    .groupName("test")
                    .build())
        .isInstanceOf(AxonConfigurationException.class)
        .hasMessageContaining("must not be null");
  }

  @Test
  void shouldRejectNullSerializer() {
    assertThatThrownBy(
            () ->
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                    .subscriptionClient(subscriptionClient)
                    .eventSerializer(null)
                    .groupName("test")
                    .build())
        .isInstanceOf(AxonConfigurationException.class)
        .hasMessageContaining("must not be null");
  }

  @Test
  void shouldRejectBlankGroupName() {
    assertThatThrownBy(
            () ->
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                    .subscriptionClient(subscriptionClient)
                    .eventSerializer(eventSerializer)
                    .groupName("")
                    .build())
        .isInstanceOf(AxonConfigurationException.class)
        .hasMessageContaining("must not be blank");
  }

  @Test
  void shouldRejectNullGroupName() {
    assertThatThrownBy(
            () ->
                EventStoreDBPersistentSubscriptionMessageSource.builder()
                    .subscriptionClient(subscriptionClient)
                    .eventSerializer(eventSerializer)
                    .groupName(null)
                    .build())
        .isInstanceOf(AxonConfigurationException.class)
        .hasMessageContaining("must not be blank");
  }

  @Test
  void shouldUseDefaultBufferSizeWhenNotSet() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .groupName("test-group")
            .build();

    assertThat(source).isNotNull();
  }

  @Test
  void shouldAcceptCustomBufferSizeAndNaming() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(naming)
            .groupName("custom-group")
            .bufferSize(512)
            .metrics(metrics)
            .build();

    assertThat(source.getGroupName()).isEqualTo("custom-group");
  }

  // ── createSubscriptionIfNotExists ─────────────────────────────────────

  @Test
  void shouldCreateSubscriptionGroupSuccessfully() throws Exception {
    when(subscriptionClient.createToAll(
            anyString(),
            any(CreatePersistentSubscriptionToAllOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("create-test");

    assertThatCode(source::createSubscriptionIfNotExists).doesNotThrowAnyException();

    verify(subscriptionClient)
        .createToAll(
            eq("create-test"),
            any(CreatePersistentSubscriptionToAllOptions.class));
  }

  @Test
  void shouldIgnoreIfSubscriptionAlreadyExists() throws Exception {
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(
        new RuntimeException("Subscription group already exists"));
    when(subscriptionClient.createToAll(
            anyString(),
            any(CreatePersistentSubscriptionToAllOptions.class)))
        .thenReturn(failedFuture);

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("existing-group");

    assertThatCode(source::createSubscriptionIfNotExists).doesNotThrowAnyException();
  }

  // ── start / stop ──────────────────────────────────────────────────────

  @Test
  void shouldStartSubscription() throws Exception {
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("start-test");

    source.start();

    assertThat(source.isRunning()).isTrue();
    verify(subscriptionClient)
        .subscribeToAll(
            eq("start-test"),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class));
  }

  @Test
  void shouldNotStartTwice() throws Exception {
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("no-double-start");

    source.start();
    source.start(); // Should warn but not start again

    assertThat(source.isRunning()).isTrue();
    verify(subscriptionClient, times(1))
        .subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class));
  }

  @Test
  void shouldStopRunningSubscription() throws Exception {
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("stop-test");

    source.start();
    assertThat(source.isRunning()).isTrue();

    source.stop();
    assertThat(source.isRunning()).isFalse();
    verify(subscription).stop();
  }

  @Test
  void shouldBeNoOpWhenStopCalledWithoutStart() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("noop-stop");

    assertThatCode(source::stop).doesNotThrowAnyException();
    assertThat(source.isRunning()).isFalse();
  }

  // ── readEvents ────────────────────────────────────────────────────────

  @Test
  void shouldReturnEmptyStreamWhenNoEvents() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("empty-read");

    java.util.stream.Stream<? extends TrackedEventMessage<?>> events =
        source.readEvents(null, false);

    assertThat(events.count()).isZero();
  }

  @Test
  void shouldReturnEmptyStreamWhenNoEventsBlocking() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("empty-blocking-read");

    // Non-blocking read returns empty immediately
    long count = source.readEvents(null, false).count();
    assertThat(count).isZero();
  }

  @Test
  void shouldThrowOnStartFailure() throws Exception {
    CompletableFuture<PersistentSubscription> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("connection refused"));
    when(subscriptionClient.subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(failedFuture);

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("start-fail");

    assertThatThrownBy(source::start)
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to start persistent subscription");
    assertThat(source.isRunning()).isFalse();
  }

  @Test
  void shouldIgnoreCreateSubscriptionAlreadyExistsWrapped() throws Exception {
    // Test the case where the "already exists" message is in the cause
    RuntimeException cause = new RuntimeException("Subscription group EXIST");
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("wrapped", cause));
    when(subscriptionClient.createToAll(
            anyString(),
            any(CreatePersistentSubscriptionToAllOptions.class)))
        .thenReturn(failedFuture);

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("exist-wrapped");

    assertThatCode(source::createSubscriptionIfNotExists).doesNotThrowAnyException();
  }

  @Test
  void shouldThrowOnCreateSubscriptionUnexpectedError() throws Exception {
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(
        new RuntimeException("unexpected error"));
    when(subscriptionClient.createToAll(
            anyString(),
            any(CreatePersistentSubscriptionToAllOptions.class)))
        .thenReturn(failedFuture);

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("create-fail");

    assertThatThrownBy(source::createSubscriptionIfNotExists)
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to create persistent subscription");
  }

  @Test
  void shouldHandleStopWithErrorGracefully() throws Exception {
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    doThrow(new RuntimeException("stop error")).when(subscription).stop();
    when(subscriptionClient.subscribeToAll(
            anyString(),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        buildSource("stop-error");

    source.start();
    assertThatCode(source::stop).doesNotThrowAnyException();
    assertThat(source.isRunning()).isFalse();
  }

  @Test
  void shouldUseDefaultBufferSizeWhenNegative() {
    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .groupName("neg-buffer")
            .bufferSize(-1)
            .build();

    assertThat(source.getGroupName()).isEqualTo("neg-buffer");
  }

  // ── handleEvent (via captured listener) ───────────────────────────────

  @Test
  void shouldSkipAndAckSystemStreamEvents() throws Exception {
    PersistentSubscriptionListener listener = captureListener("skip-system");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    ResolvedEvent event = mockSystemStreamEvent("$system-stream");

    listener.onEvent(sub, 0, event);

    verify(sub).ack(event);
    verify(sub, never()).nack(any(), anyString(), any(ResolvedEvent.class));
  }

  @Test
  void shouldSkipAndAckSnapshotStreamEvents() throws Exception {
    PersistentSubscriptionListener listener = captureListener("skip-snapshot");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    ResolvedEvent event = mockSystemStreamEvent("__snapshot-Order-abc");

    listener.onEvent(sub, 0, event);

    verify(sub).ack(event);
    verify(sub, never()).nack(any(), anyString(), any(ResolvedEvent.class));
  }

  @Test
  void shouldSkipAndAckTokenStreamEvents() throws Exception {
    PersistentSubscriptionListener listener = captureListener("skip-token");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    ResolvedEvent event = mockSystemStreamEvent("__axon-tokens-Proj-0");

    listener.onEvent(sub, 0, event);

    verify(sub).ack(event);
    verify(sub, never()).nack(any(), anyString(), any(ResolvedEvent.class));
  }

  @Test
  void shouldDeserializeAndBufferValidEvent() throws Exception {
    PersistentSubscriptionListener listener = captureListener("handle-valid");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    ResolvedEvent event = mockValidDomainEvent("Order-abc123", 200, 100);

    listener.onEvent(sub, 0, event);

    verify(sub).ack(event);
    verify(sub, never()).nack(any(), anyString(), any(ResolvedEvent.class));
    verify(metrics).recordEventsRead(1);
  }

  @Test
  void shouldNackWhenDeserializationFails() throws Exception {
    PersistentSubscriptionListener listener = captureListener("handle-deser-fail");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    // Event with invalid payload data — deserialization will fail
    ResolvedEvent event = mockResolvedEventWithBadData("Order-xyz", 40, 30);

    listener.onEvent(sub, 0, event);

    verify(sub, never()).ack(event);
    verify(sub).nack(eq(NackAction.Retry), anyString(), eq(event));
    verify(metrics).recordError("persistentSubscription.processError");
  }

  @Test
  void shouldNackWhenBufferIsFull() throws Exception {
    // Build source with buffer size of 1 so it fills immediately
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            eq("buffer-full"),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(naming)
            .groupName("buffer-full")
            .bufferSize(1)
            .metrics(metrics)
            .build();

    source.start();

    ArgumentCaptor<PersistentSubscriptionListener> captor =
        ArgumentCaptor.forClass(PersistentSubscriptionListener.class);
    verify(subscriptionClient).subscribeToAll(
        eq("buffer-full"),
        any(SubscribePersistentSubscriptionOptions.class),
        captor.capture());
    PersistentSubscriptionListener listener = captor.getValue();

    PersistentSubscription sub = mock(PersistentSubscription.class);

    // Fill the buffer with one valid event
    ResolvedEvent first = mockValidDomainEvent("Order-1", 20, 10);
    listener.onEvent(sub, 0, first);
    verify(sub).ack(first);

    // Second event should be nacked — buffer is full
    ResolvedEvent second = mockValidDomainEvent("Order-2", 40, 30);
    listener.onEvent(sub, 0, second);
    verify(sub).nack(eq(NackAction.Retry), eq("Buffer full"), eq(second));
    verify(metrics).recordError("persistentSubscription.bufferFull");
  }

  // ── onCancelled (via captured listener) ───────────────────────────────

  @Test
  void shouldHandleCancelledWithError() throws Exception {
    PersistentSubscriptionListener listener = captureListener("cancel-error");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    listener.onCancelled(sub, new RuntimeException("network failure"));

    verify(metrics).recordError("persistentSubscription");
  }

  @Test
  void shouldHandleCancelledWithoutError() throws Exception {
    PersistentSubscriptionListener listener = captureListener("cancel-normal");

    PersistentSubscription sub = mock(PersistentSubscription.class);
    listener.onCancelled(sub, null);

    // Should not record any error for graceful cancellation
    verify(metrics, never()).recordError(anyString());
  }

  // ── readEvents with buffered events ───────────────────────────────────

  @Test
  void shouldReadBufferedEventsNonBlocking() throws Exception {
    // Build a source, push events via listener, then read from the same source
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            eq("read-buffered"),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(naming)
            .groupName("read-buffered")
            .bufferSize(10)
            .metrics(metrics)
            .build();

    source.start();

    ArgumentCaptor<PersistentSubscriptionListener> captor =
        ArgumentCaptor.forClass(PersistentSubscriptionListener.class);
    verify(subscriptionClient).subscribeToAll(
        eq("read-buffered"),
        any(SubscribePersistentSubscriptionOptions.class),
        captor.capture());

    PersistentSubscription sub = mock(PersistentSubscription.class);
    for (int i = 0; i < 3; i++) {
      ResolvedEvent event = mockValidDomainEvent("Order-" + i, 20 + i, 10 + i);
      captor.getValue().onEvent(sub, 0, event);
    }

    Stream<? extends TrackedEventMessage<?>> events = source.readEvents(null, false);
    assertThat(events.count()).isEqualTo(3);
  }

  @Test
  void shouldReadEventsWithMetricsRecording() throws Exception {
    // Build a source, push events into its buffer via listener, then read
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            eq("read-metrics"),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(naming)
            .groupName("read-metrics")
            .bufferSize(10)
            .metrics(metrics)
            .build();

    source.start();

    ArgumentCaptor<PersistentSubscriptionListener> captor =
        ArgumentCaptor.forClass(PersistentSubscriptionListener.class);
    verify(subscriptionClient).subscribeToAll(
        eq("read-metrics"),
        any(SubscribePersistentSubscriptionOptions.class),
        captor.capture());

    PersistentSubscription sub = mock(PersistentSubscription.class);
    ResolvedEvent event = mockValidDomainEvent("Order-m1", 60, 50);
    captor.getValue().onEvent(sub, 0, event);

    // Now read the events (non-blocking)
    Stream<? extends TrackedEventMessage<?>> events = source.readEvents(null, false);
    long count = events.count();

    assertThat(count).isEqualTo(1);
    // recordEventsRead called once from handleEvent(1) and once from readEvents(1)
    verify(metrics, atLeast(1)).recordEventsRead(1);
  }

  @Test
  void shouldReturnEmptyAndNotRecordMetricsWhenNoEvents() {
    EventStoreDBPersistentSubscriptionMessageSource source = buildSource("no-events-metrics");

    Stream<? extends TrackedEventMessage<?>> events = source.readEvents(null, false);
    assertThat(events.count()).isZero();

    // Should not record metrics for empty batch
    verify(metrics, never()).recordEventsRead(anyInt());
  }

  @Test
  void shouldBuildWithoutMetrics() throws Exception {
    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(naming)
            .groupName("no-metrics")
            .build();

    // Ensure it doesn't NPE on operations without metrics
    assertThat(source.getGroupName()).isEqualTo("no-metrics");

    Stream<? extends TrackedEventMessage<?>> events = source.readEvents(null, false);
    assertThat(events.count()).isZero();
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  /**
   * Starts a source and captures the PersistentSubscriptionListener passed to subscribeToAll.
   */
  private PersistentSubscriptionListener captureListener(String group) throws Exception {
    PersistentSubscription subscription = mock(PersistentSubscription.class);
    when(subscriptionClient.subscribeToAll(
            eq(group),
            any(SubscribePersistentSubscriptionOptions.class),
            any(PersistentSubscriptionListener.class)))
        .thenReturn(CompletableFuture.completedFuture(subscription));

    EventStoreDBPersistentSubscriptionMessageSource source = buildSource(group);
    source.start();

    ArgumentCaptor<PersistentSubscriptionListener> captor =
        ArgumentCaptor.forClass(PersistentSubscriptionListener.class);
    verify(subscriptionClient).subscribeToAll(
        eq(group),
        any(SubscribePersistentSubscriptionOptions.class),
        captor.capture());

    return captor.getValue();
  }

  /**
   * Creates a mock ResolvedEvent for a system/internal stream.
   * Only stubs getStreamId() — no position or payload needed since
   * handleEvent skips and acks immediately.
   */
  private ResolvedEvent mockSystemStreamEvent(String streamId) {
    RecordedEvent recorded = mock(RecordedEvent.class);
    when(recorded.getStreamId()).thenReturn(streamId);

    ResolvedEvent resolved = mock(ResolvedEvent.class);
    when(resolved.getOriginalEvent()).thenReturn(recorded);
    return resolved;
  }

  /**
   * Creates a mock ResolvedEvent with valid serialized Axon event metadata and payload
   * so that EventStoreDBSerializer.deserialize() succeeds.
   */
  private ResolvedEvent mockValidDomainEvent(
      String streamId, long commit, long prepare) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> metadata = Map.of(
          "axon-message-id", UUID.randomUUID().toString(),
          "axon-message-type", "java.util.LinkedHashMap",
          "axon-message-timestamp", Instant.now().toString(),
          "axon-message-aggregate-type", "Order",
          "axon-message-aggregate-id", streamId.contains("-")
              ? streamId.substring(streamId.indexOf('-') + 1) : streamId,
          "axon-message-aggregate-seq", 0);
      byte[] metadataBytes = mapper.writeValueAsBytes(metadata);

      Map<String, Object> payload = Map.of("orderId", "test-123");
      byte[] payloadBytes = mapper.writeValueAsBytes(payload);

      RecordedEvent recorded = mock(RecordedEvent.class);
      when(recorded.getStreamId()).thenReturn(streamId);
      Position position = new Position(commit, prepare);
      when(recorded.getPosition()).thenReturn(position);
      when(recorded.getUserMetadata()).thenReturn(metadataBytes);
      when(recorded.getEventData()).thenReturn(payloadBytes);
      when(recorded.getEventType()).thenReturn("OrderCreatedEvent");
      when(recorded.getEventId()).thenReturn(UUID.randomUUID());
      when(recorded.getCreated()).thenReturn(Instant.now());
      when(recorded.getRevision()).thenReturn(0L);

      ResolvedEvent resolved = mock(ResolvedEvent.class);
      when(resolved.getOriginalEvent()).thenReturn(recorded);
      return resolved;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create mock event", e);
    }
  }

  /**
   * Creates a mock ResolvedEvent with a valid stream name but invalid event data
   * that will cause deserialization to fail.
   */
  private ResolvedEvent mockResolvedEventWithBadData(
      String streamId, long commit, long prepare) {
    RecordedEvent recorded = mock(RecordedEvent.class);
    when(recorded.getStreamId()).thenReturn(streamId);
    lenient().when(recorded.getPosition()).thenReturn(new Position(commit, prepare));
    lenient().when(recorded.getUserMetadata()).thenReturn(new byte[]{0x00});
    lenient().when(recorded.getEventData()).thenReturn(new byte[]{0x00});
    lenient().when(recorded.getEventType()).thenReturn("BadEvent");
    lenient().when(recorded.getEventId()).thenReturn(UUID.randomUUID());
    lenient().when(recorded.getRevision()).thenReturn(0L);

    ResolvedEvent resolved = mock(ResolvedEvent.class);
    when(resolved.getOriginalEvent()).thenReturn(recorded);
    return resolved;
  }

  private EventStoreDBPersistentSubscriptionMessageSource buildSource(String groupName) {
    return EventStoreDBPersistentSubscriptionMessageSource.builder()
        .subscriptionClient(subscriptionClient)
        .eventSerializer(eventSerializer)
        .naming(naming)
        .groupName(groupName)
        .bufferSize(10)
        .metrics(metrics)
        .build();
  }
}
