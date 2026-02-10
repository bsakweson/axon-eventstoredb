package com.bakalr.axon.eventstoredb;

import com.bakalr.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.Position;
import com.eventstore.dbclient.ReadAllOptions;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundExceptionFactory;
import com.eventstore.dbclient.WriteResult;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventStoreDBEventStorageEngineTest {

    @Mock
    private EventStoreDBClient client;

    private Serializer axonSerializer;
    private EventStoreDBStreamNaming naming;
    private EventStoreDBEventStorageEngine engine;

    @BeforeEach
    void setUp() {
        axonSerializer = JacksonSerializer.defaultSerializer();
        naming = new EventStoreDBStreamNaming();
        engine = new EventStoreDBEventStorageEngine(client, axonSerializer, naming, 256);
    }

    // ── Constructor validation ──────────────────────────────────────────

    @Test
    void shouldRejectNullClient() {
        assertThatThrownBy(() -> new EventStoreDBEventStorageEngine(null, axonSerializer, naming, 256))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("EventStoreDBClient must not be null");
    }

    @Test
    void shouldRejectNullSerializer() {
        assertThatThrownBy(() -> new EventStoreDBEventStorageEngine(client, null, naming, 256))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("Event Serializer must not be null");
    }

    @Test
    void shouldUseDefaultNamingWhenNull() {
        EventStoreDBEventStorageEngine eng =
                new EventStoreDBEventStorageEngine(client, axonSerializer, null, 256);
        assertThat(eng).isNotNull();
    }

    @Test
    void shouldUseDefaultBatchSizeWhenZeroOrNegative() {
        assertThatCode(() -> new EventStoreDBEventStorageEngine(client, axonSerializer, naming, 0))
                .doesNotThrowAnyException();
        assertThatCode(() -> new EventStoreDBEventStorageEngine(client, axonSerializer, naming, -5))
                .doesNotThrowAnyException();
    }

    // ── appendEvents ────────────────────────────────────────────────────

    @Test
    void shouldNotAppendEmptyEventList() {
        engine.appendEvents(Collections.emptyList());
        verifyNoInteractions(client);
    }

    @Test
    void shouldAppendFirstDomainEventWithNoStreamExpectedRevision() throws Exception {
        TestPayload payload = new TestPayload("data");
        GenericDomainEventMessage<TestPayload> event =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload);

        WriteResult writeResult = mock(WriteResult.class);
        when(writeResult.getNextExpectedRevision()).thenReturn(ExpectedRevision.expectedRevision(0));
        when(client.appendToStream(eq("Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        engine.appendEvents(List.of(event));

        verify(client).appendToStream(eq("Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldAppendSubsequentDomainEventWithExpectedRevision() throws Exception {
        TestPayload payload = new TestPayload("data");
        GenericDomainEventMessage<TestPayload> event =
                new GenericDomainEventMessage<>("Order", "order-1", 5L, payload);

        WriteResult writeResult = mock(WriteResult.class);
        when(writeResult.getNextExpectedRevision()).thenReturn(ExpectedRevision.expectedRevision(5));
        when(client.appendToStream(eq("Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        engine.appendEvents(List.of(event));

        verify(client).appendToStream(eq("Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldThrowEventStoreExceptionOnConcurrencyConflict() throws Exception {
        TestPayload payload = new TestPayload("data");
        GenericDomainEventMessage<TestPayload> event =
                new GenericDomainEventMessage<>("Order", "order-1", 1L, payload);

        CompletableFuture<WriteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(mock(WrongExpectedVersionException.class));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.appendEvents(List.of(event)))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Concurrent modification");
    }

    @Test
    void shouldThrowEventStoreExceptionOnGenericAppendFailure() throws Exception {
        TestPayload payload = new TestPayload("data");
        GenericDomainEventMessage<TestPayload> event =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload);

        CompletableFuture<WriteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("connection lost"));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.appendEvents(List.of(event)))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to append events");
    }

    @Test
    void shouldHandleInterruptedExceptionOnAppend() throws Exception {
        TestPayload payload = new TestPayload("data");
        GenericDomainEventMessage<TestPayload> event =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload);

        CompletableFuture<WriteResult> interruptedFuture = new CompletableFuture<>();
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(interruptedFuture);

        // Simulate InterruptedException by cancelling the future's get()
        Thread testThread = Thread.currentThread();
        new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // intentionally empty
            }
            testThread.interrupt();
        }).start();

        assertThatThrownBy(() -> engine.appendEvents(List.of(event)))
                .isInstanceOf(EventStoreException.class);

        // Clear interrupted flag
        Thread.interrupted();
    }

    @Test
    void shouldLogWarningForNonDomainEventMessage() {
        // Non-DomainEventMessage should be silently skipped (with warning log)
        GenericEventMessage<TestPayload> nonDomainEvent =
                new GenericEventMessage<>(new TestPayload("non-domain"));

        // Should not throw or interact with client
        assertThatCode(() -> engine.appendEvents(List.of(nonDomainEvent)))
                .doesNotThrowAnyException();
        verifyNoInteractions(client);
    }

    // ── storeSnapshot ───────────────────────────────────────────────────

    @Test
    void shouldStoreSnapshotInDedicatedStream() throws Exception {
        TestPayload payload = new TestPayload("snapshot-data");
        GenericDomainEventMessage<TestPayload> snapshot =
                new GenericDomainEventMessage<>("Order", "order-1", 5L, payload);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(eq("__snapshot-Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        engine.storeSnapshot(snapshot);

        verify(client).appendToStream(eq("__snapshot-Order-order-1"), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldThrowOnSnapshotStoreFailure() throws Exception {
        TestPayload payload = new TestPayload("snapshot-data");
        GenericDomainEventMessage<TestPayload> snapshot =
                new GenericDomainEventMessage<>("Order", "order-1", 5L, payload);

        CompletableFuture<WriteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("write failed"));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.storeSnapshot(snapshot))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to store snapshot");
    }

    @Test
    void shouldThrowOnSnapshotStoreInterruption() throws Exception {
        TestPayload payload = new TestPayload("snapshot-data");
        GenericDomainEventMessage<TestPayload> snapshot =
                new GenericDomainEventMessage<>("Order", "order-1", 5L, payload);

        CompletableFuture<WriteResult> hangingFuture = new CompletableFuture<>();
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(hangingFuture);

        Thread testThread = Thread.currentThread();
        new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // intentionally empty
            }
            testThread.interrupt();
        }).start();

        assertThatThrownBy(() -> engine.storeSnapshot(snapshot))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── createTailToken / createHeadToken ────────────────────────────────

    @Test
    void shouldReturnNullForTailToken() {
        assertThat(engine.createTailToken()).isNull();
    }

    @Test
    void shouldReturnHeadTokenFromLastAllEvent() throws Exception {
        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        Position position = new Position(500L, 500L);
        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);
        when(recordedEvent.getPosition()).thenReturn(position);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        TrackingToken head = engine.createHeadToken();

        assertThat(head).isNotNull().isInstanceOf(EventStoreDBTrackingToken.class);
        assertThat(((EventStoreDBTrackingToken) head).getCommitPosition()).isEqualTo(500L);
    }

    @Test
    void shouldReturnNullHeadTokenWhenStoreEmpty() throws Exception {
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        assertThat(engine.createHeadToken()).isNull();
    }

    @Test
    void shouldThrowOnHeadTokenReadFailure() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("connection failed"));
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.createHeadToken())
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to read head token");
    }

    @Test
    void shouldThrowOnHeadTokenInterruption() throws Exception {
        CompletableFuture<ReadResult> hangingFuture = new CompletableFuture<>();
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(hangingFuture);

        Thread testThread = Thread.currentThread();
        new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // intentionally empty
            }
            testThread.interrupt();
        }).start();

        assertThatThrownBy(() -> engine.createHeadToken())
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── readEvents (tracking) ───────────────────────────────────────────

    @Test
    void shouldReadTrackedEventsFromAllStream() throws Exception {
        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        Position position = new Position(100L, 100L);

        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);
        when(recordedEvent.getStreamId()).thenReturn("Order-order-1");
        when(recordedEvent.getPosition()).thenReturn(position);
        when(recordedEvent.getEventId()).thenReturn(UUID.randomUUID());
        when(recordedEvent.getEventType()).thenReturn("TestPayload");
        when(recordedEvent.getRevision()).thenReturn(0L);
        when(recordedEvent.getCreated()).thenReturn(Instant.now());

        byte[] payloadBytes = "{\"data\":\"test\"}".getBytes();
        byte[] metadataBytes = ("{\"axon-message-id\":\"" + UUID.randomUUID()
                + "\",\"axon-payload-type\":\"" + TestPayload.class.getName()
                + "\",\"axon-timestamp\":\"" + Instant.now()
                + "\",\"axon-aggregate-type\":\"Order\""
                + ",\"axon-aggregate-id\":\"order-1\""
                + ",\"axon-aggregate-seq\":0}").getBytes();
        when(recordedEvent.getEventData()).thenReturn(payloadBytes);
        when(recordedEvent.getUserMetadata()).thenReturn(metadataBytes);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(null, false);
        List<? extends TrackedEventMessage<?>> eventList = events.toList();

        assertThat(eventList).hasSize(1);
        assertThat(eventList.get(0).trackingToken()).isInstanceOf(EventStoreDBTrackingToken.class);
    }

    @Test
    void shouldFilterOutSystemStreamsWhenReadingTrackedEvents() throws Exception {
        ResolvedEvent systemEvent = mock(ResolvedEvent.class);
        RecordedEvent systemRecorded = mock(RecordedEvent.class);
        when(systemEvent.getOriginalEvent()).thenReturn(systemRecorded);
        when(systemRecorded.getStreamId()).thenReturn("$all");

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(systemEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(null, false);
        assertThat(events.toList()).isEmpty();
    }

    @Test
    void shouldFilterOutSnapshotStreamsWhenReadingTrackedEvents() throws Exception {
        ResolvedEvent snapEvent = mock(ResolvedEvent.class);
        RecordedEvent snapRecorded = mock(RecordedEvent.class);
        when(snapEvent.getOriginalEvent()).thenReturn(snapRecorded);
        when(snapRecorded.getStreamId()).thenReturn("__snapshot-Order-1");

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(snapEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(null, false);
        assertThat(events.toList()).isEmpty();
    }

    @Test
    void shouldReadTrackedEventsFromEventStoreDBTrackingToken() throws Exception {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(token, false);
        assertThat(events.toList()).isEmpty();

        verify(client).readAll(any(ReadAllOptions.class));
    }

    @Test
    void shouldReadTrackedEventsFromGlobalSequenceTrackingToken() throws Exception {
        GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(50L);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(token, false);
        assertThat(events.toList()).isEmpty();
    }

    @Test
    void shouldReadTrackedEventsFromUnknownTokenType() throws Exception {
        TrackingToken unknown = mock(TrackingToken.class);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Stream<? extends TrackedEventMessage<?>> events = engine.readEvents(unknown, false);
        assertThat(events.toList()).isEmpty();
    }

    @Test
    void shouldThrowOnTrackedEventsReadFailure() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("connection failed"));
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.readEvents(null, false))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to read tracked events");
    }

    @Test
    void shouldThrowOnTrackedEventsInterruption() throws Exception {
        CompletableFuture<ReadResult> hangingFuture = new CompletableFuture<>();
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(hangingFuture);

        Thread testThread = Thread.currentThread();
        new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // intentionally empty
            }
            testThread.interrupt();
        }).start();

        assertThatThrownBy(() -> engine.readEvents(null, false))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── readEvents (aggregate) ──────────────────────────────────────────

    @Test
    void shouldReturnEmptyStreamWhenAggregateNotFound() throws Exception {
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        DomainEventStream stream = engine.readEvents("nonexistent-id", 0);
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    void shouldReadEventsForKnownAggregate() throws Exception {
        // findStreamForAggregate will scan $all and find our stream
        String aggregateId = "order-1";
        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        // Mock readStream for the aggregate stream
        ResolvedEvent streamEvent = mock(ResolvedEvent.class);
        RecordedEvent streamRecorded = mock(RecordedEvent.class);
        UUID eventId = UUID.randomUUID();
        when(streamEvent.getOriginalEvent()).thenReturn(streamRecorded);
        when(streamRecorded.getEventId()).thenReturn(eventId);
        when(streamRecorded.getEventType()).thenReturn("TestPayload");
        when(streamRecorded.getRevision()).thenReturn(0L);
        when(streamRecorded.getCreated()).thenReturn(Instant.now());
        byte[] payloadBytes = "{\"data\":\"test\"}".getBytes();
        byte[] metaBytes = ("{\"axon-message-id\":\"" + eventId
                + "\",\"axon-payload-type\":\"" + TestPayload.class.getName()
                + "\",\"axon-timestamp\":\"" + Instant.now()
                + "\",\"axon-aggregate-type\":\"Order\""
                + ",\"axon-aggregate-id\":\"" + aggregateId + "\""
                + ",\"axon-aggregate-seq\":0}").getBytes();
        when(streamRecorded.getEventData()).thenReturn(payloadBytes);
        when(streamRecorded.getUserMetadata()).thenReturn(metaBytes);

        ReadResult streamResult = mock(ReadResult.class);
        when(streamResult.getEvents()).thenReturn(List.of(streamEvent));

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(streamResult));

        DomainEventStream stream = engine.readEvents(aggregateId, 0);
        assertThat(stream.hasNext()).isTrue();
        DomainEventMessage<?> event = stream.next();
        assertThat(event.getAggregateIdentifier()).isEqualTo(aggregateId);
    }

    @Test
    void shouldReturnEmptyOnStreamNotFoundException() throws Exception {
        // findStreamForAggregate returns the stream
        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-order-1");

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));

        // But readStream throws StreamNotFound
        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("Order-order-1"));
        when(client.readStream(eq("Order-order-1"), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        DomainEventStream stream = engine.readEvents("order-1", 0);
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    void shouldDelegateReadEventsNoSequence() throws Exception {
        // readEvents(id) delegates to readEvents(id, 0)
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        DomainEventStream stream = engine.readEvents("nonexistent");
        assertThat(stream.hasNext()).isFalse();
    }

    // ── readSnapshot ────────────────────────────────────────────────────

    @Test
    void shouldReturnEmptyWhenNoSnapshotExists() throws Exception {
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Optional<DomainEventMessage<?>> snapshot = engine.readSnapshot("order-1");
        assertThat(snapshot).isEmpty();
    }

    @Test
    void shouldReadSnapshotForKnownAggregate() throws Exception {
        String aggregateId = "order-1";
        UUID eventId = UUID.randomUUID();

        // findSnapshotForAggregate scans $all to find the aggregate type
        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        // readLatestSnapshot reads the snapshot stream
        ResolvedEvent snapEvent = mock(ResolvedEvent.class);
        RecordedEvent snapRecorded = mock(RecordedEvent.class);
        when(snapEvent.getOriginalEvent()).thenReturn(snapRecorded);
        when(snapRecorded.getEventId()).thenReturn(eventId);
        when(snapRecorded.getEventType()).thenReturn("TestPayload");
        when(snapRecorded.getRevision()).thenReturn(5L);
        when(snapRecorded.getCreated()).thenReturn(Instant.now());
        byte[] payloadBytes = "{\"data\":\"snap\"}".getBytes();
        byte[] metaBytes = ("{\"axon-message-id\":\"" + eventId
                + "\",\"axon-payload-type\":\"" + TestPayload.class.getName()
                + "\",\"axon-timestamp\":\"" + Instant.now()
                + "\",\"axon-aggregate-type\":\"Order\""
                + ",\"axon-aggregate-id\":\"" + aggregateId + "\""
                + ",\"axon-aggregate-seq\":5}").getBytes();
        when(snapRecorded.getEventData()).thenReturn(payloadBytes);
        when(snapRecorded.getUserMetadata()).thenReturn(metaBytes);

        ReadResult snapResult = mock(ReadResult.class);
        when(snapResult.getEvents()).thenReturn(List.of(snapEvent));

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("__snapshot-Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(snapResult));

        Optional<DomainEventMessage<?>> snapshot = engine.readSnapshot(aggregateId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getSequenceNumber()).isEqualTo(5L);
    }

    @Test
    void shouldReturnEmptySnapshotWhenStreamNotFound() throws Exception {
        // $all scan throws stream not found
        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("$all"));
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(notFoundFuture);

        Optional<DomainEventMessage<?>> snapshot = engine.readSnapshot("order-1");
        assertThat(snapshot).isEmpty();
    }

    @Test
    void shouldThrowOnSnapshotReadNonStreamNotFoundException() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("db error"));
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.readSnapshot("order-1"))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to read snapshot");
    }

    @Test
    void shouldReturnEmptySnapshotWhenSnapshotStreamEmpty() throws Exception {
        String aggregateId = "order-1";

        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        // Snapshot stream exists but is empty
        ReadResult emptySnapResult = mock(ReadResult.class);
        when(emptySnapResult.getEvents()).thenReturn(Collections.emptyList());

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("__snapshot-Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(emptySnapResult));

        Optional<DomainEventMessage<?>> snapshot = engine.readSnapshot(aggregateId);
        assertThat(snapshot).isEmpty();
    }

    @Test
    void shouldReturnEmptySnapshotWhenSnapshotStreamNotFound() throws Exception {
        String aggregateId = "order-1";

        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("__snapshot-Order-" + aggregateId));

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("__snapshot-Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        Optional<DomainEventMessage<?>> snapshot = engine.readSnapshot(aggregateId);
        assertThat(snapshot).isEmpty();
    }

    // ── lastSequenceNumberFor ───────────────────────────────────────────

    @Test
    void shouldReturnEmptyLastSequenceWhenAggregateNotFound() throws Exception {
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        Optional<Long> seq = engine.lastSequenceNumberFor("nonexistent-id");
        assertThat(seq).isEmpty();
    }

    @Test
    void shouldReturnLastSequenceNumberForKnownAggregate() throws Exception {
        String aggregateId = "order-1";

        // findStreamForAggregate
        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        // readStream for last event
        ResolvedEvent lastEvent = mock(ResolvedEvent.class);
        RecordedEvent lastRecorded = mock(RecordedEvent.class);
        when(lastEvent.getOriginalEvent()).thenReturn(lastRecorded);
        when(lastRecorded.getRevision()).thenReturn(7L);

        ReadResult streamResult = mock(ReadResult.class);
        when(streamResult.getEvents()).thenReturn(List.of(lastEvent));

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(streamResult));

        Optional<Long> seq = engine.lastSequenceNumberFor(aggregateId);
        assertThat(seq).isPresent().contains(7L);
    }

    @Test
    void shouldReturnEmptyLastSequenceOnStreamNotFound() throws Exception {
        String aggregateId = "order-1";

        ResolvedEvent allEvent = mock(ResolvedEvent.class);
        RecordedEvent allRecorded = mock(RecordedEvent.class);
        when(allEvent.getOriginalEvent()).thenReturn(allRecorded);
        when(allRecorded.getStreamId()).thenReturn("Order-" + aggregateId);

        ReadResult allResult = mock(ReadResult.class);
        when(allResult.getEvents()).thenReturn(List.of(allEvent));

        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("Order-" + aggregateId));

        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(allResult));
        when(client.readStream(eq("Order-" + aggregateId), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        Optional<Long> seq = engine.lastSequenceNumberFor(aggregateId);
        assertThat(seq).isEmpty();
    }

    // ── createTokenAt ───────────────────────────────────────────────────

    @Test
    void shouldCreateTokenAtTimestamp() throws Exception {
        Instant targetTime = Instant.parse("2026-02-09T10:00:00Z");

        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        Position position = new Position(200L, 200L);

        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);
        when(recordedEvent.getCreated()).thenReturn(Instant.parse("2026-02-09T10:00:01Z"));
        when(recordedEvent.getPosition()).thenReturn(position);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        TrackingToken token = engine.createTokenAt(targetTime);

        assertThat(token).isNotNull().isInstanceOf(EventStoreDBTrackingToken.class);
        EventStoreDBTrackingToken esdbToken = (EventStoreDBTrackingToken) token;
        assertThat(esdbToken.getCommitPosition()).isEqualTo(199L);
    }

    @Test
    void shouldCreateStartTokenWhenEventAtPositionZero() throws Exception {
        Instant targetTime = Instant.parse("2026-02-09T10:00:00Z");

        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        Position position = new Position(0L, 0L);

        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);
        when(recordedEvent.getCreated()).thenReturn(Instant.parse("2026-02-09T10:00:01Z"));
        when(recordedEvent.getPosition()).thenReturn(position);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        TrackingToken token = engine.createTokenAt(targetTime);

        assertThat(token).isNotNull().isInstanceOf(EventStoreDBTrackingToken.class);
        EventStoreDBTrackingToken esdbToken = (EventStoreDBTrackingToken) token;
        assertThat(esdbToken.getCommitPosition()).isEqualTo(-1L);
    }

    @Test
    void shouldReturnHeadTokenWhenNoEventsAtTimestamp() throws Exception {
        ReadResult emptyResult = mock(ReadResult.class);
        when(emptyResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readAll(any(ReadAllOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(emptyResult));

        TrackingToken token = engine.createTokenAt(Instant.now().plusSeconds(3600));
        assertThat(token).isNull();
    }

    @Test
    void shouldThrowOnCreateTokenAtFailure() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("db error"));
        when(client.readAll(any(ReadAllOptions.class))).thenReturn(failedFuture);

        assertThatThrownBy(() -> engine.createTokenAt(Instant.now()))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to create token at timestamp");
    }

    // ── Test payload ────────────────────────────────────────────────────

    static class TestPayload {
        public String data;

        TestPayload() {
        }

        TestPayload(String data) {
            this.data = data;
        }
    }
}
