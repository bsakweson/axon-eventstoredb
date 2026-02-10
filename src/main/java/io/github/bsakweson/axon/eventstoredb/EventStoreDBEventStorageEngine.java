package io.github.bsakweson.axon.eventstoredb;

import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.upcasting.EventStoreDBDomainEventData;
import io.github.bsakweson.axon.eventstoredb.upcasting.EventStoreDBTrackedDomainEventData;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
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
import com.eventstore.dbclient.StreamNotFoundException;
import com.eventstore.dbclient.WriteResult;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericTrackedDomainEventMessage;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Axon {@link EventStorageEngine} implementation backed by EventStoreDB (KurrentDB).
 *
 * <p>This engine stores aggregate events in per-aggregate streams following EventStoreDB's category
 * convention ({@code {AggregateType}-{AggregateId}}), enabling built-in category projections
 * ({@code $ce-{AggregateType}}) automatically.
 *
 * <p>Tracking processors read from the {@code $all} global stream, providing total ordering across
 * all aggregates. Snapshots are stored in dedicated streams ({@code __snapshot-{Type}-{Id}}).
 *
 * <p><b>Phase 2 features:</b>
 * <ul>
 *   <li><b>Event Upcasting</b> — optional {@link EventUpcaster} chain applied during read</li>
 *   <li><b>Connection Resilience</b> — configurable retry with exponential backoff via
 *       {@link EventStoreDBRetryExecutor}</li>
 *   <li><b>Micrometer Metrics</b> — optional instrumentation via {@link EventStoreDBMetrics}</li>
 * </ul>
 *
 * @see EventStoreDBTrackingToken
 * @see EventStoreDBStreamNaming
 */
public class EventStoreDBEventStorageEngine implements EventStorageEngine {

  private static final Logger log = LoggerFactory.getLogger(EventStoreDBEventStorageEngine.class);

  private final EventStoreDBClient client;
  private final EventStoreDBSerializer serializer;
  private final Serializer eventSerializer;
  private final EventStoreDBStreamNaming naming;
  private final int batchSize;
  @Nullable private final EventUpcaster upcasterChain;
  private final EventStoreDBRetryExecutor retryExecutor;
  @Nullable private final EventStoreDBMetrics metrics;

  /**
   * Creates a new EventStoreDB-backed event storage engine (backward-compatible constructor).
   *
   * @param client          the EventStoreDB gRPC client
   * @param eventSerializer the Axon serializer for event payloads
   * @param naming          stream naming strategy
   * @param batchSize       number of events to read per batch when tracking
   */
  public EventStoreDBEventStorageEngine(
      EventStoreDBClient client,
      Serializer eventSerializer,
      EventStoreDBStreamNaming naming,
      int batchSize) {
    this(client, eventSerializer, naming, batchSize, null, null, null);
  }

  /**
   * Creates a new EventStoreDB-backed event storage engine with Phase 2 features.
   *
   * @param client          the EventStoreDB gRPC client
   * @param eventSerializer the Axon serializer for event payloads
   * @param naming          stream naming strategy
   * @param batchSize       number of events to read per batch when tracking
   * @param upcasterChain   optional event upcaster chain (may be null)
   * @param retryExecutor   optional retry executor (may be null — defaults to no-retry)
   * @param metrics         optional Micrometer metrics (may be null)
   */
  public EventStoreDBEventStorageEngine(
      EventStoreDBClient client,
      Serializer eventSerializer,
      EventStoreDBStreamNaming naming,
      int batchSize,
      @Nullable EventUpcaster upcasterChain,
      @Nullable EventStoreDBRetryExecutor retryExecutor,
      @Nullable EventStoreDBMetrics metrics) {
    if (client == null) {
      throw new AxonConfigurationException("EventStoreDBClient must not be null");
    }
    if (eventSerializer == null) {
      throw new AxonConfigurationException("Event Serializer must not be null");
    }
    this.client = client;
    this.eventSerializer = eventSerializer;
    this.serializer = new EventStoreDBSerializer(eventSerializer);
    this.naming = naming != null ? naming : new EventStoreDBStreamNaming();
    this.batchSize = batchSize > 0 ? batchSize : 256;
    this.upcasterChain = upcasterChain;
    this.retryExecutor = retryExecutor != null
        ? retryExecutor : EventStoreDBRetryExecutor.noRetry();
    this.metrics = metrics;
  }

  // ────────────────────────────────────────────────────────────────────────
  // APPEND EVENTS
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void appendEvents(@Nonnull List<? extends EventMessage<?>> events) {
    if (events.isEmpty()) {
      return;
    }

    for (EventMessage<?> event : events) {
      if (event instanceof DomainEventMessage<?> domainEvent) {
        String streamName =
            naming.aggregateStream(
                domainEvent.getType(), domainEvent.getAggregateIdentifier());

        EventData eventData = serializer.serialize(domainEvent);

        try {
          AppendToStreamOptions options = AppendToStreamOptions.get();
          if (domainEvent.getSequenceNumber() == 0) {
            options.expectedRevision(ExpectedRevision.noStream());
          } else {
            options.expectedRevision(
                ExpectedRevision.expectedRevision(domainEvent.getSequenceNumber() - 1));
          }

          WriteResult result = retryExecutor.execute(
              () -> client.appendToStream(streamName, options, eventData).get(),
              "appendEvents");
          log.debug(
              "Appended event {} to stream '{}' at revision {}",
              domainEvent.getIdentifier(),
              streamName,
              result.getNextExpectedRevision().toRawLong());

          if (metrics != null) {
            metrics.recordEventsAppended(1);
          }

        } catch (ExecutionException e) {
          if (metrics != null) {
            metrics.recordError("append");
          }
          Throwable cause = e.getCause();
          if (cause instanceof WrongExpectedVersionException) {
            throw new EventStoreException(
                "Concurrent modification detected for aggregate '"
                    + domainEvent.getAggregateIdentifier()
                    + "'. Expected revision "
                    + (domainEvent.getSequenceNumber() - 1)
                    + " but stream has moved ahead.",
                cause);
          }
          throw new EventStoreException("Failed to append events to EventStoreDB", cause);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new EventStoreException("Interrupted while appending events to EventStoreDB", e);
        }
      } else {
        log.warn(
            "Non-domain event received in appendEvents: {}. "
                + "EventStoreDB engine requires DomainEventMessage for stream routing.",
            event.getPayloadType().getSimpleName());
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // STORE / READ SNAPSHOTS
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void storeSnapshot(@Nonnull DomainEventMessage<?> snapshot) {
    String streamName =
        naming.snapshotStream(snapshot.getType(), snapshot.getAggregateIdentifier());

    EventData eventData = serializer.serialize(snapshot);

    try {
      AppendToStreamOptions options =
          AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any());
      retryExecutor.execute(
          () -> client.appendToStream(streamName, options, eventData).get(),
          "storeSnapshot");

      log.debug(
          "Stored snapshot for aggregate {}/{} at sequence {}",
          snapshot.getType(),
          snapshot.getAggregateIdentifier(),
          snapshot.getSequenceNumber());

      if (metrics != null) {
        metrics.recordSnapshotStored();
      }

    } catch (ExecutionException e) {
      if (metrics != null) {
        metrics.recordError("storeSnapshot");
      }
      throw new EventStoreException("Failed to store snapshot in EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while storing snapshot", e);
    }
  }

  @Override
  public Optional<DomainEventMessage<?>> readSnapshot(@Nonnull String aggregateIdentifier) {
    return findSnapshotForAggregate(aggregateIdentifier);
  }

  private Optional<DomainEventMessage<?>> findSnapshotForAggregate(String aggregateIdentifier) {
    try {
      ReadResult allResult =
          retryExecutor.execute(
              () -> client.readAll(
                  ReadAllOptions.get()
                      .forwards()
                      .fromStart()
                      .maxCount(batchSize)).get(),
              "findSnapshotForAggregate");

      String aggregateType = null;
      for (ResolvedEvent resolved : allResult.getEvents()) {
        RecordedEvent recorded = resolved.getOriginalEvent();
        if (recorded.getStreamId().contains(aggregateIdentifier)) {
          aggregateType = naming.extractAggregateType(recorded.getStreamId());
          break;
        }
      }

      if (aggregateType == null) {
        return Optional.empty();
      }

      String snapshotStream = naming.snapshotStream(aggregateType, aggregateIdentifier);
      return readLatestSnapshot(snapshotStream);

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return Optional.empty();
      }
      throw new EventStoreException("Failed to read snapshot from EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading snapshot", e);
    }
  }

  private Optional<DomainEventMessage<?>> readLatestSnapshot(String snapshotStream) {
    try {
      ReadResult result =
          retryExecutor.execute(
              () -> client.readStream(
                  snapshotStream,
                  ReadStreamOptions.get().backwards().fromEnd().maxCount(1)).get(),
              "readLatestSnapshot");

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return Optional.empty();
      }

      DomainEventMessage<?> snapshot = serializer.deserialize(events.get(0));
      if (metrics != null) {
        metrics.recordSnapshotRead();
      }
      return Optional.of(snapshot);

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return Optional.empty();
      }
      throw new EventStoreException(
          "Failed to read snapshot stream '" + snapshotStream + "'", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading snapshot", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // READ EVENTS (for aggregate event sourcing)
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public DomainEventStream readEvents(@Nonnull String aggregateIdentifier, long firstSequenceNumber) {
    String streamName = findStreamForAggregate(aggregateIdentifier);
    if (streamName == null) {
      return DomainEventStream.of(Collections.<DomainEventMessage<?>>emptyList());
    }

    try {
      ReadStreamOptions options =
          ReadStreamOptions.get()
              .forwards()
              .fromRevision(firstSequenceNumber)
              .maxCount(Long.MAX_VALUE);

      ReadResult result = retryExecutor.execute(
          () -> client.readStream(streamName, options).get(),
          "readEvents");
      List<ResolvedEvent> resolvedEvents = result.getEvents();

      List<? extends DomainEventMessage<?>> domainEvents;
      if (upcasterChain != null) {
        domainEvents = upcastAndDeserializeDomain(resolvedEvents);
      } else {
        List<DomainEventMessage<?>> directEvents =
            new ArrayList<>(resolvedEvents.size());
        for (ResolvedEvent resolved : resolvedEvents) {
          directEvents.add(serializer.deserialize(resolved));
        }
        domainEvents = directEvents;
      }

      if (metrics != null) {
        metrics.recordEventsRead(domainEvents.size());
      }
      return DomainEventStream.of(domainEvents);

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return DomainEventStream.of(Collections.<DomainEventMessage<?>>emptyList());
      }
      throw new EventStoreException(
          "Failed to read events for aggregate '" + aggregateIdentifier + "'", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading aggregate events", e);
    }
  }

  @Override
  public DomainEventStream readEvents(@Nonnull String aggregateIdentifier) {
    return readEvents(aggregateIdentifier, 0);
  }

  /**
   * Finds the EventStoreDB stream name for a given aggregate identifier.
   */
  private String findStreamForAggregate(String aggregateIdentifier) {
    try {
      ReadResult allResult =
          retryExecutor.execute(
              () -> client.readAll(
                  ReadAllOptions.get().forwards().fromStart().maxCount(4096)).get(),
              "findStreamForAggregate");

      for (ResolvedEvent resolved : allResult.getEvents()) {
        RecordedEvent recorded = resolved.getOriginalEvent();
        String streamId = recorded.getStreamId();
        if (!streamId.startsWith("$")
            && !naming.isSystemStream(streamId)
            && streamId.endsWith(aggregateIdentifier)) {
          return streamId;
        }
      }
      return null;
    } catch (Exception e) {
      log.warn("Failed to find stream for aggregate {}", aggregateIdentifier, e);
      return null;
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // READ EVENTS (for tracking processors — global stream)
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public Stream<? extends TrackedEventMessage<?>> readEvents(
      @Nullable TrackingToken trackingToken, boolean mayBlock) {
    ReadAllOptions options = ReadAllOptions.get().forwards().maxCount(batchSize);

    if (trackingToken == null) {
      options.fromStart();
    } else if (trackingToken instanceof EventStoreDBTrackingToken esdbToken) {
      options.fromPosition(
          new Position(esdbToken.getCommitPosition(), esdbToken.getPreparePosition()));
    } else if (trackingToken instanceof GlobalSequenceTrackingToken globalToken) {
      log.warn(
          "Received GlobalSequenceTrackingToken instead of EventStoreDBTrackingToken. "
              + "Starting from beginning of $all stream. Position: {}",
          globalToken.getGlobalIndex());
      options.fromStart();
    } else {
      log.warn("Unknown tracking token type: {}. Starting from beginning.",
          trackingToken.getClass());
      options.fromStart();
    }

    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readAll(options).get(),
          "readTrackedEvents");
      List<ResolvedEvent> resolvedEvents = result.getEvents();

      List<TrackedEventMessage<?>> trackedMessages;
      if (upcasterChain != null) {
        trackedMessages = upcastAndDeserializeTracked(resolvedEvents);
      } else {
        trackedMessages = deserializeTrackedDirect(resolvedEvents);
      }

      if (metrics != null) {
        metrics.recordEventsRead(trackedMessages.size());
      }
      return trackedMessages.stream();

    } catch (ExecutionException e) {
      if (metrics != null) {
        metrics.recordError("readTrackedEvents");
      }
      throw new EventStoreException(
          "Failed to read tracked events from EventStoreDB $all stream", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading tracked events", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // UPCASTING HELPERS
  // ────────────────────────────────────────────────────────────────────────

  /**
   * Upcasts and deserializes domain events using the configured upcaster chain.
   */
  private List<DomainEventMessage<?>> upcastAndDeserializeDomain(
      List<ResolvedEvent> resolvedEvents) {
    Stream<IntermediateEventRepresentation> representations = resolvedEvents.stream()
        .map(EventStoreDBDomainEventData::new)
        .map(data -> new InitialEventRepresentation(data, eventSerializer));

    Stream<IntermediateEventRepresentation> upcasted =
        upcasterChain.upcast(representations);

    return upcasted.map(this::toGenericDomainEventMessage)
        .collect(Collectors.toList());
  }

  /**
   * Upcasts and deserializes tracked events using the configured upcaster chain.
   * Filters out system and internal streams before upcasting.
   */
  private List<TrackedEventMessage<?>> upcastAndDeserializeTracked(
      List<ResolvedEvent> resolvedEvents) {
    Stream<IntermediateEventRepresentation> representations = resolvedEvents.stream()
        .filter(resolved -> {
          String streamId = resolved.getOriginalEvent().getStreamId();
          return !streamId.startsWith("$") && !naming.isSystemStream(streamId);
        })
        .map(resolved -> {
          EventStoreDBTrackedDomainEventData trackedData =
              new EventStoreDBTrackedDomainEventData(resolved);
          return new InitialEventRepresentation(trackedData, eventSerializer);
        });

    Stream<IntermediateEventRepresentation> upcasted =
        upcasterChain.upcast(representations);

    return upcasted.map(rep -> {
      DomainEventMessage<?> domainEvent = toGenericDomainEventMessage(rep);
      TrackingToken token = rep.getTrackingToken().orElse(null);
      return (TrackedEventMessage<?>) new GenericTrackedDomainEventMessage<>(
          token, domainEvent);
    }).collect(Collectors.toList());
  }

  /**
   * Direct deserialization of tracked events without upcasting (original path).
   */
  private List<TrackedEventMessage<?>> deserializeTrackedDirect(
      List<ResolvedEvent> resolvedEvents) {
    List<TrackedEventMessage<?>> trackedMessages = new ArrayList<>();
    for (ResolvedEvent resolved : resolvedEvents) {
      RecordedEvent recorded = resolved.getOriginalEvent();
      String streamId = recorded.getStreamId();

      if (streamId.startsWith("$") || naming.isSystemStream(streamId)) {
        continue;
      }

      DomainEventMessage<?> domainEvent = serializer.deserialize(resolved);
      Position position = resolved.getOriginalEvent().getPosition();
      EventStoreDBTrackingToken token =
          EventStoreDBTrackingToken.of(
              position.getCommitUnsigned(), position.getPrepareUnsigned());

      TrackedEventMessage<?> tracked =
          new GenericTrackedDomainEventMessage<>(token, domainEvent);
      trackedMessages.add(tracked);
    }
    return trackedMessages;
  }

  /**
   * Converts an upcasted intermediate representation into a domain event message.
   */
  @SuppressWarnings("unchecked")
  private DomainEventMessage<?> toGenericDomainEventMessage(
      IntermediateEventRepresentation rep) {
    SerializedObject<byte[]> serializedPayload = rep.getData(byte[].class);
    Object payload = eventSerializer.deserialize(serializedPayload);

    MetaData metaData;
    try {
      LazyDeserializingObject<MetaData> lazyMeta = rep.getMetaData();
      metaData = lazyMeta != null ? lazyMeta.getObject() : MetaData.emptyInstance();
    } catch (Exception e) {
      log.debug("Failed to deserialize metadata, using empty", e);
      metaData = MetaData.emptyInstance();
    }

    return new GenericDomainEventMessage<>(
        rep.getAggregateType().orElse(""),
        rep.getAggregateIdentifier().orElse(""),
        rep.getSequenceNumber().orElse(0L),
        payload,
        metaData,
        rep.getMessageIdentifier(),
        rep.getTimestamp());
  }

  // ────────────────────────────────────────────────────────────────────────
  // TOKEN CREATION
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public TrackingToken createTailToken() {
    return null;
  }

  @Override
  public TrackingToken createHeadToken() {
    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readAll(
              ReadAllOptions.get().backwards().fromEnd().maxCount(1)).get(),
          "createHeadToken");

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return null;
      }

      Position position = events.get(0).getOriginalEvent().getPosition();
      return EventStoreDBTrackingToken.of(
          position.getCommitUnsigned(), position.getPrepareUnsigned());

    } catch (ExecutionException e) {
      throw new EventStoreException("Failed to read head token from EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading head token", e);
    }
  }

  @Override
  public TrackingToken createTokenAt(@Nonnull Instant dateTime) {
    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readAll(
              ReadAllOptions.get().forwards().fromStart().maxCount(4096)).get(),
          "createTokenAt");

      for (ResolvedEvent resolved : result.getEvents()) {
        RecordedEvent recorded = resolved.getOriginalEvent();
        Instant eventTime = recorded.getCreated();
        if (!eventTime.isBefore(dateTime)) {
          Position position = recorded.getPosition();
          long commitPos = position.getCommitUnsigned();
          long preparePos = position.getPrepareUnsigned();
          return commitPos > 0
              ? EventStoreDBTrackingToken.of(commitPos - 1, preparePos - 1)
              : EventStoreDBTrackingToken.start();
        }
      }

      return createHeadToken();

    } catch (ExecutionException e) {
      throw new EventStoreException("Failed to create token at timestamp", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while creating token at timestamp", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // LAST SEQUENCE NUMBER
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public Optional<Long> lastSequenceNumberFor(@Nonnull String aggregateIdentifier) {
    String streamName = findStreamForAggregate(aggregateIdentifier);
    if (streamName == null) {
      return Optional.empty();
    }

    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readStream(
              streamName,
              ReadStreamOptions.get().backwards().fromEnd().maxCount(1)).get(),
          "lastSequenceNumberFor");

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return Optional.empty();
      }

      long revision = events.get(0).getOriginalEvent().getRevision();
      return Optional.of(revision);

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return Optional.empty();
      }
      throw new EventStoreException(
          "Failed to read last sequence number for '" + aggregateIdentifier + "'", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading last sequence number", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // UTILITIES
  // ────────────────────────────────────────────────────────────────────────

  private boolean isStreamNotFound(ExecutionException e) {
    Throwable cause = e.getCause();
    return cause instanceof StreamNotFoundException
        || (cause != null
            && cause.getMessage() != null
            && cause.getMessage().contains("not found"));
  }
}
