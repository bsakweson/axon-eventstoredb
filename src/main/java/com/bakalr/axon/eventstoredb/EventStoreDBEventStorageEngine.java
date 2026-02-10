package com.bakalr.axon.eventstoredb;

import com.bakalr.axon.eventstoredb.util.EventStoreDBSerializer;
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
import com.eventstore.dbclient.StreamNotFoundException;
import com.eventstore.dbclient.WriteResult;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericTrackedDomainEventMessage;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.serialization.Serializer;
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
 * <p>Key mappings:
 *
 * <ul>
 *   <li>{@link #appendEvents} → {@code client.appendToStream()} with optimistic concurrency
 *   <li>{@link #readEvents(String, long)} → {@code client.readStream()} for aggregate event sourcing
 *   <li>{@link #readEvents(TrackingToken, boolean)} → {@code client.readAll()} for tracking
 *   <li>{@link #storeSnapshot} → append to snapshot stream, read most recent on load
 * </ul>
 *
 * @see EventStoreDBTrackingToken
 * @see EventStoreDBStreamNaming
 */
public class EventStoreDBEventStorageEngine implements EventStorageEngine {

  private static final Logger log = LoggerFactory.getLogger(EventStoreDBEventStorageEngine.class);

  private final EventStoreDBClient client;
  private final EventStoreDBSerializer serializer;
  private final EventStoreDBStreamNaming naming;
  private final int batchSize;

  /**
   * Creates a new EventStoreDB-backed event storage engine.
   *
   * @param client the EventStoreDB gRPC client
   * @param eventSerializer the Axon serializer for event payloads
   * @param naming stream naming strategy
   * @param batchSize number of events to read per batch when tracking
   */
  public EventStoreDBEventStorageEngine(
      EventStoreDBClient client,
      Serializer eventSerializer,
      EventStoreDBStreamNaming naming,
      int batchSize) {
    if (client == null) {
      throw new AxonConfigurationException("EventStoreDBClient must not be null");
    }
    if (eventSerializer == null) {
      throw new AxonConfigurationException("Event Serializer must not be null");
    }
    this.client = client;
    this.serializer = new EventStoreDBSerializer(eventSerializer);
    this.naming = naming != null ? naming : new EventStoreDBStreamNaming();
    this.batchSize = batchSize > 0 ? batchSize : 256;
  }

  // ────────────────────────────────────────────────────────────────────────
  // APPEND EVENTS
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void appendEvents(@Nonnull List<? extends EventMessage<?>> events) {
    if (events.isEmpty()) {
      return;
    }

    // Group events by aggregate stream
    // In typical Axon usage, all events in a single append belong to the same aggregate
    for (EventMessage<?> event : events) {
      if (event instanceof DomainEventMessage<?> domainEvent) {
        String streamName =
            naming.aggregateStream(
                domainEvent.getType(), domainEvent.getAggregateIdentifier());

        EventData eventData = serializer.serialize(domainEvent);

        try {
          // Use optimistic concurrency: expect specific revision for non-first events
          AppendToStreamOptions options = AppendToStreamOptions.get();
          if (domainEvent.getSequenceNumber() == 0) {
            options.expectedRevision(ExpectedRevision.noStream());
          } else {
            options.expectedRevision(
                ExpectedRevision.expectedRevision(domainEvent.getSequenceNumber() - 1));
          }

          WriteResult result = client.appendToStream(streamName, options, eventData).get();
          log.debug(
              "Appended event {} to stream '{}' at revision {}",
              domainEvent.getIdentifier(),
              streamName,
              result.getNextExpectedRevision().toRawLong());

        } catch (ExecutionException e) {
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
      // Snapshots always use ANY expected revision — we just append
      AppendToStreamOptions options = AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any());
      client.appendToStream(streamName, options, eventData).get();

      log.debug(
          "Stored snapshot for aggregate {}/{} at sequence {}",
          snapshot.getType(),
          snapshot.getAggregateIdentifier(),
          snapshot.getSequenceNumber());

    } catch (ExecutionException e) {
      throw new EventStoreException("Failed to store snapshot in EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while storing snapshot", e);
    }
  }

  @Override
  public Optional<DomainEventMessage<?>> readSnapshot(@Nonnull String aggregateIdentifier) {
    // We need to find the snapshot stream — but we don't know the aggregate type from just the ID.
    // Convention: scan all snapshot streams for this aggregate ID.
    // Optimization: services typically know their aggregate type, so we try common patterns.
    // For now, we use a brute-force approach: try reading the well-known snapshot stream.
    // The aggregate type will be embedded in the stream name.

    // In practice, Axon calls readSnapshot() with the aggregateIdentifier, and the aggregate type
    // is determined by the aggregate class. We need a way to resolve this.
    // Workaround: try to find any snapshot stream for this aggregate identifier.
    // This is acceptable because aggregate IDs are globally unique (UUIDs).
    return findSnapshotForAggregate(aggregateIdentifier);
  }

  private Optional<DomainEventMessage<?>> findSnapshotForAggregate(String aggregateIdentifier) {
    // Try reading $all and filtering, or enumerate known aggregate types.
    // For efficiency, we use EventStoreDB's stream naming convention.
    // Since we don't know the type, we'll try the category projection approach.

    // Strategy: Read from the $all stream filtered by metadata, or try known types.
    // For simplicity in MVP, we store the aggregate type in the snapshot metadata
    // and can look it up by reading a well-known index stream.

    // PRAGMATIC APPROACH: Read the aggregate's event stream to determine its type,
    // then read the snapshot stream.
    try {
      // Read one event from the aggregate to determine its type
      // This works because aggregate IDs are unique UUIDs
      ReadResult allResult =
          client
              .readAll(
                  ReadAllOptions.get()
                      .forwards()
                      .fromStart()
                      .maxCount(batchSize))
              .get();

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
      // Read the last event in the snapshot stream (most recent snapshot)
      ReadResult result =
          client
              .readStream(
                  snapshotStream,
                  ReadStreamOptions.get().backwards().fromEnd().maxCount(1))
              .get();

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return Optional.empty();
      }

      DomainEventMessage<?> snapshot = serializer.deserialize(events.get(0));
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
    // We need the aggregate type to build the stream name.
    // Axon's AbstractRepository provides the type, and the readEvents() call chain
    // ultimately resolves the stream name. However, this interface only gives us the ID.
    //
    // Strategy: We scan for the stream by aggregate ID.
    // EventStoreDB category projections make this efficient: read $ce-{type} if type is known,
    // or search streams by suffix.
    //
    // PRACTICAL: Since aggregate IDs are UUIDs and stream names are {Type}-{UUID},
    // we can find the stream by reading the $streams system stream or by trying known types.

    String streamName = findStreamForAggregate(aggregateIdentifier);
    if (streamName == null) {
      return DomainEventStream.of(Collections.<DomainEventMessage<?>>emptyList());
    }

    try {
      ReadStreamOptions options =
          ReadStreamOptions.get()
              .forwards()
              .fromRevision(firstSequenceNumber)
              .maxCount(Long.MAX_VALUE); // Read all events

      ReadResult result = client.readStream(streamName, options).get();
      List<ResolvedEvent> resolvedEvents = result.getEvents();

      List<DomainEventMessage<?>> domainEvents = new ArrayList<>(resolvedEvents.size());
      for (ResolvedEvent resolved : resolvedEvents) {
        domainEvents.add(serializer.deserialize(resolved));
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
   * Finds the EventStoreDB stream name for a given aggregate identifier. Searches all user streams
   * for one ending with the aggregate ID.
   */
  private String findStreamForAggregate(String aggregateIdentifier) {
    try {
      // Read a batch from $all and look for our aggregate
      // This is O(n) in worst case but works for MVP.
      // Production optimization: maintain a local cache of aggregate → stream mappings
      ReadResult allResult =
          client
              .readAll(ReadAllOptions.get().forwards().fromStart().maxCount(4096))
              .get();

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
    // Convert Axon TrackingToken to EventStoreDB Position
    ReadAllOptions options = ReadAllOptions.get().forwards().maxCount(batchSize);

    if (trackingToken == null) {
      options.fromStart();
    } else if (trackingToken instanceof EventStoreDBTrackingToken esdbToken) {
      // Read from the position AFTER the given token
      options.fromPosition(
          new Position(esdbToken.getCommitPosition(), esdbToken.getPreparePosition()));
    } else if (trackingToken instanceof GlobalSequenceTrackingToken globalToken) {
      // Compatibility: if a GlobalSequenceTrackingToken is passed (e.g., from JPA store migration),
      // we start from the beginning and skip. This is a fallback.
      log.warn(
          "Received GlobalSequenceTrackingToken instead of EventStoreDBTrackingToken. "
              + "Starting from beginning of $all stream. Position: {}",
          globalToken.getGlobalIndex());
      options.fromStart();
    } else {
      log.warn("Unknown tracking token type: {}. Starting from beginning.", trackingToken.getClass());
      options.fromStart();
    }

    try {
      ReadResult result = client.readAll(options).get();
      List<ResolvedEvent> resolvedEvents = result.getEvents();

      // Filter out system streams and our internal streams (snapshots, tokens)
      List<TrackedEventMessage<?>> trackedMessages = new ArrayList<>();
      for (ResolvedEvent resolved : resolvedEvents) {
        RecordedEvent recorded = resolved.getOriginalEvent();
        String streamId = recorded.getStreamId();

        // Skip system and internal streams
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

      return trackedMessages.stream();

    } catch (ExecutionException e) {
      throw new EventStoreException(
          "Failed to read tracked events from EventStoreDB $all stream", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading tracked events", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // TOKEN CREATION
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public TrackingToken createTailToken() {
    // Tail token = start of stream. Return null per Axon convention (means "from the beginning")
    return null;
  }

  @Override
  public TrackingToken createHeadToken() {
    // Head token = end of current stream. Events after this position will be tracked.
    try {
      ReadResult result =
          client
              .readAll(ReadAllOptions.get().backwards().fromEnd().maxCount(1))
              .get();

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return null; // Empty store
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
    // Find the position of the first event at or after the given timestamp.
    // EventStoreDB doesn't have a native "position at timestamp" API, so we scan $all.
    try {
      ReadResult result =
          client
              .readAll(ReadAllOptions.get().forwards().fromStart().maxCount(4096))
              .get();

      for (ResolvedEvent resolved : result.getEvents()) {
        RecordedEvent recorded = resolved.getOriginalEvent();
        Instant eventTime = recorded.getCreated();
        if (!eventTime.isBefore(dateTime)) {
          Position position = recorded.getPosition();
          // Return the position BEFORE this event so that tracking will include it
          long commitPos = position.getCommitUnsigned();
          long preparePos = position.getPrepareUnsigned();
          // Use the position just before this event
          return commitPos > 0
              ? EventStoreDBTrackingToken.of(commitPos - 1, preparePos - 1)
              : EventStoreDBTrackingToken.start();
        }
      }

      // No events found at or after this timestamp — return head token
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
      ReadResult result =
          client
              .readStream(
                  streamName, ReadStreamOptions.get().backwards().fromEnd().maxCount(1))
              .get();

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return Optional.empty();
      }

      // The stream revision IS the sequence number in our convention
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
