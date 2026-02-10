package io.github.bsakweson.axon.eventstoredb;

import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.eventstore.dbclient.WrongExpectedVersionException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.annotation.Nullable;

import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TokenStore} implementation that persists tracking processor tokens in EventStoreDB streams.
 *
 * <p>Each processor's token state is stored in a dedicated stream named
 * {@code __axon-tokens-{processorName}-{segment}}. Token updates are appended as new events, and
 * the current token is read by fetching the last event in that stream.
 *
 * <p><b>Phase 2 features:</b>
 * <ul>
 *   <li><b>Connection Resilience</b> — configurable retry with exponential backoff</li>
 *   <li><b>Micrometer Metrics</b> — optional instrumentation of token operations</li>
 * </ul>
 */
public class EventStoreDBTokenStore implements TokenStore {

  private static final Logger log = LoggerFactory.getLogger(EventStoreDBTokenStore.class);
  private static final String TOKEN_EVENT_TYPE = "AxonTrackingTokenUpdate";
  private static final String INIT_EVENT_TYPE = "AxonTokenSegmentInitialized";

  private final EventStoreDBClient client;
  private final EventStoreDBStreamNaming naming;
  private final ObjectMapper objectMapper;
  private final String nodeId;
  private final EventStoreDBRetryExecutor retryExecutor;
  @Nullable private final EventStoreDBMetrics metrics;

  /**
   * Creates a new token store (backward-compatible constructor).
   *
   * @param client the EventStoreDB gRPC client
   * @param naming stream naming strategy
   */
  public EventStoreDBTokenStore(EventStoreDBClient client, EventStoreDBStreamNaming naming) {
    this(client, naming, UUID.randomUUID().toString(), null, null);
  }

  /**
   * Creates a new token store with a specific node ID.
   *
   * @param client the EventStoreDB gRPC client
   * @param naming stream naming strategy
   * @param nodeId unique node identifier for claim management
   */
  public EventStoreDBTokenStore(
      EventStoreDBClient client, EventStoreDBStreamNaming naming, String nodeId) {
    this(client, naming, nodeId, null, null);
  }

  /**
   * Creates a new token store with Phase 2 features.
   *
   * @param client        the EventStoreDB gRPC client
   * @param naming        stream naming strategy
   * @param nodeId        unique node identifier for claim management
   * @param retryExecutor optional retry executor (may be null)
   * @param metrics       optional Micrometer metrics (may be null)
   */
  public EventStoreDBTokenStore(
      EventStoreDBClient client,
      EventStoreDBStreamNaming naming,
      String nodeId,
      @Nullable EventStoreDBRetryExecutor retryExecutor,
      @Nullable EventStoreDBMetrics metrics) {
    this.client = client;
    this.naming = naming != null ? naming : new EventStoreDBStreamNaming();
    this.nodeId = nodeId;
    this.retryExecutor = retryExecutor != null
        ? retryExecutor : EventStoreDBRetryExecutor.noRetry();
    this.metrics = metrics;
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
    this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    this.objectMapper.registerSubtypes(EventStoreDBTrackingToken.class);
  }

  // ────────────────────────────────────────────────────────────────────────
  // TOKEN OPERATIONS
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void storeToken(TrackingToken token, String processorName, int segment)
      throws UnableToClaimTokenException {
    String streamName = tokenStreamName(processorName, segment);

    TokenEntry entry =
        new TokenEntry(processorName, segment, token, nodeId, Instant.now());

    try {
      byte[] data = objectMapper.writeValueAsBytes(entry);
      EventData eventData =
          EventDataBuilder.json(UUID.randomUUID(), TOKEN_EVENT_TYPE, data).build();

      retryExecutor.execute(
          () -> client.appendToStream(
              streamName,
              AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any()),
              eventData).get(),
          "storeToken");

      log.trace("Stored token for processor '{}' segment {}: {}",
          processorName, segment, token);

      if (metrics != null) {
        metrics.recordTokenOperation("store");
      }

    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize tracking token", e);
    } catch (ExecutionException e) {
      if (metrics != null) {
        metrics.recordError("storeToken");
      }
      throw new EventStoreException("Failed to store token in EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while storing token", e);
    }
  }

  @Override
  public TrackingToken fetchToken(String processorName, int segment)
      throws UnableToClaimTokenException {
    String streamName = tokenStreamName(processorName, segment);

    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readStream(
              streamName,
              ReadStreamOptions.get().backwards().fromEnd().maxCount(1)).get(),
          "fetchToken");

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return null;
      }

      RecordedEvent recorded = events.get(0).getOriginalEvent();
      TokenEntry entry = objectMapper.readValue(recorded.getEventData(), TokenEntry.class);

      if (metrics != null) {
        metrics.recordTokenOperation("fetch");
      }
      return entry.token();

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return null;
      }
      if (metrics != null) {
        metrics.recordError("fetchToken");
      }
      throw new EventStoreException("Failed to fetch token from EventStoreDB", e.getCause());
    } catch (IOException e) {
      throw new EventStoreException("Failed to deserialize tracking token", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while fetching token", e);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // SEGMENT MANAGEMENT
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void initializeTokenSegments(String processorName, int segmentCount)
      throws UnableToClaimTokenException {
    initializeTokenSegments(processorName, segmentCount, null);
  }

  @Override
  public void initializeTokenSegments(
      String processorName, int segmentCount, TrackingToken initialToken)
      throws UnableToClaimTokenException {
    for (int i = 0; i < segmentCount; i++) {
      String streamName = tokenStreamName(processorName, i);

      try {
        ReadResult result = retryExecutor.execute(
            () -> client.readStream(
                streamName,
                ReadStreamOptions.get().forwards().fromStart().maxCount(1)).get(),
            "checkTokenSegment");
        if (!result.getEvents().isEmpty()) {
          log.debug("Token segment {}/{} already initialized, skipping",
              processorName, i);
          continue;
        }
      } catch (ExecutionException e) {
        if (!isStreamNotFound(e)) {
          throw new EventStoreException(
              "Failed to check token segment existence", e.getCause());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new EventStoreException("Interrupted during token initialization", e);
      }

      TokenEntry entry =
          new TokenEntry(processorName, i, initialToken, nodeId, Instant.now());

      try {
        byte[] data = objectMapper.writeValueAsBytes(entry);
        EventData eventData =
            EventDataBuilder.json(UUID.randomUUID(), INIT_EVENT_TYPE, data).build();

        retryExecutor.execute(
            () -> client.appendToStream(
                streamName,
                AppendToStreamOptions.get().expectedRevision(ExpectedRevision.noStream()),
                eventData).get(),
            "initializeTokenSegment");

        log.info("Initialized token segment {}/{} with token: {}",
            processorName, i, initialToken);

      } catch (ExecutionException e) {
        if (e.getCause() instanceof WrongExpectedVersionException) {
          log.debug("Token segment {}/{} already initialized by another node",
              processorName, i);
          continue;
        }
        throw new EventStoreException("Failed to initialize token segment", e.getCause());
      } catch (JsonProcessingException e) {
        throw new EventStoreException("Failed to serialize initial token", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new EventStoreException("Interrupted during token initialization", e);
      }
    }
  }

  @Override
  public int[] fetchSegments(String processorName) {
    int maxSegments = 64;
    int[] segments = new int[maxSegments];
    int count = 0;

    for (int i = 0; i < maxSegments; i++) {
      String streamName = tokenStreamName(processorName, i);
      try {
        ReadResult result = retryExecutor.execute(
            () -> client.readStream(
                streamName,
                ReadStreamOptions.get().forwards().fromStart().maxCount(1)).get(),
            "fetchSegments");
        if (!result.getEvents().isEmpty()) {
          segments[count++] = i;
        }
      } catch (ExecutionException e) {
        if (isStreamNotFound(e)) {
          if (i == 0) {
            break;
          }
          continue;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    return Arrays.copyOf(segments, count);
  }

  // ────────────────────────────────────────────────────────────────────────
  // CLAIM MANAGEMENT
  // ────────────────────────────────────────────────────────────────────────

  @Override
  public void extendClaim(String processorName, int segment)
      throws UnableToClaimTokenException {
    TrackingToken current = fetchToken(processorName, segment);
    if (current != null) {
      storeToken(current, processorName, segment);
    }
  }

  @Override
  public void releaseClaim(String processorName, int segment) {
    log.trace("Released claim for processor '{}' segment {}", processorName, segment);
  }

  @Override
  public void deleteToken(String processorName, int segment)
      throws UnableToClaimTokenException {
    String streamName = tokenStreamName(processorName, segment);
    try {
      retryExecutor.execute(
          () -> {
            client.deleteStream(streamName, DeleteStreamOptions.get()).get();
            return null;
          },
          "deleteToken");
      log.info("Deleted token stream for processor '{}' segment {}",
          processorName, segment);

      if (metrics != null) {
        metrics.recordTokenOperation("delete");
      }
    } catch (ExecutionException e) {
      if (!isStreamNotFound(e)) {
        throw new EventStoreException("Failed to delete token stream", e.getCause());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while deleting token", e);
    }
  }

  @Override
  public boolean requiresExplicitSegmentInitialization() {
    return true;
  }

  @Override
  public Optional<String> retrieveStorageIdentifier() {
    return Optional.of("eventstoredb-" + nodeId);
  }

  // ────────────────────────────────────────────────────────────────────────
  // HELPERS
  // ────────────────────────────────────────────────────────────────────────

  private String tokenStreamName(String processorName, int segment) {
    return naming.tokenStream(processorName) + "-" + segment;
  }

  private boolean isStreamNotFound(ExecutionException e) {
    Throwable cause = e.getCause();
    return cause instanceof StreamNotFoundException
        || (cause != null
            && cause.getMessage() != null
            && cause.getMessage().contains("not found"));
  }

  /**
   * Internal record stored in EventStoreDB for each token update.
   */
  record TokenEntry(
      @JsonProperty("processorName") String processorName,
      @JsonProperty("segmentId") int segmentId,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
      @JsonProperty("token") TrackingToken token,
      @JsonProperty("owner") String owner,
      @JsonProperty("timestamp") Instant timestamp) {

    @JsonCreator
    TokenEntry {}
  }
}
