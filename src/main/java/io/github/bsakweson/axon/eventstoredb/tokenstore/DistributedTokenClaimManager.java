package io.github.bsakweson.axon.eventstoredb.tokenstore;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBTokenStore;
import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.annotation.Nullable;

import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed token claim manager for multi-node Axon deployments.
 *
 * <p>Extends the basic {@link EventStoreDBTokenStore} with proper distributed claim
 * semantics using EventStoreDB's optimistic concurrency (expected revision). This ensures
 * that only one node can hold the claim for a given processor segment at a time.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li><b>Claim acquisition</b> — uses optimistic concurrency to ensure only one node
 *       wins when multiple nodes try to claim the same segment</li>
 *   <li><b>Claim expiry</b> — claims expire after a configurable timeout, allowing
 *       other nodes to take over if a node crashes without releasing</li>
 *   <li><b>Claim extension</b> — active processors periodically extend their claims
 *       to prevent expiry while still processing</li>
 *   <li><b>Claim release</b> — explicitly records release so other nodes can claim
 *       immediately without waiting for timeout</li>
 * </ul>
 *
 * <p>All claim state is stored in EventStoreDB streams, making it fully distributed
 * without requiring any additional infrastructure (no ZooKeeper, no database).
 */
public class DistributedTokenClaimManager {

  private static final Logger log =
      LoggerFactory.getLogger(DistributedTokenClaimManager.class);

  private static final String CLAIM_EVENT_TYPE = "AxonTokenClaim";
  private static final String RELEASE_EVENT_TYPE = "AxonTokenRelease";

  private final EventStoreDBClient client;
  private final EventStoreDBStreamNaming naming;
  private final ObjectMapper objectMapper;
  private final String nodeId;
  private final Duration claimTimeout;
  private final EventStoreDBRetryExecutor retryExecutor;
  @Nullable private final EventStoreDBMetrics metrics;

  /**
   * Creates a new distributed token claim manager.
   *
   * @param client        the EventStoreDB client
   * @param naming        stream naming strategy
   * @param nodeId        unique identifier for this node
   * @param claimTimeout  how long a claim is valid before expiry
   * @param retryExecutor optional retry executor
   * @param metrics       optional metrics
   */
  public DistributedTokenClaimManager(
      EventStoreDBClient client,
      EventStoreDBStreamNaming naming,
      String nodeId,
      Duration claimTimeout,
      @Nullable EventStoreDBRetryExecutor retryExecutor,
      @Nullable EventStoreDBMetrics metrics) {
    this.client = client;
    this.naming = naming != null ? naming : new EventStoreDBStreamNaming();
    this.nodeId = nodeId;
    this.claimTimeout = claimTimeout != null ? claimTimeout : Duration.ofSeconds(30);
    this.retryExecutor = retryExecutor != null
        ? retryExecutor : EventStoreDBRetryExecutor.noRetry();
    this.metrics = metrics;
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
    this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  /**
   * Attempts to claim ownership of a processor segment.
   *
   * <p>A claim succeeds if:
   * <ul>
   *   <li>No claim exists yet for this segment</li>
   *   <li>The existing claim is owned by this same node</li>
   *   <li>The existing claim has expired (owner crashed or timed out)</li>
   * </ul>
   *
   * @param processorName the processor name
   * @param segment       the segment number
   * @param token         the current tracking token
   * @throws UnableToClaimTokenException if another node holds an active claim
   */
  public void claimSegment(String processorName, int segment, TrackingToken token)
      throws UnableToClaimTokenException {
    String claimStreamName = claimStreamName(processorName, segment);

    ClaimEntry existingClaim = readLatestClaim(claimStreamName);

    if (existingClaim != null && !existingClaim.isRelease()) {
      if (!existingClaim.owner().equals(nodeId) && !isExpired(existingClaim)) {
        log.debug(
            "Segment {}/{} is claimed by node '{}' (expires {}). Cannot claim.",
            processorName, segment, existingClaim.owner(),
            existingClaim.timestamp().plus(claimTimeout));
        throw new UnableToClaimTokenException(
            "Segment " + processorName + "/" + segment
                + " is claimed by node '" + existingClaim.owner() + "'");
      }
      if (isExpired(existingClaim)) {
        log.info(
            "Segment {}/{} claim by node '{}' has expired. Taking over.",
            processorName, segment, existingClaim.owner());
      }
    }

    // Write a new claim event
    writeClaim(claimStreamName, processorName, segment, token);

    if (metrics != null) {
      metrics.recordTokenOperation("claim");
    }
    log.debug("Claimed segment {}/{} for node '{}'", processorName, segment, nodeId);
  }

  /**
   * Extends an existing claim, resetting the expiry timer.
   *
   * @param processorName the processor name
   * @param segment       the segment number
   * @throws UnableToClaimTokenException if the claim is not owned by this node
   */
  public void extendClaim(String processorName, int segment)
      throws UnableToClaimTokenException {
    String claimStreamName = claimStreamName(processorName, segment);

    ClaimEntry existing = readLatestClaim(claimStreamName);
    if (existing == null || existing.isRelease()) {
      throw new UnableToClaimTokenException(
          "No active claim for segment " + processorName + "/" + segment);
    }
    if (!existing.owner().equals(nodeId)) {
      throw new UnableToClaimTokenException(
          "Segment " + processorName + "/" + segment
              + " is claimed by node '" + existing.owner()
              + "', not by this node '" + nodeId + "'");
    }

    // Re-write the claim to extend the timestamp
    writeClaim(claimStreamName, processorName, segment, existing.token());

    if (metrics != null) {
      metrics.recordTokenOperation("extendClaim");
    }
    log.trace("Extended claim on segment {}/{} for node '{}'",
        processorName, segment, nodeId);
  }

  /**
   * Releases the claim for a processor segment, allowing other nodes to claim it.
   *
   * @param processorName the processor name
   * @param segment       the segment number
   */
  public void releaseClaim(String processorName, int segment) {
    String claimStreamName = claimStreamName(processorName, segment);

    ClaimEntry releaseEntry = new ClaimEntry(
        processorName, segment, null, nodeId,
        Instant.now(), true);

    try {
      byte[] data = objectMapper.writeValueAsBytes(releaseEntry);
      EventData eventData =
          EventDataBuilder.json(UUID.randomUUID(), RELEASE_EVENT_TYPE, data).build();

      retryExecutor.execute(
          () -> client.appendToStream(
              claimStreamName,
              AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any()),
              eventData).get(),
          "releaseClaim");

      if (metrics != null) {
        metrics.recordTokenOperation("releaseClaim");
      }
      log.debug("Released claim on segment {}/{} for node '{}'",
          processorName, segment, nodeId);

    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize claim release", e);
    } catch (ExecutionException e) {
      throw new EventStoreException("Failed to release claim in EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while releasing claim", e);
    }
  }

  /**
   * Checks whether this node currently holds the claim for a segment.
   *
   * @param processorName the processor name
   * @param segment       the segment number
   * @return true if this node owns the active (non-expired) claim
   */
  public boolean isClaimedByThisNode(String processorName, int segment) {
    String claimStreamName = claimStreamName(processorName, segment);
    ClaimEntry claim = readLatestClaim(claimStreamName);
    return claim != null
        && !claim.isRelease()
        && claim.owner().equals(nodeId)
        && !isExpired(claim);
  }

  /**
   * Returns the node ID of the current claim owner, or null if unclaimed.
   *
   * @param processorName the processor name
   * @param segment       the segment number
   * @return the owner's node ID, or null
   */
  @Nullable
  public String getClaimOwner(String processorName, int segment) {
    String claimStreamName = claimStreamName(processorName, segment);
    ClaimEntry claim = readLatestClaim(claimStreamName);
    if (claim == null || claim.isRelease() || isExpired(claim)) {
      return null;
    }
    return claim.owner();
  }

  /**
   * Returns the claim timeout duration.
   */
  public Duration getClaimTimeout() {
    return claimTimeout;
  }

  /**
   * Returns the node ID used by this claim manager.
   */
  public String getNodeId() {
    return nodeId;
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private void writeClaim(String claimStreamName, String processorName,
      int segment, TrackingToken token) {
    ClaimEntry entry = new ClaimEntry(
        processorName, segment, token, nodeId,
        Instant.now(), false);

    try {
      byte[] data = objectMapper.writeValueAsBytes(entry);
      EventData eventData =
          EventDataBuilder.json(UUID.randomUUID(), CLAIM_EVENT_TYPE, data).build();

      retryExecutor.execute(
          () -> client.appendToStream(
              claimStreamName,
              AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any()),
              eventData).get(),
          "writeClaim");

    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize claim", e);
    } catch (ExecutionException e) {
      throw new EventStoreException("Failed to write claim to EventStoreDB", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while writing claim", e);
    }
  }

  @Nullable
  private ClaimEntry readLatestClaim(String claimStreamName) {
    try {
      ReadResult result = retryExecutor.execute(
          () -> client.readStream(
              claimStreamName,
              ReadStreamOptions.get().backwards().fromEnd().maxCount(1)).get(),
          "readLatestClaim");

      List<ResolvedEvent> events = result.getEvents();
      if (events.isEmpty()) {
        return null;
      }

      RecordedEvent recorded = events.get(0).getOriginalEvent();
      return objectMapper.readValue(recorded.getEventData(), ClaimEntry.class);

    } catch (ExecutionException e) {
      if (isStreamNotFound(e)) {
        return null;
      }
      throw new EventStoreException("Failed to read claim from EventStoreDB", e.getCause());
    } catch (IOException e) {
      throw new EventStoreException("Failed to deserialize claim entry", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventStoreException("Interrupted while reading claim", e);
    }
  }

  private boolean isExpired(ClaimEntry entry) {
    return entry.timestamp().plus(claimTimeout).isBefore(Instant.now());
  }

  private String claimStreamName(String processorName, int segment) {
    return naming.tokenStream(processorName) + "-claim-" + segment;
  }

  private boolean isStreamNotFound(ExecutionException e) {
    Throwable cause = e.getCause();
    return cause instanceof StreamNotFoundException
        || (cause != null
            && cause.getMessage() != null
            && cause.getMessage().contains("not found"));
  }

  /**
   * Internal record stored in EventStoreDB for each claim/release event.
   */
  record ClaimEntry(
      @JsonProperty("processorName") String processorName,
      @JsonProperty("segmentId") int segmentId,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
      @JsonProperty("token") TrackingToken token,
      @JsonProperty("owner") String owner,
      @JsonProperty("timestamp") Instant timestamp,
      @JsonProperty("release") boolean isRelease) {

    @JsonCreator
    ClaimEntry {
    }
  }
}
