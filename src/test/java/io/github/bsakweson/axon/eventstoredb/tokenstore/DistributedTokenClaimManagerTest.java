package io.github.bsakweson.axon.eventstoredb.tokenstore;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken;
import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundExceptionFactory;
import com.eventstore.dbclient.WriteResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedTokenClaimManagerTest {

  @Mock private EventStoreDBClient client;
  @Mock private EventStoreDBMetrics metrics;

  private EventStoreDBStreamNaming naming;
  private DistributedTokenClaimManager claimManager;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    naming = new EventStoreDBStreamNaming();
    claimManager =
        new DistributedTokenClaimManager(
            client, naming, "node-1", Duration.ofSeconds(30), null, metrics);
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  // ── Claim acquisition ─────────────────────────────────────────────────

  @Test
  void shouldClaimUnclaimedSegment() throws Exception {
    // No existing claim — stream not found
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(
        StreamNotFoundExceptionFactory.create("__axon-tokens-proc-claim-0"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(10L, 10L);
    assertThatCode(() -> claimManager.claimSegment("proc", 0, token))
        .doesNotThrowAnyException();

    verify(client)
        .appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    verify(metrics).recordTokenOperation("claim");
  }

  @Test
  void shouldClaimSegmentWhenExistingClaimExpired() throws Exception {
    // Return an expired claim from another node
    DistributedTokenClaimManager.ClaimEntry expiredEntry =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now().minus(Duration.ofMinutes(5)), false);

    mockReadLatestClaim(expiredEntry);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.claimSegment("proc", 0, null))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldClaimSegmentOwnedBySameNode() throws Exception {
    // Return an active claim owned by the same node
    DistributedTokenClaimManager.ClaimEntry ownClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now(), false);

    mockReadLatestClaim(ownClaim);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.claimSegment("proc", 0, null))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldRejectClaimWhenActivelyClaimedByOtherNode() throws Exception {
    // Return an active, non-expired claim from another node
    DistributedTokenClaimManager.ClaimEntry otherClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), false);

    mockReadLatestClaim(otherClaim);

    assertThatThrownBy(() -> claimManager.claimSegment("proc", 0, null))
        .isInstanceOf(UnableToClaimTokenException.class)
        .hasMessageContaining("node-2");
  }

  @Test
  void shouldClaimSegmentAfterRelease() throws Exception {
    // Return a release entry from another node
    DistributedTokenClaimManager.ClaimEntry released =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), true);

    mockReadLatestClaim(released);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.claimSegment("proc", 0, null))
        .doesNotThrowAnyException();
  }

  // ── Extend claim ──────────────────────────────────────────────────────

  @Test
  void shouldExtendClaimOwnedByThisNode() throws Exception {
    DistributedTokenClaimManager.ClaimEntry activeClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now(), false);

    mockReadLatestClaim(activeClaim);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.extendClaim("proc", 0))
        .doesNotThrowAnyException();
    verify(metrics).recordTokenOperation("extendClaim");
  }

  @Test
  void shouldRejectExtendWhenClaimedByOtherNode() throws Exception {
    DistributedTokenClaimManager.ClaimEntry otherClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), false);

    mockReadLatestClaim(otherClaim);

    assertThatThrownBy(() -> claimManager.extendClaim("proc", 0))
        .isInstanceOf(UnableToClaimTokenException.class)
        .hasMessageContaining("node-2");
  }

  @Test
  void shouldRejectExtendWhenNoActiveClaim() throws Exception {
    // Stream not found
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(
        StreamNotFoundExceptionFactory.create("stream"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    assertThatThrownBy(() -> claimManager.extendClaim("proc", 0))
        .isInstanceOf(UnableToClaimTokenException.class)
        .hasMessageContaining("No active claim");
  }

  // ── Release claim ─────────────────────────────────────────────────────

  @Test
  void shouldReleaseClaim() throws Exception {
    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.releaseClaim("proc", 0))
        .doesNotThrowAnyException();
    verify(metrics).recordTokenOperation("releaseClaim");
  }

  // ── isClaimedByThisNode ───────────────────────────────────────────────

  @Test
  void shouldReturnTrueWhenClaimedByThisNode() throws Exception {
    DistributedTokenClaimManager.ClaimEntry activeClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now(), false);

    mockReadLatestClaim(activeClaim);

    assertThat(claimManager.isClaimedByThisNode("proc", 0)).isTrue();
  }

  @Test
  void shouldReturnFalseWhenClaimedByOtherNode() throws Exception {
    DistributedTokenClaimManager.ClaimEntry otherClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), false);

    mockReadLatestClaim(otherClaim);

    assertThat(claimManager.isClaimedByThisNode("proc", 0)).isFalse();
  }

  @Test
  void shouldReturnFalseWhenClaimExpired() throws Exception {
    DistributedTokenClaimManager.ClaimEntry expiredClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now().minus(Duration.ofMinutes(5)), false);

    mockReadLatestClaim(expiredClaim);

    assertThat(claimManager.isClaimedByThisNode("proc", 0)).isFalse();
  }

  @Test
  void shouldReturnFalseWhenReleased() throws Exception {
    DistributedTokenClaimManager.ClaimEntry released =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now(), true);

    mockReadLatestClaim(released);

    assertThat(claimManager.isClaimedByThisNode("proc", 0)).isFalse();
  }

  @Test
  void shouldReturnFalseWhenNoClaimExists() throws Exception {
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(
        StreamNotFoundExceptionFactory.create("stream"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    assertThat(claimManager.isClaimedByThisNode("proc", 0)).isFalse();
  }

  // ── getClaimOwner ─────────────────────────────────────────────────────

  @Test
  void shouldReturnOwnerOfActiveClaim() throws Exception {
    DistributedTokenClaimManager.ClaimEntry claim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), false);

    mockReadLatestClaim(claim);

    assertThat(claimManager.getClaimOwner("proc", 0)).isEqualTo("node-2");
  }

  @Test
  void shouldReturnNullOwnerWhenNoActiveClaim() throws Exception {
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(
        StreamNotFoundExceptionFactory.create("stream"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    assertThat(claimManager.getClaimOwner("proc", 0)).isNull();
  }

  // ── Configuration ─────────────────────────────────────────────────────

  @Test
  void shouldExposeClaimTimeout() {
    assertThat(claimManager.getClaimTimeout()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void shouldExposeNodeId() {
    assertThat(claimManager.getNodeId()).isEqualTo("node-1");
  }

  @Test
  void shouldDefaultClaimTimeoutWhenNull() {
    DistributedTokenClaimManager mgr =
        new DistributedTokenClaimManager(client, naming, "n", null, null, null);
    assertThat(mgr.getClaimTimeout()).isEqualTo(Duration.ofSeconds(30));
  }

  // ── Error handling ────────────────────────────────────────────────────

  @Test
  void shouldThrowOnClaimWriteFailure() throws Exception {
    // No existing claim
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(
        StreamNotFoundExceptionFactory.create("stream"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    // Write fails
    CompletableFuture<WriteResult> writeFuture = new CompletableFuture<>();
    writeFuture.completeExceptionally(new RuntimeException("connection lost"));
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(writeFuture);

    assertThatThrownBy(() -> claimManager.claimSegment("proc", 0, null))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to write claim");
  }

  @Test
  void shouldThrowOnReleaseWriteFailure() throws Exception {
    CompletableFuture<WriteResult> writeFuture = new CompletableFuture<>();
    writeFuture.completeExceptionally(new RuntimeException("connection lost"));
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(writeFuture);

    assertThatThrownBy(() -> claimManager.releaseClaim("proc", 0))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to release claim");
  }

  @Test
  void shouldThrowOnReadFailure() throws Exception {
    CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
    readFuture.completeExceptionally(new RuntimeException("connection lost"));
    when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

    assertThatThrownBy(() -> claimManager.claimSegment("proc", 0, null))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to read claim");
  }

  @Test
  void shouldReturnNullOwnerWhenClaimExpired() throws Exception {
    DistributedTokenClaimManager.ClaimEntry expiredClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now().minus(Duration.ofMinutes(5)), false);

    mockReadLatestClaim(expiredClaim);

    assertThat(claimManager.getClaimOwner("proc", 0)).isNull();
  }

  @Test
  void shouldReturnNullOwnerWhenClaimReleased() throws Exception {
    DistributedTokenClaimManager.ClaimEntry released =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-2",
            Instant.now(), true);

    mockReadLatestClaim(released);

    assertThat(claimManager.getClaimOwner("proc", 0)).isNull();
  }

  @Test
  void shouldHandleEmptyReadResultForClaim() throws Exception {
    ReadResult readResult = mock(ReadResult.class);
    when(readResult.getEvents()).thenReturn(List.of());
    when(client.readStream(anyString(), any(ReadStreamOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(readResult));

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.claimSegment("proc", 0, null))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldExtendClaimPreservingToken() throws Exception {
    EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(50L, 50L);
    DistributedTokenClaimManager.ClaimEntry activeClaim =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, token, "node-1",
            Instant.now(), false);

    mockReadLatestClaim(activeClaim);

    WriteResult writeResult = mock(WriteResult.class);
    when(client.appendToStream(
            anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(writeResult));

    assertThatCode(() -> claimManager.extendClaim("proc", 0))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldRejectExtendWhenClaimIsReleased() throws Exception {
    DistributedTokenClaimManager.ClaimEntry released =
        new DistributedTokenClaimManager.ClaimEntry(
            "proc", 0, null, "node-1",
            Instant.now(), true);

    mockReadLatestClaim(released);

    assertThatThrownBy(() -> claimManager.extendClaim("proc", 0))
        .isInstanceOf(UnableToClaimTokenException.class)
        .hasMessageContaining("No active claim");
  }

  @Test
  void shouldCreateWithNullNaming() {
    DistributedTokenClaimManager mgr =
        new DistributedTokenClaimManager(client, null, "n", Duration.ofSeconds(10), null, null);
    assertThat(mgr.getNodeId()).isEqualTo("n");
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  private void mockReadLatestClaim(DistributedTokenClaimManager.ClaimEntry entry)
      throws Exception {
    byte[] data = objectMapper.writeValueAsBytes(entry);

    RecordedEvent recordedEvent = mock(RecordedEvent.class);
    when(recordedEvent.getEventData()).thenReturn(data);

    ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
    when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);

    ReadResult readResult = mock(ReadResult.class);
    when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));

    when(client.readStream(anyString(), any(ReadStreamOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(readResult));
  }
}
