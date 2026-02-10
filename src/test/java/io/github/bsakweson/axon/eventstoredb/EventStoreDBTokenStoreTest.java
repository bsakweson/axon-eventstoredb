package io.github.bsakweson.axon.eventstoredb;

import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import io.github.bsakweson.axon.eventstoredb.tokenstore.DistributedTokenClaimManager;
import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.DeleteResult;
import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundExceptionFactory;
import com.eventstore.dbclient.WriteResult;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.axonframework.eventhandling.TrackingToken;
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
class EventStoreDBTokenStoreTest {

    @Mock
    private EventStoreDBClient client;

    private EventStoreDBStreamNaming naming;
    private EventStoreDBTokenStore tokenStore;

    @BeforeEach
    void setUp() {
        naming = new EventStoreDBStreamNaming();
        tokenStore = new EventStoreDBTokenStore(client, naming, "test-node");
    }

    // ── storeToken ──────────────────────────────────────────────────────

    @Test
    void shouldStoreTokenInCorrectStream() throws Exception {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(eq("__axon-tokens-my-processor-0"), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        tokenStore.storeToken(token, "my-processor", 0);

        verify(client).appendToStream(eq("__axon-tokens-my-processor-0"), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldStoreNullTokenSuccessfully() throws Exception {
        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        tokenStore.storeToken(null, "my-processor", 0);

        verify(client).appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldThrowOnStoreTokenFailure() throws Exception {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);

        CompletableFuture<WriteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("connection lost"));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> tokenStore.storeToken(token, "my-processor", 0))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to store token");
    }

    @Test
    void shouldThrowOnStoreTokenInterruption() throws Exception {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);

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

        assertThatThrownBy(() -> tokenStore.storeToken(token, "my-processor", 0))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── fetchToken ──────────────────────────────────────────────────────

    @Test
    void shouldReturnNullWhenNoTokenStored() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(eq("__axon-tokens-my-processor-0"), any(ReadStreamOptions.class)))
                .thenReturn(failedFuture);

        TrackingToken token = tokenStore.fetchToken("my-processor", 0);
        assertThat(token).isNull();
    }

    @Test
    void shouldFetchStoredToken() throws Exception {
        EventStoreDBTrackingToken expectedToken = EventStoreDBTrackingToken.of(200L, 200L);
        String json = "{\"processorName\":\"my-processor\",\"segmentId\":0,"
                + "\"token\":{\"@class\":\"io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken\","
                + "\"commitPosition\":200,\"preparePosition\":200},"
                + "\"owner\":\"test-node\",\"timestamp\":\"2026-02-09T10:00:00Z\"}";

        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        when(recordedEvent.getEventData()).thenReturn(json.getBytes());

        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readStream(eq("__axon-tokens-my-processor-0"), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        TrackingToken token = tokenStore.fetchToken("my-processor", 0);

        assertThat(token).isNotNull().isInstanceOf(EventStoreDBTrackingToken.class);
        EventStoreDBTrackingToken esdbToken = (EventStoreDBTrackingToken) token;
        assertThat(esdbToken.getCommitPosition()).isEqualTo(200L);
        assertThat(esdbToken.getPreparePosition()).isEqualTo(200L);
    }

    @Test
    void shouldReturnNullWhenEventsListEmpty() throws Exception {
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(Collections.emptyList());
        when(client.readStream(eq("__axon-tokens-my-processor-0"), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        TrackingToken token = tokenStore.fetchToken("my-processor", 0);
        assertThat(token).isNull();
    }

    @Test
    void shouldThrowOnFetchTokenGenericError() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("db error"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> tokenStore.fetchToken("my-processor", 0))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to fetch token");
    }

    @Test
    void shouldThrowOnFetchTokenDeserializationError() throws Exception {
        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        when(recordedEvent.getEventData()).thenReturn("invalid-json".getBytes());

        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);

        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        assertThatThrownBy(() -> tokenStore.fetchToken("my-processor", 0))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    @Test
    void shouldThrowOnFetchTokenInterruption() throws Exception {
        CompletableFuture<ReadResult> hangingFuture = new CompletableFuture<>();
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
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

        assertThatThrownBy(() -> tokenStore.fetchToken("my-processor", 0))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── fetchSegments ───────────────────────────────────────────────────

    @Test
    void shouldReturnEmptySegmentsWhenNoneExist() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failedFuture);

        int[] segments = tokenStore.fetchSegments("my-processor");
        assertThat(segments).isEmpty();
    }

    @Test
    void shouldReturnExistingSegments() throws Exception {
        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        ReadResult existsResult = mock(ReadResult.class);
        when(existsResult.getEvents()).thenReturn(List.of(resolvedEvent));

        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));

        when(client.readStream(eq("__axon-tokens-my-processor-0"), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(existsResult));
        when(client.readStream(
                argThat(s -> s != null && s.startsWith("__axon-tokens-my-processor-")
                        && !s.equals("__axon-tokens-my-processor-0")),
                any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        int[] segments = tokenStore.fetchSegments("my-processor");
        assertThat(segments).contains(0);
    }

    @Test
    void shouldHandleInterruptionDuringFetchSegments() throws Exception {
        CompletableFuture<ReadResult> hangingFuture = new CompletableFuture<>();
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
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

        int[] segments = tokenStore.fetchSegments("my-processor");
        assertThat(segments).isEmpty();
        Thread.interrupted();
    }

    // ── initializeTokenSegments ─────────────────────────────────────────

    @Test
    void shouldInitializeNewSegments() throws Exception {
        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        tokenStore.initializeTokenSegments("my-processor", 2);

        verify(client, times(2)).appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldInitializeWithInitialToken() throws Exception {
        EventStoreDBTrackingToken initialToken = EventStoreDBTrackingToken.of(100L, 100L);

        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        tokenStore.initializeTokenSegments("my-processor", 1, initialToken);

        verify(client).appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldSkipAlreadyInitializedSegments() throws Exception {
        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        ReadResult existsResult = mock(ReadResult.class);
        when(existsResult.getEvents()).thenReturn(List.of(resolvedEvent));

        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(existsResult));

        tokenStore.initializeTokenSegments("my-processor", 1);

        verify(client, never()).appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldHandleConcurrentInitialization() throws Exception {
        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        CompletableFuture<WriteResult> wrongVersionFuture = new CompletableFuture<>();
        wrongVersionFuture.completeExceptionally(mock(WrongExpectedVersionException.class));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(wrongVersionFuture);

        // Should not throw — WrongExpectedVersionException is handled by continuing
        assertThatCode(() -> tokenStore.initializeTokenSegments("my-processor", 1))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowOnInitializeCheckExecutionException() throws Exception {
        CompletableFuture<ReadResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("db error"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> tokenStore.initializeTokenSegments("my-processor", 1))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to check token segment existence");
    }

    @Test
    void shouldThrowOnInitializeInterruption() throws Exception {
        CompletableFuture<ReadResult> hangingFuture = new CompletableFuture<>();
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
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

        assertThatThrownBy(() -> tokenStore.initializeTokenSegments("my-processor", 1))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── extendClaim ─────────────────────────────────────────────────────

    @Test
    void shouldExtendClaimByRestoringToken() throws Exception {
        // fetchToken returns a token
        String json = "{\"processorName\":\"my-processor\",\"segmentId\":0,"
                + "\"token\":{\"@class\":\"io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken\","
                + "\"commitPosition\":100,\"preparePosition\":100},"
                + "\"owner\":\"test-node\",\"timestamp\":\"2026-02-09T10:00:00Z\"}";

        RecordedEvent recordedEvent = mock(RecordedEvent.class);
        when(recordedEvent.getEventData()).thenReturn(json.getBytes());
        ResolvedEvent resolvedEvent = mock(ResolvedEvent.class);
        when(resolvedEvent.getOriginalEvent()).thenReturn(recordedEvent);
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolvedEvent));
        when(client.readStream(eq("__axon-tokens-my-processor-0"), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(eq("__axon-tokens-my-processor-0"), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        tokenStore.extendClaim("my-processor", 0);

        // Should have stored the token again
        verify(client).appendToStream(eq("__axon-tokens-my-processor-0"), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldNoOpExtendClaimWhenNoToken() throws Exception {
        CompletableFuture<ReadResult> notFoundFuture = new CompletableFuture<>();
        notFoundFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(notFoundFuture);

        tokenStore.extendClaim("my-processor", 0);

        verify(client, never()).appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    // ── deleteToken ─────────────────────────────────────────────────────

    @Test
    void shouldDeleteTokenStream() throws Exception {
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(client.deleteStream(eq("__axon-tokens-my-processor-0"), any(DeleteStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(deleteResult));

        tokenStore.deleteToken("my-processor", 0);

        verify(client).deleteStream(eq("__axon-tokens-my-processor-0"), any(DeleteStreamOptions.class));
    }

    @Test
    void shouldNotThrowWhenDeletingNonexistentTokenStream() throws Exception {
        CompletableFuture<DeleteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(StreamNotFoundExceptionFactory.create("stream"));
        when(client.deleteStream(anyString(), any(DeleteStreamOptions.class)))
                .thenReturn(failedFuture);

        assertThatCode(() -> tokenStore.deleteToken("my-processor", 0))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowOnDeleteTokenGenericError() throws Exception {
        CompletableFuture<DeleteResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("db error"));
        when(client.deleteStream(anyString(), any(DeleteStreamOptions.class)))
                .thenReturn(failedFuture);

        assertThatThrownBy(() -> tokenStore.deleteToken("my-processor", 0))
                .isInstanceOf(EventStoreException.class)
                .hasMessageContaining("Failed to delete token stream");
    }

    @Test
    void shouldThrowOnDeleteTokenInterruption() throws Exception {
        CompletableFuture<DeleteResult> hangingFuture = new CompletableFuture<>();
        when(client.deleteStream(anyString(), any(DeleteStreamOptions.class)))
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

        assertThatThrownBy(() -> tokenStore.deleteToken("my-processor", 0))
                .isInstanceOf(EventStoreException.class);
        Thread.interrupted();
    }

    // ── releaseClaim ────────────────────────────────────────────────────

    @Test
    void shouldReleaseClaimWithoutError() {
        assertThatCode(() -> tokenStore.releaseClaim("my-processor", 0))
                .doesNotThrowAnyException();
    }

    // ── requiresExplicitSegmentInitialization ────────────────────────────

    @Test
    void shouldRequireExplicitSegmentInitialization() {
        assertThat(tokenStore.requiresExplicitSegmentInitialization()).isTrue();
    }

    // ── retrieveStorageIdentifier ───────────────────────────────────────

    @Test
    void shouldReturnStorageIdentifier() {
        Optional<String> id = tokenStore.retrieveStorageIdentifier();
        assertThat(id).isPresent().contains("eventstoredb-test-node");
    }

    @Test
    void shouldUseRandomNodeIdWhenNotSpecified() {
        EventStoreDBTokenStore store = new EventStoreDBTokenStore(client, naming);
        Optional<String> id = store.retrieveStorageIdentifier();
        assertThat(id).isPresent();
        assertThat(id.get()).startsWith("eventstoredb-");
    }

    @Test
    void shouldUseDefaultNamingWhenNull() {
        EventStoreDBTokenStore store = new EventStoreDBTokenStore(client, null, "node-1");
        assertThat(store.retrieveStorageIdentifier()).isPresent();
    }

    // ── Distributed claim manager integration ───────────────────────────

    @Test
    void shouldDelegateExtendClaimToClaimManager() throws Exception {
        DistributedTokenClaimManager claimManager = mock(DistributedTokenClaimManager.class);
        EventStoreDBTokenStore storeWithClaims =
            new EventStoreDBTokenStore(client, naming, "test-node", null, null, claimManager);

        storeWithClaims.extendClaim("proc", 0);

        verify(claimManager).extendClaim("proc", 0);
        verifyNoInteractions(client);
    }

    @Test
    void shouldDelegateReleaseClaimToClaimManager() {
        DistributedTokenClaimManager claimManager = mock(DistributedTokenClaimManager.class);
        EventStoreDBTokenStore storeWithClaims =
            new EventStoreDBTokenStore(client, naming, "test-node", null, null, claimManager);

        storeWithClaims.releaseClaim("proc", 0);

        verify(claimManager).releaseClaim("proc", 0);
    }

    @Test
    void shouldCheckClaimBeforeStoreToken() throws Exception {
        DistributedTokenClaimManager claimManager = mock(DistributedTokenClaimManager.class);
        when(claimManager.isClaimedByThisNode("proc", 0)).thenReturn(true);

        EventStoreDBTokenStore storeWithClaims =
            new EventStoreDBTokenStore(client, naming, "test-node", null, null, claimManager);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
            .thenReturn(CompletableFuture.completedFuture(writeResult));

        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(10L, 10L);
        storeWithClaims.storeToken(token, "proc", 0);

        // Should not attempt to claim since already claimed by this node
        verify(claimManager, never()).claimSegment(anyString(), anyInt(), any());
    }

    @Test
    void shouldAttemptClaimBeforeStoreTokenWhenNotOwned() throws Exception {
        DistributedTokenClaimManager claimManager = mock(DistributedTokenClaimManager.class);
        when(claimManager.isClaimedByThisNode("proc", 0)).thenReturn(false);

        EventStoreDBTokenStore storeWithClaims =
            new EventStoreDBTokenStore(client, naming, "test-node", null, null, claimManager);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
            .thenReturn(CompletableFuture.completedFuture(writeResult));

        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(10L, 10L);
        storeWithClaims.storeToken(token, "proc", 0);

        verify(claimManager).claimSegment("proc", 0, token);
    }

    @Test
    void shouldAttemptClaimBeforeFetchToken() throws Exception {
        DistributedTokenClaimManager claimManager = mock(DistributedTokenClaimManager.class);
        when(claimManager.isClaimedByThisNode("proc", 0)).thenReturn(false);

        EventStoreDBTokenStore storeWithClaims =
            new EventStoreDBTokenStore(client, naming, "test-node", null, null, claimManager);

        CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
        readFuture.completeExceptionally(
            StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class))).thenReturn(readFuture);

        storeWithClaims.fetchToken("proc", 0);

        verify(claimManager).claimSegment("proc", 0, null);
    }

    // ── Metrics branch coverage ─────────────────────────────────────────

    @Test
    void shouldRecordMetricsOnStoreToken() throws Exception {
        io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics metrics =
            mock(io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics.class);
        EventStoreDBTokenStore storeWithMetrics =
            new EventStoreDBTokenStore(client, naming, "test-node", null, metrics);

        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));

        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);
        storeWithMetrics.storeToken(token, "proc", 0);

        verify(metrics).recordTokenOperation("store");
    }

    @Test
    void shouldRecordMetricsErrorOnStoreTokenFailure() {
        io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics metrics =
            mock(io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics.class);
        EventStoreDBTokenStore storeWithMetrics =
            new EventStoreDBTokenStore(client, naming, "test-node", null, metrics);

        CompletableFuture<WriteResult> failFuture = new CompletableFuture<>();
        failFuture.completeExceptionally(new RuntimeException("connection lost"));
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(failFuture);

        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);
        assertThatThrownBy(() -> storeWithMetrics.storeToken(token, "proc", 0))
                .isInstanceOf(EventStoreException.class);

        verify(metrics).recordError("storeToken");
    }

    @Test
    void shouldRecordMetricsOnFetchToken() throws Exception {
        io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics metrics =
            mock(io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics.class);
        EventStoreDBTokenStore storeWithMetrics =
            new EventStoreDBTokenStore(client, naming, "test-node", null, metrics);

        // First store a token to have something to fetch
        WriteResult writeResult = mock(WriteResult.class);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(CompletableFuture.completedFuture(writeResult));
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);
        storeWithMetrics.storeToken(token, "proc", 0);

        // Now mock read to return a token
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        om.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        om.registerSubtypes(EventStoreDBTrackingToken.class);
        byte[] tokenData = om.writeValueAsBytes(new java.util.LinkedHashMap<>(java.util.Map.of(
                "processorName", "proc",
                "segmentId", 0,
                "token", java.util.Map.of("@class", "io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken", "commitPosition", 100, "preparePosition", 100),
                "owner", "test-node",
                "timestamp", java.time.Instant.now().toString()
        )));

        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventData()).thenReturn(tokenData);
        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(resolved.getOriginalEvent()).thenReturn(recorded);
        ReadResult readResult = mock(ReadResult.class);
        when(readResult.getEvents()).thenReturn(List.of(resolved));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        storeWithMetrics.fetchToken("proc", 0);

        verify(metrics).recordTokenOperation("fetch");
    }

    @Test
    void shouldRecordMetricsErrorOnFetchTokenFailure() {
        io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics metrics =
            mock(io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics.class);
        EventStoreDBTokenStore storeWithMetrics =
            new EventStoreDBTokenStore(client, naming, "test-node", null, metrics);

        CompletableFuture<ReadResult> failFuture = new CompletableFuture<>();
        failFuture.completeExceptionally(new RuntimeException("connection lost"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failFuture);

        assertThatThrownBy(() -> storeWithMetrics.fetchToken("proc", 0))
                .isInstanceOf(EventStoreException.class);

        verify(metrics).recordError("fetchToken");
    }

    @Test
    void shouldRecordMetricsOnDeleteToken() throws Exception {
        io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics metrics =
            mock(io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics.class);
        EventStoreDBTokenStore storeWithMetrics =
            new EventStoreDBTokenStore(client, naming, "test-node", null, metrics);

        DeleteResult deleteResult = mock(DeleteResult.class);
        when(client.deleteStream(anyString(), any(DeleteStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(deleteResult));

        storeWithMetrics.deleteToken("proc", 0);

        verify(metrics).recordTokenOperation("delete");
    }

    // ── isStreamNotFound branch coverage ─────────────────────────────────

    @Test
    void shouldTreatMessageBasedNotFoundAsStreamNotFound() {
        CompletableFuture<ReadResult> failFuture = new CompletableFuture<>();
        failFuture.completeExceptionally(new RuntimeException("stream not found in EventStoreDB"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failFuture);

        // Should return null (not throw) because isStreamNotFound returns true
        TrackingToken result = tokenStore.fetchToken("proc", 0);
        assertThat(result).isNull();
    }

    @Test
    void shouldStopScanningSegmentsOnNonStreamNotFoundError() {
        // Segment 0 exists
        ReadResult readResult = mock(ReadResult.class);
        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(readResult.getEvents()).thenReturn(List.of(resolved));

        // Segment 1 fails with non-stream-not-found error
        CompletableFuture<ReadResult> failFuture = new CompletableFuture<>();
        failFuture.completeExceptionally(new RuntimeException("connection refused"));

        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult))
                .thenReturn(failFuture);

        // fetchSegments should continue past non-fatal errors for i > 0
        int[] segments = tokenStore.fetchSegments("proc");
        assertThat(segments).contains(0);
    }

    @Test
    void shouldThrowOnInitializeAppendInterruption() {
        // Mock readStream to throw StreamNotFound so we proceed to append
        CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
        readFuture.completeExceptionally(
            StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(readFuture);

        // Mock appendToStream to return an unresolved future (blocks on .get())
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(new CompletableFuture<>());

        // Set interrupt flag — readStream.get() won't see it (future already failed)
        // but appendToStream.get() WILL detect it when it tries to wait
        Thread.currentThread().interrupt();

        try {
            assertThatThrownBy(() -> tokenStore.initializeTokenSegments("proc", 1))
                    .isInstanceOf(EventStoreException.class)
                    .hasMessageContaining("Interrupted during token initialization");
        } finally {
            Thread.interrupted(); // clear interrupt flag to avoid affecting other tests
        }
    }

    // ── initializeTokenSegments additional branch coverage ───────────────

    @Test
    void shouldSkipAlreadyInitializedSegmentDuringInit() throws Exception {
        // Segment 0 already has events
        ReadResult readResult = mock(ReadResult.class);
        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(readResult.getEvents()).thenReturn(List.of(resolved));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(readResult));

        // Should complete without appending (segment already initialized)
        tokenStore.initializeTokenSegments("proc", 1);

        verify(client, never())
                .appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class));
    }

    @Test
    void shouldHandleWrongExpectedVersionOnInit() throws Exception {
        // readStream throws SNF → proceed to append
        CompletableFuture<ReadResult> readFuture = new CompletableFuture<>();
        readFuture.completeExceptionally(
            StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(readFuture);

        // appendToStream fails with WrongExpectedVersion (concurrent init by another node)
        WrongExpectedVersionException wrongVersion = mock(WrongExpectedVersionException.class);
        CompletableFuture<WriteResult> appendFuture = new CompletableFuture<>();
        appendFuture.completeExceptionally(wrongVersion);
        when(client.appendToStream(anyString(), any(AppendToStreamOptions.class), any(EventData.class)))
                .thenReturn(appendFuture);

        // Should not throw — handled gracefully as "already initialized by another node"
        assertThatCode(() -> tokenStore.initializeTokenSegments("proc", 1))
                .doesNotThrowAnyException();
    }

    // ── fetchToken with claimManager coverage ────────────────────────────

    @Test
    void shouldSkipReClaimWhenAlreadyClaimedByThisNode() throws Exception {
        DistributedTokenClaimManager claimMgr = mock(DistributedTokenClaimManager.class);
        when(claimMgr.isClaimedByThisNode("proc", 0)).thenReturn(true);

        EventStoreDBTokenStore storeWithClaims = new EventStoreDBTokenStore(
                client, naming, "test-node", null, null, claimMgr);

        // readStream returns empty → null token
        CompletableFuture<ReadResult> snfFuture = new CompletableFuture<>();
        snfFuture.completeExceptionally(
            StreamNotFoundExceptionFactory.create("stream"));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(snfFuture);

        TrackingToken result = storeWithClaims.fetchToken("proc", 0);
        assertThat(result).isNull();

        // claimSegment should NOT be called (already claimed by this node)
        verify(claimMgr, never()).claimSegment(anyString(), anyInt(), any());
    }

    // ── isStreamNotFound null cause/message coverage ─────────────────────

    @Test
    void shouldNotTreatNullMessageAsStreamNotFound() {
        // ExecutionException with a cause that has null message
        CompletableFuture<ReadResult> failFuture = new CompletableFuture<>();
        failFuture.completeExceptionally(new RuntimeException((String) null));
        when(client.readStream(anyString(), any(ReadStreamOptions.class)))
                .thenReturn(failFuture);

        // fetchSegments with i=0: non-SNF error → break loop
        int[] segments = tokenStore.fetchSegments("proc");
        assertThat(segments).isEmpty();
    }
}
