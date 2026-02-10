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
}
