package io.github.bsakweson.axon.eventstoredb.resilience;

import com.eventstore.dbclient.StreamNotFoundExceptionFactory;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

class EventStoreDBRetryExecutorTest {

    // ── No-retry mode ───────────────────────────────────────────────────

    @Test
    void shouldExecuteWithoutRetryWhenDisabled() throws Exception {
        EventStoreDBRetryExecutor executor = EventStoreDBRetryExecutor.noRetry();

        String result = executor.execute(() -> "hello", "test-op");
        assertThat(result).isEqualTo("hello");
    }

    @Test
    void shouldPropagateExceptionWithoutRetryWhenDisabled() {
        EventStoreDBRetryExecutor executor = EventStoreDBRetryExecutor.noRetry();

        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    throw new ExecutionException(new RuntimeException("fail"));
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    void shouldReturnNoRetryPolicy() {
        EventStoreDBRetryExecutor executor = EventStoreDBRetryExecutor.noRetry();
        assertThat(executor.getPolicy().isEnabled()).isFalse();
    }

    // ── Retry behavior ──────────────────────────────────────────────────

    @Test
    void shouldRetryOnTransientFailure() throws Exception {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(3)
                .initialBackoffMs(1)
                .maxBackoffMs(10)
                .multiplier(1.0)
                .build();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        String result = executor.execute(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new ExecutionException(new RuntimeException("transient"));
            }
            return "success";
        }, "test-op");

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void shouldExhaustRetriesAndThrow() {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(2)
                .initialBackoffMs(1)
                .maxBackoffMs(5)
                .multiplier(1.0)
                .build();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    attempts.incrementAndGet();
                    throw new ExecutionException(new RuntimeException("persistent"));
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class);

        // initial + 2 retries = 3
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void shouldNotRetryStreamNotFoundException() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    attempts.incrementAndGet();
                    throw new ExecutionException(
                            StreamNotFoundExceptionFactory.create("test-stream"));
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class);

        // Should not retry — only 1 attempt
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void shouldNotRetryWrongExpectedVersionException() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    attempts.incrementAndGet();
                    throw new ExecutionException(mock(WrongExpectedVersionException.class));
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class);

        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void shouldHandleNonExecutionExceptions() throws Exception {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(2)
                .initialBackoffMs(1)
                .maxBackoffMs(5)
                .multiplier(1.0)
                .build();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        String result = executor.execute(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException("unexpected");
            }
            return "recovered";
        }, "test-op");

        assertThat(result).isEqualTo("recovered");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void shouldPropagateInterruptedException() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    throw new InterruptedException("interrupted");
                },
                "test-op"))
                .isInstanceOf(InterruptedException.class);
    }

    @Test
    void shouldDefaultToNoRetryWhenNullPolicy() throws Exception {
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(null);

        String result = executor.execute(() -> "ok", "test-op");
        assertThat(result).isEqualTo("ok");
        assertThat(executor.getPolicy().isEnabled()).isFalse();
    }

    // ── Jitter ──────────────────────────────────────────────────────────

    @Test
    void shouldApplyJitterWithinBounds() {
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(
                RetryPolicy.defaultPolicy());

        for (int i = 0; i < 100; i++) {
            long jittered = executor.applyJitter(1000);
            // ±25% of 1000 → range [750, 1250]
            assertThat(jittered).isBetween(750L, 1250L);
        }
    }

    @Test
    void shouldReturnZeroJitterForZeroBackoff() {
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(
                RetryPolicy.defaultPolicy());
        assertThat(executor.applyJitter(0)).isZero();
    }

    @Test
    void shouldReturnOriginalForVerySmallBackoff() {
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(
                RetryPolicy.defaultPolicy());
        //  backoff=1 → jitterRange=0 → returns backoff
        assertThat(executor.applyJitter(1)).isEqualTo(1);
    }

    // ── Additional branch coverage ──────────────────────────────────────

    @Test
    void shouldRetryOnExecutionExceptionWithNullCause() throws Exception {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(2)
                .initialBackoffMs(1)
                .maxBackoffMs(5)
                .multiplier(1.0)
                .build();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        String result = executor.execute(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new ExecutionException(null);  // null cause → isRetryable returns true
            }
            return "ok";
        }, "test-op");

        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void shouldWrapGenericExceptionInNoRetryMode() {
        EventStoreDBRetryExecutor executor = EventStoreDBRetryExecutor.noRetry();

        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    throw new RuntimeException("unexpected");
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("unexpected");
    }

    @Test
    void shouldExhaustRetriesOnGenericException() {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(1)
                .initialBackoffMs(1)
                .maxBackoffMs(5)
                .multiplier(1.0)
                .build();
        EventStoreDBRetryExecutor executor = new EventStoreDBRetryExecutor(policy);

        AtomicInteger attempts = new AtomicInteger(0);
        assertThatThrownBy(() -> executor.execute(
                (Callable<String>) () -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("persistent failure");
                },
                "test-op"))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class);

        // initial + 1 retry = 2 attempts
        assertThat(attempts.get()).isEqualTo(2);
    }
}
