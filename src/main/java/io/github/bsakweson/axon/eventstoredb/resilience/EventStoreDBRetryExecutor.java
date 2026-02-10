package io.github.bsakweson.axon.eventstoredb.resilience;

import com.eventstore.dbclient.StreamNotFoundException;
import com.eventstore.dbclient.WrongExpectedVersionException;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes EventStoreDB operations with configurable retry and exponential backoff.
 *
 * <p>Retries are triggered only for transient failures. Semantic errors such as
 * {@link StreamNotFoundException} and {@link WrongExpectedVersionException} are
 * never retried because they indicate application-level conditions that do not
 * resolve by waiting.
 *
 * <p>Jitter (±25%) is applied to backoff durations to prevent thundering herd
 * problems when multiple clients retry simultaneously.
 */
public class EventStoreDBRetryExecutor {

  private static final Logger log =
      LoggerFactory.getLogger(EventStoreDBRetryExecutor.class);

  private final RetryPolicy policy;

  /**
   * Creates an executor with the given retry policy.
   *
   * @param policy the retry policy to use (null defaults to no-retry)
   */
  public EventStoreDBRetryExecutor(RetryPolicy policy) {
    this.policy = policy != null ? policy : RetryPolicy.noRetry();
  }

  /**
   * Executes the given operation with retry logic.
   *
   * @param operation     the operation to execute
   * @param operationName descriptive name for logging
   * @param <T>           the result type
   * @return the operation result
   * @throws ExecutionException   if the operation fails after all retries
   * @throws InterruptedException if the thread is interrupted
   */
  @SuppressWarnings("BusyWait")
  public <T> T execute(Callable<T> operation, String operationName)
      throws ExecutionException, InterruptedException {
    if (!policy.isEnabled()) {
      return callUnchecked(operation);
    }

    int attempt = 0;
    long backoff = policy.getInitialBackoffMs();

    while (true) {
      try {
        return operation.call();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        if (!isRetryable(e) || attempt >= policy.getMaxRetries()) {
          throw e;
        }
        attempt++;
        long jitteredBackoff = applyJitter(backoff);
        log.warn(
            "Transient failure on '{}' (attempt {}/{}). "
                + "Retrying in {}ms: {}",
            operationName, attempt, policy.getMaxRetries(),
            jitteredBackoff,
            e.getCause() != null ? e.getCause().getMessage()
                : e.getMessage());
        Thread.sleep(jitteredBackoff);
        backoff = Math.min(
            (long) (backoff * policy.getMultiplier()),
            policy.getMaxBackoffMs());
      } catch (Exception e) {
        if (attempt >= policy.getMaxRetries()) {
          throw new ExecutionException(e);
        }
        attempt++;
        long jitteredBackoff = applyJitter(backoff);
        log.warn(
            "Unexpected failure on '{}' (attempt {}/{}). "
                + "Retrying in {}ms: {}",
            operationName, attempt, policy.getMaxRetries(),
            jitteredBackoff, e.getMessage());
        Thread.sleep(jitteredBackoff);
        backoff = Math.min(
            (long) (backoff * policy.getMultiplier()),
            policy.getMaxBackoffMs());
      }
    }
  }

  /**
   * Determines whether the given exception represents a transient failure
   * that should be retried.
   */
  private boolean isRetryable(ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause == null) {
      return true;
    }
    // Never retry semantic errors
    if (cause instanceof StreamNotFoundException) {
      return false;
    }
    return !(cause instanceof WrongExpectedVersionException);
  }

  /**
   * Applies ±25 percent jitter to the backoff duration to prevent
   * thundering herd.
   */
  long applyJitter(long backoff) {
    if (backoff <= 0) {
      return 0;
    }
    long jitterRange = backoff / 4;
    if (jitterRange == 0) {
      return backoff;
    }
    return backoff - jitterRange
        + ThreadLocalRandom.current().nextLong(2 * jitterRange + 1);
  }

  /** Returns a no-op executor that executes operations exactly once. */
  public static EventStoreDBRetryExecutor noRetry() {
    return new EventStoreDBRetryExecutor(RetryPolicy.noRetry());
  }

  /** Returns the retry policy used by this executor. */
  public RetryPolicy getPolicy() {
    return policy;
  }

  private <T> T callUnchecked(Callable<T> operation)
      throws ExecutionException, InterruptedException {
    try {
      return operation.call();
    } catch (ExecutionException | InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(e);
    }
  }
}
