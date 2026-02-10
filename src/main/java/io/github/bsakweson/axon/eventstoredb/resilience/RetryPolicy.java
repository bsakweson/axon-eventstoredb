package io.github.bsakweson.axon.eventstoredb.resilience;

import org.axonframework.common.AxonConfigurationException;

/**
 * Immutable configuration for retry behavior on transient EventStoreDB failures.
 *
 * <p>Uses exponential backoff with jitter to avoid thundering herd problems when
 * multiple clients retry simultaneously.
 *
 * <p>Example:
 * <pre>
 * RetryPolicy policy = RetryPolicy.builder()
 *     .maxRetries(3)
 *     .initialBackoffMs(100)
 *     .maxBackoffMs(5000)
 *     .multiplier(2.0)
 *     .build();
 * </pre>
 */
public class RetryPolicy {

  private final int maxRetries;
  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final double multiplier;

  private RetryPolicy(int maxRetries, long initialBackoffMs,
      long maxBackoffMs, double multiplier) {
    this.maxRetries = maxRetries;
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.multiplier = multiplier;
  }

  /** Creates a no-retry policy that executes operations exactly once. */
  public static RetryPolicy noRetry() {
    return new RetryPolicy(0, 0, 0, 1.0);
  }

  /** Creates the default retry policy: 3 retries, 100ms initial, 5s max, 2x multiplier. */
  public static RetryPolicy defaultPolicy() {
    return new RetryPolicy(3, 100, 5000, 2.0);
  }

  /** Creates a new builder for a custom retry policy. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the maximum number of retry attempts after the initial call. */
  public int getMaxRetries() {
    return maxRetries;
  }

  /** Returns the initial backoff duration in milliseconds before the first retry. */
  public long getInitialBackoffMs() {
    return initialBackoffMs;
  }

  /** Returns the maximum backoff duration cap in milliseconds. */
  public long getMaxBackoffMs() {
    return maxBackoffMs;
  }

  /** Returns the backoff multiplier applied after each retry. */
  public double getMultiplier() {
    return multiplier;
  }

  /** Returns {@code true} if this policy enables retries (maxRetries &gt; 0). */
  public boolean isEnabled() {
    return maxRetries > 0;
  }

  @Override
  public String toString() {
    return "RetryPolicy{maxRetries=" + maxRetries
        + ", initialBackoffMs=" + initialBackoffMs
        + ", maxBackoffMs=" + maxBackoffMs
        + ", multiplier=" + multiplier + "}";
  }

  /**
   * Builder for creating custom {@link RetryPolicy} instances.
   */
  public static class Builder {

    private int maxRetries = 3;
    private long initialBackoffMs = 100;
    private long maxBackoffMs = 5000;
    private double multiplier = 2.0;

    Builder() {
    }

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param maxRetries retry count (must be &gt;= 0)
     * @return this builder
     */
    public Builder maxRetries(int maxRetries) {
      if (maxRetries < 0) {
        throw new AxonConfigurationException("maxRetries must be >= 0");
      }
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the initial backoff duration before the first retry.
     *
     * @param initialBackoffMs backoff in milliseconds (must be &gt;= 0)
     * @return this builder
     */
    public Builder initialBackoffMs(long initialBackoffMs) {
      if (initialBackoffMs < 0) {
        throw new AxonConfigurationException("initialBackoffMs must be >= 0");
      }
      this.initialBackoffMs = initialBackoffMs;
      return this;
    }

    /**
     * Sets the maximum backoff cap.
     *
     * @param maxBackoffMs max backoff in milliseconds (must be &gt;= 0)
     * @return this builder
     */
    public Builder maxBackoffMs(long maxBackoffMs) {
      if (maxBackoffMs < 0) {
        throw new AxonConfigurationException("maxBackoffMs must be >= 0");
      }
      this.maxBackoffMs = maxBackoffMs;
      return this;
    }

    /**
     * Sets the backoff multiplier applied after each retry.
     *
     * @param multiplier multiplier (must be &gt;= 1.0)
     * @return this builder
     */
    public Builder multiplier(double multiplier) {
      if (multiplier < 1.0) {
        throw new AxonConfigurationException("multiplier must be >= 1.0");
      }
      this.multiplier = multiplier;
      return this;
    }

    /** Builds the configured {@link RetryPolicy}. */
    public RetryPolicy build() {
      return new RetryPolicy(maxRetries, initialBackoffMs, maxBackoffMs, multiplier);
    }
  }
}
