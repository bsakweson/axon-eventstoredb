package io.github.bsakweson.axon.eventstoredb.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for EventStoreDB integration with Axon Framework.
 *
 * <p>All properties are prefixed with {@code axon.eventstoredb}.
 *
 * <p>Example configuration:
 *
 * <pre>
 * axon:
 *   eventstoredb:
 *     enabled: true
 *     connection-string: "esdb://localhost:2113?tls=false"
 *     batch-size: 256
 *     stream-prefix: ""
 *     snapshot-stream-prefix: "__snapshot"
 *     token-stream-prefix: "__axon-tokens"
 * </pre>
 */
@ConfigurationProperties(prefix = "axon.eventstoredb")
public class EventStoreDBProperties {

  /** Whether EventStoreDB storage engine is enabled. Default: false. */
  private boolean enabled = false;

  /**
   * EventStoreDB connection string. Supports: esdb://, kurrentdb:// Examples: -
   * esdb://localhost:2113?tls=false - esdb://admin:changeit@node1:2113,node2:2113?tls=true -
   * kurrentdb+discover://cluster.example.com:2113
   */
  private String connectionString;

  /** EventStoreDB host. Used when connectionString is not set. Default: localhost. */
  private String host = "localhost";

  /** EventStoreDB gRPC port. Default: 2113. */
  private int port = 2113;

  /** Whether to use TLS. Default: false for dev, true for production. */
  private boolean tls = false;

  /** Whether to verify TLS certificates. Default: true. */
  private boolean tlsVerifyCert = true;

  /** EventStoreDB username. Default: admin. */
  private String username = "admin";

  /** EventStoreDB password. */
  private String password;

  /** Number of events to read per batch. Default: 256. */
  private int batchSize = 256;

  /** Prefix for all aggregate event streams. Empty by default. */
  private String streamPrefix = "";

  /** Prefix for snapshot streams. Default: __snapshot. */
  private String snapshotStreamPrefix = "__snapshot";

  /** Prefix for token streams. Default: __axon-tokens. */
  private String tokenStreamPrefix = "__axon-tokens";

  /**
   * Node identifier for token claim management. Defaults to a random UUID. Set to a stable value
   * in multi-node deployments for claim tracking.
   */
  private String nodeId;

  /** Retry configuration for transient EventStoreDB failures. */
  private Retry retry = new Retry();

  /** Micrometer metrics configuration. */
  private Metrics metrics = new Metrics();

  /** Distributed token claim configuration. */
  private Claims claims = new Claims();

  // ── Getters / Setters ──────────────────────────────────────────────────

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public boolean isTls() {
    return tls;
  }

  public void setTls(boolean tls) {
    this.tls = tls;
  }

  public boolean isTlsVerifyCert() {
    return tlsVerifyCert;
  }

  public void setTlsVerifyCert(boolean tlsVerifyCert) {
    this.tlsVerifyCert = tlsVerifyCert;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getStreamPrefix() {
    return streamPrefix;
  }

  public void setStreamPrefix(String streamPrefix) {
    this.streamPrefix = streamPrefix;
  }

  public String getSnapshotStreamPrefix() {
    return snapshotStreamPrefix;
  }

  public void setSnapshotStreamPrefix(String snapshotStreamPrefix) {
    this.snapshotStreamPrefix = snapshotStreamPrefix;
  }

  public String getTokenStreamPrefix() {
    return tokenStreamPrefix;
  }

  public void setTokenStreamPrefix(String tokenStreamPrefix) {
    this.tokenStreamPrefix = tokenStreamPrefix;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public Retry getRetry() {
    return retry;
  }

  public void setRetry(Retry retry) {
    this.retry = retry;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public Claims getClaims() {
    return claims;
  }

  public void setClaims(Claims claims) {
    this.claims = claims;
  }

  /**
   * Builds the effective connection string, either from {@link #connectionString} if set, or from
   * individual host/port/tls properties.
   */
  public String getEffectiveConnectionString() {
    if (connectionString != null && !connectionString.isBlank()) {
      return connectionString;
    }
    StringBuilder sb = new StringBuilder("esdb://");
    if (username != null && password != null) {
      sb.append(username).append(":").append(password).append("@");
    }
    sb.append(host).append(":").append(port);
    sb.append("?tls=").append(tls);
    if (tls && !tlsVerifyCert) {
      sb.append("&tlsVerifyCert=false");
    }
    return sb.toString();
  }

  /**
   * Retry configuration for transient EventStoreDB connection failures.
   *
   * <p>Example:
   * <pre>
   * axon:
   *   eventstoredb:
   *     retry:
   *       enabled: true
   *       max-retries: 3
   *       initial-backoff-ms: 100
   *       max-backoff-ms: 5000
   *       multiplier: 2.0
   * </pre>
   */
  public static class Retry {

    /** Whether retry is enabled. Default: true. */
    private boolean enabled = true;

    /** Maximum number of retry attempts. Default: 3. */
    private int maxRetries = 3;

    /** Initial backoff in milliseconds. Default: 100. */
    private long initialBackoffMs = 100;

    /** Maximum backoff cap in milliseconds. Default: 5000. */
    private long maxBackoffMs = 5000;

    /** Backoff multiplier after each retry. Default: 2.0. */
    private double multiplier = 2.0;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    public long getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public void setInitialBackoffMs(long initialBackoffMs) {
      this.initialBackoffMs = initialBackoffMs;
    }

    public long getMaxBackoffMs() {
      return maxBackoffMs;
    }

    public void setMaxBackoffMs(long maxBackoffMs) {
      this.maxBackoffMs = maxBackoffMs;
    }

    public double getMultiplier() {
      return multiplier;
    }

    public void setMultiplier(double multiplier) {
      this.multiplier = multiplier;
    }
  }

  /**
   * Micrometer metrics configuration.
   *
   * <p>Example:
   * <pre>
   * axon:
   *   eventstoredb:
   *     metrics:
   *       enabled: true
   * </pre>
   */
  public static class Metrics {

    /** Whether metrics collection is enabled. Default: true. */
    private boolean enabled = true;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }

  /**
   * Distributed token claim configuration for multi-node deployments.
   *
   * <p>When enabled, token claims are enforced using EventStoreDB's optimistic
   * concurrency, ensuring only one node can process a given segment at a time.
   *
   * <p>Example:
   * <pre>
   * axon:
   *   eventstoredb:
   *     claims:
   *       enabled: true
   *       timeout-seconds: 30
   * </pre>
   */
  public static class Claims {

    /** Whether distributed claim management is enabled. Default: false. */
    private boolean enabled = false;

    /** Claim timeout in seconds. Default: 30. */
    private long timeoutSeconds = 30;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public long getTimeoutSeconds() {
      return timeoutSeconds;
    }

    public void setTimeoutSeconds(long timeoutSeconds) {
      this.timeoutSeconds = timeoutSeconds;
    }
  }
}
