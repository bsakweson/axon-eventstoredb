package io.github.bsakweson.axon.eventstoredb.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Micrometer instrumentation for EventStoreDB operations.
 *
 * <p>Exposes the following meters under the {@code axon.eventstoredb.*} prefix:
 * <ul>
 *   <li>{@code events.appended} — total events appended</li>
 *   <li>{@code events.read} — total events read</li>
 *   <li>{@code snapshots.stored} — total snapshots stored</li>
 *   <li>{@code snapshots.read} — total snapshots read</li>
 *   <li>{@code tokens.operations} — token store operations (tagged by type)</li>
 *   <li>{@code append.duration} — histogram of append latencies</li>
 *   <li>{@code read.duration} — histogram of read latencies</li>
 *   <li>{@code errors} — total errors (tagged by operation)</li>
 *   <li>{@code retries} — total retry attempts</li>
 * </ul>
 *
 * <p>Requires {@code io.micrometer:micrometer-core} on the classpath. When used
 * via auto-configuration, this bean is only created if Micrometer is available.
 */
public class EventStoreDBMetrics {

  private static final String PREFIX = "axon.eventstoredb";

  private final MeterRegistry registry;
  private final Counter eventsAppended;
  private final Counter eventsRead;
  private final Counter snapshotsStored;
  private final Counter snapshotsRead;
  private final Counter retries;
  private final Timer appendDuration;
  private final Timer readDuration;

  /**
   * Creates metrics backed by the given Micrometer registry.
   *
   * @param registry the Micrometer meter registry
   */
  public EventStoreDBMetrics(MeterRegistry registry) {
    this.registry = registry;
    this.eventsAppended = Counter.builder(PREFIX + ".events.appended")
        .description("Total events appended to EventStoreDB")
        .register(registry);
    this.eventsRead = Counter.builder(PREFIX + ".events.read")
        .description("Total events read from EventStoreDB")
        .register(registry);
    this.snapshotsStored = Counter.builder(PREFIX + ".snapshots.stored")
        .description("Total snapshots stored in EventStoreDB")
        .register(registry);
    this.snapshotsRead = Counter.builder(PREFIX + ".snapshots.read")
        .description("Total snapshots read from EventStoreDB")
        .register(registry);
    this.retries = Counter.builder(PREFIX + ".retries")
        .description("Total retry attempts for EventStoreDB operations")
        .register(registry);
    this.appendDuration = Timer.builder(PREFIX + ".append.duration")
        .description("Duration of append operations to EventStoreDB")
        .register(registry);
    this.readDuration = Timer.builder(PREFIX + ".read.duration")
        .description("Duration of read operations from EventStoreDB")
        .register(registry);
  }

  /** Increments the events-appended counter by the given count. */
  public void recordEventsAppended(int count) {
    eventsAppended.increment(count);
  }

  /** Increments the events-read counter by the given count. */
  public void recordEventsRead(int count) {
    eventsRead.increment(count);
  }

  /** Increments the snapshots-stored counter. */
  public void recordSnapshotStored() {
    snapshotsStored.increment();
  }

  /** Increments the snapshots-read counter. */
  public void recordSnapshotRead() {
    snapshotsRead.increment();
  }

  /** Increments the retry counter. */
  public void recordRetry() {
    retries.increment();
  }

  /**
   * Records an error for the given operation.
   *
   * @param operation the operation during which the error occurred
   */
  public void recordError(String operation) {
    Counter.builder(PREFIX + ".errors")
        .tag("operation", operation)
        .register(registry)
        .increment();
  }

  /**
   * Records a token store operation of the given type.
   *
   * @param operationType the type of token operation (store, fetch, etc.)
   */
  public void recordTokenOperation(String operationType) {
    Counter.builder(PREFIX + ".tokens.operations")
        .tag("type", operationType)
        .register(registry)
        .increment();
  }

  /** Returns a timer sample that can be stopped against the append timer. */
  public Timer.Sample startTimer() {
    return Timer.start(registry);
  }

  /**
   * Stops the sample and records the duration against the append timer.
   *
   * @param sample the timer sample to stop
   */
  public void stopAppendTimer(Timer.Sample sample) {
    sample.stop(appendDuration);
  }

  /**
   * Stops the sample and records the duration against the read timer.
   *
   * @param sample the timer sample to stop
   */
  public void stopReadTimer(Timer.Sample sample) {
    sample.stop(readDuration);
  }
}
