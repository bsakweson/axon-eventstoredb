package io.github.bsakweson.axon.eventstoredb.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class EventStoreDBMetricsTest {

    private MeterRegistry registry;
    private EventStoreDBMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new EventStoreDBMetrics(registry);
    }

    // ── Events counter ──────────────────────────────────────────────────

    @Test
    void shouldIncrementEventsAppendedCounter() {
        metrics.recordEventsAppended(3);
        metrics.recordEventsAppended(2);

        double count = registry.get("axon.eventstoredb.events.appended").counter().count();
        assertThat(count).isEqualTo(5.0);
    }

    @Test
    void shouldIncrementEventsReadCounter() {
        metrics.recordEventsRead(10);

        double count = registry.get("axon.eventstoredb.events.read").counter().count();
        assertThat(count).isEqualTo(10.0);
    }

    // ── Snapshot counters ───────────────────────────────────────────────

    @Test
    void shouldIncrementSnapshotStoredCounter() {
        metrics.recordSnapshotStored();
        metrics.recordSnapshotStored();

        double count = registry.get("axon.eventstoredb.snapshots.stored").counter().count();
        assertThat(count).isEqualTo(2.0);
    }

    @Test
    void shouldIncrementSnapshotReadCounter() {
        metrics.recordSnapshotRead();

        double count = registry.get("axon.eventstoredb.snapshots.read").counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    // ── Retry counter ───────────────────────────────────────────────────

    @Test
    void shouldIncrementRetryCounter() {
        metrics.recordRetry();
        metrics.recordRetry();
        metrics.recordRetry();

        double count = registry.get("axon.eventstoredb.retries").counter().count();
        assertThat(count).isEqualTo(3.0);
    }

    // ── Error counter ───────────────────────────────────────────────────

    @Test
    void shouldIncrementErrorCounterWithTag() {
        metrics.recordError("append");
        metrics.recordError("append");
        metrics.recordError("read");

        double appendErrors = registry.get("axon.eventstoredb.errors")
                .tag("operation", "append").counter().count();
        double readErrors = registry.get("axon.eventstoredb.errors")
                .tag("operation", "read").counter().count();

        assertThat(appendErrors).isEqualTo(2.0);
        assertThat(readErrors).isEqualTo(1.0);
    }

    // ── Token operation counter ─────────────────────────────────────────

    @Test
    void shouldIncrementTokenOperationWithTag() {
        metrics.recordTokenOperation("store");
        metrics.recordTokenOperation("fetch");
        metrics.recordTokenOperation("store");

        double storeOps = registry.get("axon.eventstoredb.tokens.operations")
                .tag("type", "store").counter().count();
        double fetchOps = registry.get("axon.eventstoredb.tokens.operations")
                .tag("type", "fetch").counter().count();

        assertThat(storeOps).isEqualTo(2.0);
        assertThat(fetchOps).isEqualTo(1.0);
    }

    // ── Timers ──────────────────────────────────────────────────────────

    @Test
    void shouldRecordAppendDuration() {
        Timer.Sample sample = metrics.startTimer();
        // Simulate some work
        metrics.stopAppendTimer(sample);

        long count = registry.get("axon.eventstoredb.append.duration").timer().count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldRecordReadDuration() {
        Timer.Sample sample = metrics.startTimer();
        metrics.stopReadTimer(sample);

        long count = registry.get("axon.eventstoredb.read.duration").timer().count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldSupportMultipleTimerSamples() {
        Timer.Sample sample1 = metrics.startTimer();
        Timer.Sample sample2 = metrics.startTimer();
        metrics.stopAppendTimer(sample1);
        metrics.stopAppendTimer(sample2);

        long count = registry.get("axon.eventstoredb.append.duration").timer().count();
        assertThat(count).isEqualTo(2);
    }
}
