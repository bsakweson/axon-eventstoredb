package io.github.bsakweson.axon.eventstoredb.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class EventStoreDBStreamNamingTest {

    private final EventStoreDBStreamNaming naming = new EventStoreDBStreamNaming();

    // ── aggregateStream ─────────────────────────────────────────────────

    @Test
    void shouldBuildAggregateStreamName() {
        assertThat(naming.aggregateStream("Order", "abc-123")).isEqualTo("Order-abc-123");
    }

    @Test
    void shouldBuildAggregateStreamWithPrefix() {
        EventStoreDBStreamNaming custom = new EventStoreDBStreamNaming("app", "__snap", "__tok");
        assertThat(custom.aggregateStream("Order", "abc-123")).isEqualTo("app-Order-abc-123");
    }

    @Test
    void shouldBuildAggregateStreamWithEmptyPrefix() {
        EventStoreDBStreamNaming noPrefix = new EventStoreDBStreamNaming("", "__snapshot", "__axon-tokens");
        assertThat(noPrefix.aggregateStream("Order", "abc-123")).isEqualTo("Order-abc-123");
    }

    // ── snapshotStream ──────────────────────────────────────────────────

    @Test
    void shouldBuildSnapshotStreamName() {
        assertThat(naming.snapshotStream("Order", "abc-123")).isEqualTo("__snapshot-Order-abc-123");
    }

    @Test
    void shouldBuildSnapshotStreamWithCustomPrefix() {
        EventStoreDBStreamNaming custom = new EventStoreDBStreamNaming("app", "__snap", "__tok");
        assertThat(custom.snapshotStream("Order", "abc-123")).isEqualTo("__snap-Order-abc-123");
    }

    // ── tokenStream ─────────────────────────────────────────────────────

    @Test
    void shouldBuildTokenStreamName() {
        assertThat(naming.tokenStream("my-processor")).isEqualTo("__axon-tokens-my-processor");
    }

    @Test
    void shouldBuildTokenStreamWithCustomPrefix() {
        EventStoreDBStreamNaming custom = new EventStoreDBStreamNaming("app", "__snap", "__tok");
        assertThat(custom.tokenStream("my-processor")).isEqualTo("__tok-my-processor");
    }

    // ── extractAggregateType ────────────────────────────────────────────

    @Test
    void shouldExtractAggregateType() {
        assertThat(naming.extractAggregateType("Order-abc-123")).isEqualTo("Order");
    }

    @Test
    void shouldExtractAggregateTypeFromPrefixedStream() {
        EventStoreDBStreamNaming custom = new EventStoreDBStreamNaming("app", "__snap", "__tok");
        assertThat(custom.extractAggregateType("app-Order-abc-123")).isEqualTo("Order");
    }

    @Test
    void shouldReturnNullForNoDashStream() {
        assertThat(naming.extractAggregateType("nodash")).isNull();
    }

    // ── extractAggregateIdentifier ──────────────────────────────────────

    @Test
    void shouldExtractAggregateIdentifier() {
        assertThat(naming.extractAggregateIdentifier("Order-abc-123")).isEqualTo("abc-123");
    }

    @Test
    void shouldExtractUuidAggregateIdentifier() {
        String uuid = "b2ec5be0-a477-419b-bacf-d85c7e5bbeb1";
        assertThat(naming.extractAggregateIdentifier("Order-" + uuid)).isEqualTo(uuid);
    }

    @Test
    void shouldReturnNullIdentifierForNoDashStream() {
        assertThat(naming.extractAggregateIdentifier("nodash")).isNull();
    }

    // ── isSystemStream ──────────────────────────────────────────────────

    @Test
    void shouldDetectSnapshotStreamsAsSystem() {
        assertThat(naming.isSystemStream("__snapshot-Order-123")).isTrue();
    }

    @Test
    void shouldDetectTokenStreamsAsSystem() {
        assertThat(naming.isSystemStream("__axon-tokens-processor-0")).isTrue();
    }

    @Test
    void shouldDetectDollarPrefixAsSystem() {
        assertThat(naming.isSystemStream("$all")).isTrue();
        assertThat(naming.isSystemStream("$ce-Order")).isTrue();
    }

    @Test
    void shouldNotDetectAggregateStreamAsSystem() {
        assertThat(naming.isSystemStream("Order-123")).isFalse();
        assertThat(naming.isSystemStream("Product-abc")).isFalse();
    }

    // ── null-safe constructor ───────────────────────────────────────────

    @Test
    void shouldUseDefaultsForNullConstructorArgs() {
        EventStoreDBStreamNaming n = new EventStoreDBStreamNaming(null, null, null);
        assertThat(n.aggregateStream("Order", "1")).isEqualTo("Order-1");
        assertThat(n.snapshotStream("Order", "1")).isEqualTo("__snapshot-Order-1");
        assertThat(n.tokenStream("proc")).isEqualTo("__axon-tokens-proc");
    }

    @Test
    void shouldDetectCustomPrefixedSystemStreams() {
        EventStoreDBStreamNaming custom = new EventStoreDBStreamNaming("", "__snap", "__tok");
        assertThat(custom.isSystemStream("__snap-Order-1")).isTrue();
        assertThat(custom.isSystemStream("__tok-proc")).isTrue();
        assertThat(custom.isSystemStream("Order-1")).isFalse();
    }
}
