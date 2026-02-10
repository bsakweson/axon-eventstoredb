package io.github.bsakweson.axon.eventstoredb;

import org.axonframework.eventhandling.TrackingToken;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.*;

class EventStoreDBTrackingTokenTest {

    @Test
    void shouldCreateTokenWithPositions() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 99L);
        assertThat(token.getCommitPosition()).isEqualTo(100L);
        assertThat(token.getPreparePosition()).isEqualTo(99L);
        assertThat(token.position()).isEqualTo(OptionalLong.of(100L));
    }

    @Test
    void shouldCreateStartToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.start();
        assertThat(token.getCommitPosition()).isEqualTo(-1L);
        assertThat(token.getPreparePosition()).isEqualTo(-1L);
        assertThat(token.position()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void shouldReturnEmptyPositionForNegativeCommit() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(-5L, -5L);
        assertThat(token.position()).isEmpty();
    }

    @Test
    void shouldReturnPositionForZeroCommit() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(0L, 0L);
        assertThat(token.position()).isPresent();
        assertThat(token.position().getAsLong()).isEqualTo(0L);
    }

    @Test
    void shouldCoverEarlierToken() {
        EventStoreDBTrackingToken earlier = EventStoreDBTrackingToken.of(50L, 50L);
        EventStoreDBTrackingToken later = EventStoreDBTrackingToken.of(100L, 100L);

        assertThat(later.covers(earlier)).isTrue();
        assertThat(earlier.covers(later)).isFalse();
    }

    @Test
    void shouldNotCoverNullToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(50L, 50L);
        assertThat(token.covers(null)).isFalse();
    }

    @Test
    void shouldCoverSameToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);
        assertThat(token.covers(token)).isTrue();
    }

    @Test
    void shouldNotCoverNonEventStoreDBToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 100L);
        TrackingToken other = new org.axonframework.eventhandling.GlobalSequenceTrackingToken(50L);
        assertThat(token.covers(other)).isFalse();
    }

    @Test
    void shouldCompareLexicographically() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(10L, 10L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(20L, 20L);

        assertThat(a.compareTo(b)).isLessThan(0);
        assertThat(b.compareTo(a)).isGreaterThan(0);
        assertThat(a.compareTo(EventStoreDBTrackingToken.of(10L, 10L))).isEqualTo(0);
    }

    @Test
    void shouldCompareByCommitPositionFirst() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(10L, 99L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(20L, 1L);

        assertThat(a.compareTo(b)).isLessThan(0);
    }

    @Test
    void shouldCompareByPreparePositionWhenCommitEqual() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(100L, 50L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(100L, 99L);

        assertThat(a.compareTo(b)).isLessThan(0);
        assertThat(b.compareTo(a)).isGreaterThan(0);
    }

    @Test
    void shouldReturnLowerBound() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(50L, 50L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(100L, 100L);

        assertThat(a.lowerBound(b)).isEqualTo(a);
        assertThat(b.lowerBound(a)).isEqualTo(a);
    }

    @Test
    void shouldReturnUpperBound() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(50L, 50L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(100L, 100L);

        assertThat(a.upperBound(b)).isEqualTo(b);
        assertThat(b.upperBound(a)).isEqualTo(b);
    }

    @Test
    void shouldReturnThisForLowerBoundWithNonEsdbToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(50L, 50L);
        TrackingToken other = new org.axonframework.eventhandling.GlobalSequenceTrackingToken(100L);
        assertThat(token.lowerBound(other)).isSameAs(token);
    }

    @Test
    void shouldReturnThisForUpperBoundWithNonEsdbToken() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(50L, 50L);
        TrackingToken other = new org.axonframework.eventhandling.GlobalSequenceTrackingToken(100L);
        assertThat(token.upperBound(other)).isSameAs(token);
    }

    @Test
    void shouldImplementEqualsAndHashCode() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(100L, 99L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(100L, 99L);
        EventStoreDBTrackingToken c = EventStoreDBTrackingToken.of(200L, 199L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    void shouldNotEqualNull() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 99L);
        assertThat(token).isNotEqualTo(null);
    }

    @Test
    void shouldNotEqualDifferentType() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 99L);
        assertThat(token).isNotEqualTo("not a token");
    }

    @Test
    void shouldEqualSelf() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 99L);
        assertThat(token).isEqualTo(token);
    }

    @Test
    void shouldNotEqualDifferentPreparePosition() {
        EventStoreDBTrackingToken a = EventStoreDBTrackingToken.of(100L, 50L);
        EventStoreDBTrackingToken b = EventStoreDBTrackingToken.of(100L, 99L);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldImplementToString() {
        EventStoreDBTrackingToken token = EventStoreDBTrackingToken.of(100L, 99L);
        String str = token.toString();
        assertThat(str).contains("100");
        assertThat(str).contains("99");
        assertThat(str).contains("EventStoreDBTrackingToken");
    }

    @Test
    void shouldCreateWithConstructorDirectly() {
        EventStoreDBTrackingToken token = new EventStoreDBTrackingToken(42L, 41L);
        assertThat(token.getCommitPosition()).isEqualTo(42L);
        assertThat(token.getPreparePosition()).isEqualTo(41L);
    }
}
