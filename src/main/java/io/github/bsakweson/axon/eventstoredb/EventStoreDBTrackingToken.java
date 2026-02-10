package io.github.bsakweson.axon.eventstoredb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;
import java.util.OptionalLong;

import org.axonframework.eventhandling.TrackingToken;

/**
 * Axon {@link TrackingToken} implementation backed by EventStoreDB's global stream position.
 *
 * <p>EventStoreDB's {@code $all} stream uses a {@code (commitPosition, preparePosition)} tuple
 * to uniquely identify each event's position in the global log. This token wraps that position
 * so Axon tracking processors can resume from where they left off.
 *
 * <p>Token ordering is based on {@code commitPosition} — a higher commit position means a
 * later event in the global order.
 */
public class EventStoreDBTrackingToken
    implements TrackingToken, Comparable<EventStoreDBTrackingToken>, Serializable {

  private static final long serialVersionUID = 1L;

  private final long commitPosition;
  private final long preparePosition;

  @JsonCreator
  public EventStoreDBTrackingToken(
      @JsonProperty("commitPosition") long commitPosition,
      @JsonProperty("preparePosition") long preparePosition) {
    this.commitPosition = commitPosition;
    this.preparePosition = preparePosition;
  }

  /**
   * Creates a token from EventStoreDB's Position object values.
   *
   * @param commitPosition the commit position in the transaction log
   * @param preparePosition the prepare position in the transaction log
   * @return a new tracking token
   */
  public static EventStoreDBTrackingToken of(long commitPosition, long preparePosition) {
    return new EventStoreDBTrackingToken(commitPosition, preparePosition);
  }

  /** Token representing the very start of the global stream. */
  public static EventStoreDBTrackingToken start() {
    return new EventStoreDBTrackingToken(-1L, -1L);
  }

  public long getCommitPosition() {
    return commitPosition;
  }

  public long getPreparePosition() {
    return preparePosition;
  }

  /**
   * Returns a position value suitable for use with EventStoreDB's {@code ReadAllOptions}. The
   * commit position serves as the primary ordering key.
   */
  @Override
  public OptionalLong position() {
    return commitPosition >= 0 ? OptionalLong.of(commitPosition) : OptionalLong.empty();
  }

  /**
   * Returns {@code true} if this token's position is at or beyond the given other token's
   * position. Used by Axon to determine if a processor has already seen a particular event.
   */
  @Override
  public boolean covers(TrackingToken other) {
    if (other instanceof EventStoreDBTrackingToken otherToken) {
      return this.commitPosition >= otherToken.commitPosition;
    }
    return false;
  }

  @Override
  public TrackingToken lowerBound(TrackingToken other) {
    if (other instanceof EventStoreDBTrackingToken otherToken) {
      if (this.commitPosition <= otherToken.commitPosition) {
        return this;
      }
      return otherToken;
    }
    return this;
  }

  @Override
  public TrackingToken upperBound(TrackingToken other) {
    if (other instanceof EventStoreDBTrackingToken otherToken) {
      if (this.commitPosition >= otherToken.commitPosition) {
        return this;
      }
      return otherToken;
    }
    return this;
  }

  @Override
  public int compareTo(EventStoreDBTrackingToken other) {
    int result = Long.compare(this.commitPosition, other.commitPosition);
    if (result == 0) {
      result = Long.compare(this.preparePosition, other.preparePosition);
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventStoreDBTrackingToken that = (EventStoreDBTrackingToken) o;
    return commitPosition == that.commitPosition && preparePosition == that.preparePosition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitPosition, preparePosition);
  }

  @Override
  public String toString() {
    return "EventStoreDBTrackingToken{commit="
        + commitPosition
        + ", prepare="
        + preparePosition
        + "}";
  }
}
