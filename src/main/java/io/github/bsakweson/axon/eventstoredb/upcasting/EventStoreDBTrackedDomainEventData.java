package io.github.bsakweson.axon.eventstoredb.upcasting;

import com.eventstore.dbclient.Position;
import com.eventstore.dbclient.ResolvedEvent;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.serialization.SerializedObject;

import java.time.Instant;

/**
 * Combines {@link EventStoreDBDomainEventData} with a {@link TrackingToken}
 * for tracking processor integration.
 *
 * <p>The tracking token captures the EventStoreDB global position
 * (commit + prepare) so that event processors can resume from the correct
 * position.
 *
 * <p>This class implements both {@link DomainEventData} and {@link TrackedEventData},
 * allowing Axon's
 * {@link org.axonframework.serialization.upcasting.event.InitialEventRepresentation}
 * to extract aggregate info and tracking position simultaneously.
 * Implementing {@code TrackedEventData} is required for the upcasting pipeline
 * to propagate the tracking token through {@code InitialEventRepresentation}.
 */
public class EventStoreDBTrackedDomainEventData
    implements DomainEventData<byte[]>, TrackedEventData<byte[]> {

  private final EventStoreDBDomainEventData delegate;
  private final TrackingToken trackingToken;

  /**
   * Creates tracked domain event data from a resolved event.
   *
   * @param resolvedEvent the EventStoreDB resolved event
   */
  public EventStoreDBTrackedDomainEventData(ResolvedEvent resolvedEvent) {
    this.delegate = new EventStoreDBDomainEventData(resolvedEvent);
    Position position =
        resolvedEvent.getOriginalEvent().getPosition();
    this.trackingToken = EventStoreDBTrackingToken.of(
        position.getCommitUnsigned(), position.getPrepareUnsigned());
  }

  /** Returns the tracking token for this event's global position. */
  public TrackingToken trackingToken() {
    return trackingToken;
  }

  @Override
  public String getEventIdentifier() {
    return delegate.getEventIdentifier();
  }

  @Override
  public Instant getTimestamp() {
    return delegate.getTimestamp();
  }

  @Override
  public SerializedObject<byte[]> getMetaData() {
    return delegate.getMetaData();
  }

  @Override
  public SerializedObject<byte[]> getPayload() {
    return delegate.getPayload();
  }

  @Override
  public String getAggregateIdentifier() {
    return delegate.getAggregateIdentifier();
  }

  @Override
  public String getType() {
    return delegate.getType();
  }

  @Override
  public long getSequenceNumber() {
    return delegate.getSequenceNumber();
  }
}
