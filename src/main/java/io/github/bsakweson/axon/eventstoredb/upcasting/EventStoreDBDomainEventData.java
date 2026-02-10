package io.github.bsakweson.axon.eventstoredb.upcasting;

import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_AGGREGATE_ID;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_AGGREGATE_SEQ;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_AGGREGATE_TYPE;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_AXON_METADATA;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_MESSAGE_ID;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_PAYLOAD_REVISION;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_PAYLOAD_TYPE;
import static io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer.META_TIMESTAMP;

import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;

/**
 * Adapter that wraps an EventStoreDB {@link RecordedEvent} as Axon's
 * {@link DomainEventData}.
 *
 * <p>This enables Axon's standard event upcasting pipeline: events read from
 * EventStoreDB are wrapped in this adapter, then passed through the
 * {@link org.axonframework.serialization.upcasting.event.EventUpcaster} chain
 * before final deserialization into domain event messages.
 */
public class EventStoreDBDomainEventData implements DomainEventData<byte[]> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final byte[] EMPTY_META =
      "{}".getBytes(StandardCharsets.UTF_8);

  private final String eventIdentifier;
  private final Instant timestamp;
  private final String aggregateIdentifier;
  private final String aggregateType;
  private final long sequenceNumber;
  private final byte[] payload;
  private final String payloadType;
  private final String payloadRevision;
  private final byte[] axonMetadata;

  /**
   * Creates domain event data from a resolved event.
   *
   * @param resolvedEvent the EventStoreDB resolved event
   */
  public EventStoreDBDomainEventData(ResolvedEvent resolvedEvent) {
    this(resolvedEvent.getOriginalEvent());
  }

  /**
   * Creates domain event data from a recorded event.
   *
   * @param recorded the EventStoreDB recorded event
   */
  public EventStoreDBDomainEventData(RecordedEvent recorded) {
    try {
      byte[] metadataBytes = recorded.getUserMetadata();
      JsonNode metadataNode = MAPPER.readTree(metadataBytes);

      this.eventIdentifier = metadataNode
          .path(META_MESSAGE_ID)
          .asText(recorded.getEventId().toString());
      this.timestamp = Instant.parse(
          metadataNode.path(META_TIMESTAMP)
              .asText(recorded.getCreated().toString()));
      this.aggregateType = metadataNode
          .path(META_AGGREGATE_TYPE).asText("");
      this.aggregateIdentifier = metadataNode
          .path(META_AGGREGATE_ID).asText("");
      this.sequenceNumber = metadataNode
          .path(META_AGGREGATE_SEQ)
          .asLong(recorded.getRevision());
      this.payloadType = metadataNode
          .path(META_PAYLOAD_TYPE)
          .asText(recorded.getEventType());
      this.payloadRevision =
          metadataNode.has(META_PAYLOAD_REVISION)
              ? metadataNode.get(META_PAYLOAD_REVISION).asText()
              : null;

      this.payload = recorded.getEventData();

      if (metadataNode.has(META_AXON_METADATA)) {
        this.axonMetadata = MAPPER.writeValueAsBytes(
            metadataNode.get(META_AXON_METADATA));
      } else {
        this.axonMetadata = EMPTY_META;
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to parse EventStoreDB event metadata for event "
              + recorded.getEventId(), e);
    }
  }

  @Override
  public String getEventIdentifier() {
    return eventIdentifier;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public SerializedObject<byte[]> getMetaData() {
    return new SimpleSerializedObject<>(
        axonMetadata, byte[].class,
        new SimpleSerializedType(
            "org.axonframework.messaging.MetaData", null));
  }

  @Override
  public SerializedObject<byte[]> getPayload() {
    return new SimpleSerializedObject<>(
        payload, byte[].class,
        new SimpleSerializedType(payloadType, payloadRevision));
  }

  @Override
  public String getAggregateIdentifier() {
    return aggregateIdentifier;
  }

  @Override
  public String getType() {
    return aggregateType;
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }
}
