package io.github.bsakweson.axon.eventstoredb.util;

import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges Axon Framework's serialization with EventStoreDB's event format.
 *
 * <p>Converts between Axon {@link EventMessage}/{@link DomainEventMessage} and EventStoreDB's
 * {@link EventData}/{@link RecordedEvent}. Event payloads are serialized as JSON using Axon's
 * configured {@link Serializer}. Axon metadata (message ID, aggregate info, timestamps) is stored
 * in EventStoreDB's event metadata field.
 */
public class EventStoreDBSerializer {

  private static final Logger log = LoggerFactory.getLogger(EventStoreDBSerializer.class);

  // Metadata keys stored in EventStoreDB event metadata
  static final String META_MESSAGE_ID = "axon-message-id";
  static final String META_PAYLOAD_TYPE = "axon-payload-type";
  static final String META_PAYLOAD_REVISION = "axon-payload-revision";
  static final String META_TIMESTAMP = "axon-timestamp";
  static final String META_AGGREGATE_TYPE = "axon-aggregate-type";
  static final String META_AGGREGATE_ID = "axon-aggregate-id";
  static final String META_AGGREGATE_SEQ = "axon-aggregate-seq";
  static final String META_AXON_METADATA = "axon-metadata";

  private final Serializer eventSerializer;
  private final ObjectMapper objectMapper;

  public EventStoreDBSerializer(Serializer eventSerializer) {
    this.eventSerializer = eventSerializer;
    this.objectMapper = new ObjectMapper();
    this.objectMapper.findAndRegisterModules(); // Register JSR-310 etc.
  }

  /**
   * Converts an Axon {@link EventMessage} to EventStoreDB's {@link EventData}.
   *
   * @param eventMessage the Axon event message
   * @return EventStoreDB event data ready for appending
   */
  public EventData serialize(EventMessage<?> eventMessage) {
    try {
      // Serialize the payload using Axon's serializer
      SerializedObject<byte[]> serializedPayload =
          eventSerializer.serialize(eventMessage.getPayload(), byte[].class);

      // Build metadata JSON
      ObjectNode metadataNode = objectMapper.createObjectNode();
      metadataNode.put(META_MESSAGE_ID, eventMessage.getIdentifier());
      metadataNode.put(META_PAYLOAD_TYPE, serializedPayload.getType().getName());
      if (serializedPayload.getType().getRevision() != null) {
        metadataNode.put(META_PAYLOAD_REVISION, serializedPayload.getType().getRevision());
      }
      metadataNode.put(META_TIMESTAMP, eventMessage.getTimestamp().toString());

      // Add domain event metadata if applicable
      if (eventMessage instanceof DomainEventMessage<?> domainEvent) {
        metadataNode.put(META_AGGREGATE_TYPE, domainEvent.getType());
        metadataNode.put(META_AGGREGATE_ID, domainEvent.getAggregateIdentifier());
        metadataNode.put(META_AGGREGATE_SEQ, domainEvent.getSequenceNumber());
      }

      // Serialize Axon MetaData
      if (!eventMessage.getMetaData().isEmpty()) {
        SerializedObject<byte[]> serializedMeta =
            eventSerializer.serialize(eventMessage.getMetaData(), byte[].class);
        JsonNode axonMetaNode = objectMapper.readTree(serializedMeta.getData());
        metadataNode.set(META_AXON_METADATA, axonMetaNode);
      }

      byte[] metadataBytes = objectMapper.writeValueAsBytes(metadataNode);

      // Use the Axon payload type name as the EventStoreDB event type
      String eventType = serializedPayload.getType().getName();
      // Simplify: use short class name if it's a fully qualified name
      int lastDot = eventType.lastIndexOf('.');
      if (lastDot >= 0) {
        eventType = eventType.substring(lastDot + 1);
      }

      // Build EventStoreDB event
      UUID eventId = UUID.fromString(eventMessage.getIdentifier());
      return EventDataBuilder.json(eventId, eventType, serializedPayload.getData())
          .metadataAsBytes(metadataBytes)
          .build();

    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize event metadata", e);
    }
  }

  /**
   * Converts an EventStoreDB {@link ResolvedEvent} to an Axon {@link DomainEventMessage}.
   *
   * @param resolvedEvent the EventStoreDB resolved event
   * @return an Axon domain event message
   */
  public DomainEventMessage<?> deserialize(ResolvedEvent resolvedEvent) {
    RecordedEvent recorded = resolvedEvent.getOriginalEvent();
    return deserialize(recorded);
  }

  /**
   * Converts an EventStoreDB {@link RecordedEvent} to an Axon {@link DomainEventMessage}.
   *
   * @param recorded the EventStoreDB recorded event
   * @return an Axon domain event message
   */
  public DomainEventMessage<?> deserialize(RecordedEvent recorded) {
    try {
      // Parse metadata
      byte[] metadataBytes = recorded.getUserMetadata();
      JsonNode metadataNode = objectMapper.readTree(metadataBytes);

      String messageId = metadataNode.path(META_MESSAGE_ID).asText(recorded.getEventId().toString());
      String payloadType = metadataNode.path(META_PAYLOAD_TYPE).asText(recorded.getEventType());
      String payloadRevision = metadataNode.has(META_PAYLOAD_REVISION)
          ? metadataNode.get(META_PAYLOAD_REVISION).asText()
          : null;
      Instant timestamp = Instant.parse(
          metadataNode.path(META_TIMESTAMP).asText(recorded.getCreated().toString()));
      String aggregateType = metadataNode.path(META_AGGREGATE_TYPE).asText("");
      String aggregateId = metadataNode.path(META_AGGREGATE_ID).asText("");
      long aggregateSeq = metadataNode.path(META_AGGREGATE_SEQ).asLong(
          recorded.getRevision());

      // Deserialize Axon MetaData
      MetaData axonMetaData = MetaData.emptyInstance();
      if (metadataNode.has(META_AXON_METADATA)) {
        byte[] axonMetaBytes = objectMapper.writeValueAsBytes(metadataNode.get(META_AXON_METADATA));
        SimpleSerializedType metaType = new SimpleSerializedType(MetaData.class.getName(), null);
        @SuppressWarnings("unchecked")
        SerializedObject<byte[]> serializedMeta =
            new SimpleSerializedObject<>(axonMetaBytes, byte[].class, metaType);
        axonMetaData = eventSerializer.deserialize(serializedMeta);
      }

      // Deserialize the payload
      SimpleSerializedType serializedType = new SimpleSerializedType(payloadType, payloadRevision);
      byte[] payloadBytes = recorded.getEventData();
      SerializedObject<byte[]> serializedPayload =
          new SimpleSerializedObject<>(payloadBytes, byte[].class, serializedType);
      Object payload = eventSerializer.deserialize(serializedPayload);

      // Build the Axon domain event message
      return new GenericDomainEventMessage<>(
          aggregateType,
          aggregateId,
          aggregateSeq,
          payload,
          axonMetaData,
          messageId,
          timestamp);

    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to deserialize event from stream '"
              + recorded.getStreamId()
              + "' at revision "
              + recorded.getRevision(),
          e);
    }
  }

  /**
   * Deserializes only the payload portion of an event without full metadata parsing. Useful for
   * snapshot deserialization where aggregate metadata is known from context.
   */
  public Object deserializePayload(RecordedEvent recorded) {
    try {
      byte[] metadataBytes = recorded.getUserMetadata();
      JsonNode metadataNode = objectMapper.readTree(metadataBytes);

      String payloadType = metadataNode.path(META_PAYLOAD_TYPE).asText(recorded.getEventType());
      String payloadRevision = metadataNode.has(META_PAYLOAD_REVISION)
          ? metadataNode.get(META_PAYLOAD_REVISION).asText()
          : null;

      SimpleSerializedType serializedType = new SimpleSerializedType(payloadType, payloadRevision);
      byte[] payloadBytes = recorded.getEventData();
      SerializedObject<byte[]> serializedPayload =
          new SimpleSerializedObject<>(payloadBytes, byte[].class, serializedType);
      return eventSerializer.deserialize(serializedPayload);

    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize event payload", e);
    }
  }

  /** Returns a map of metadata key-value pairs for diagnostic/debugging purposes. */
  public Map<String, String> extractMetadata(RecordedEvent recorded) {
    try {
      byte[] metadataBytes = recorded.getUserMetadata();
      JsonNode metadataNode = objectMapper.readTree(metadataBytes);
      Map<String, String> result = new HashMap<>();
      metadataNode
          .properties()
          .forEach(
              entry -> {
                if (!entry.getKey().equals(META_AXON_METADATA)) {
                  result.put(entry.getKey(), entry.getValue().asText());
                }
              });
      return result;
    } catch (IOException e) {
      log.warn("Failed to extract metadata from event {}", recorded.getEventId(), e);
      return Map.of();
    }
  }
}
