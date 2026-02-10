package com.bakalr.axon.eventstoredb.util;

import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventStoreDBSerializerTest {

    private EventStoreDBSerializer serializer;
    private Serializer axonSerializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        axonSerializer = JacksonSerializer.defaultSerializer();
        serializer = new EventStoreDBSerializer(axonSerializer);
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }

    // ── serialize (DomainEventMessage) ──────────────────────────────────

    @Test
    void shouldSerializeDomainEventToEventData() {
        TestEvent payload = new TestEvent("hello", 42);
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-123", 0L, payload);

        EventData eventData = serializer.serialize(domainEvent);

        assertThat(eventData).isNotNull();
        assertThat(eventData.getEventType()).contains("TestEvent");
    }

    @Test
    void shouldPreservePayloadContent() throws Exception {
        TestEvent payload = new TestEvent("test-value", 99);
        String messageId = UUID.randomUUID().toString();
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Product", "prod-456", 5L, payload,
                        MetaData.emptyInstance(), messageId, Instant.now());

        EventData eventData = serializer.serialize(domainEvent);

        byte[] payloadBytes = eventData.getEventData();
        assertThat(payloadBytes).isNotEmpty();
        TestEvent deserialized = objectMapper.readValue(payloadBytes, TestEvent.class);
        assertThat(deserialized.name).isEqualTo("test-value");
        assertThat(deserialized.value).isEqualTo(99);
    }

    @Test
    void shouldIncludeAggregateMetadataInEventData() throws Exception {
        TestEvent payload = new TestEvent("meta-test", 1);
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-789", 3L, payload);

        EventData eventData = serializer.serialize(domainEvent);

        byte[] metadataBytes = eventData.getUserMetadata();
        var metaNode = objectMapper.readTree(metadataBytes);
        assertThat(metaNode.has(EventStoreDBSerializer.META_PAYLOAD_TYPE)).isTrue();
        assertThat(metaNode.has(EventStoreDBSerializer.META_MESSAGE_ID)).isTrue();
        assertThat(metaNode.has(EventStoreDBSerializer.META_TIMESTAMP)).isTrue();
        assertThat(metaNode.get(EventStoreDBSerializer.META_AGGREGATE_TYPE).asText()).isEqualTo("Order");
        assertThat(metaNode.get(EventStoreDBSerializer.META_AGGREGATE_ID).asText()).isEqualTo("order-789");
        assertThat(metaNode.get(EventStoreDBSerializer.META_AGGREGATE_SEQ).asLong()).isEqualTo(3L);
    }

    @Test
    void shouldIncludeAxonMetaDataWhenNonEmpty() throws Exception {
        TestEvent payload = new TestEvent("with-meta", 1);
        MetaData metaData = MetaData.with("correlationId", "abc-123").and("traceId", "xyz-789");
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload, metaData);

        EventData eventData = serializer.serialize(domainEvent);

        byte[] metadataBytes = eventData.getUserMetadata();
        var metaNode = objectMapper.readTree(metadataBytes);
        assertThat(metaNode.has(EventStoreDBSerializer.META_AXON_METADATA)).isTrue();
    }

    @Test
    void shouldOmitAxonMetaDataWhenEmpty() throws Exception {
        TestEvent payload = new TestEvent("no-meta", 1);
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        MetaData.emptyInstance());

        EventData eventData = serializer.serialize(domainEvent);

        byte[] metadataBytes = eventData.getUserMetadata();
        var metaNode = objectMapper.readTree(metadataBytes);
        assertThat(metaNode.has(EventStoreDBSerializer.META_AXON_METADATA)).isFalse();
    }

    @Test
    void shouldShortenFullyQualifiedEventType() {
        TestEvent payload = new TestEvent("data", 1);
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload);

        EventData eventData = serializer.serialize(domainEvent);

        assertThat(eventData.getEventType()).doesNotContain("com.bakalr");
    }

    @Test
    void shouldUseMessageIdentifierAsEventId() {
        TestEvent payload = new TestEvent("data", 1);
        String messageId = UUID.randomUUID().toString();
        GenericDomainEventMessage<TestEvent> domainEvent =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        MetaData.emptyInstance(), messageId, Instant.now());

        EventData eventData = serializer.serialize(domainEvent);

        assertThat(eventData.getEventId().toString()).isEqualTo(messageId);
    }

    // ── deserialize (ResolvedEvent → DomainEventMessage) ────────────────

    @Test
    void shouldDeserializeResolvedEventToDomainEventMessage() throws Exception {
        TestEvent payload = new TestEvent("roundtrip", 77);
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.parse("2026-02-09T10:00:00Z");
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Order", "order-1", 3L, payload,
                        MetaData.emptyInstance(), messageId, timestamp);

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Order-order-1", 3L, eventData.getEventType(),
                UUID.fromString(messageId), timestamp);

        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(resolved.getOriginalEvent()).thenReturn(recorded);

        DomainEventMessage<?> result = serializer.deserialize(resolved);

        assertThat(result.getAggregateIdentifier()).isEqualTo("order-1");
        assertThat(result.getType()).isEqualTo("Order");
        assertThat(result.getSequenceNumber()).isEqualTo(3L);
        assertThat(result.getIdentifier()).isEqualTo(messageId);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
    }

    @Test
    void shouldDeserializeRecordedEventDirectly() throws Exception {
        TestEvent payload = new TestEvent("direct", 55);
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Product", "prod-1", 0L, payload,
                        MetaData.emptyInstance(), messageId, timestamp);

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Product-prod-1", 0L, eventData.getEventType(),
                UUID.fromString(messageId), timestamp);

        DomainEventMessage<?> result = serializer.deserialize(recorded);

        assertThat(result.getPayload()).isInstanceOf(TestEvent.class);
        TestEvent resultPayload = (TestEvent) result.getPayload();
        assertThat(resultPayload.name).isEqualTo("direct");
        assertThat(resultPayload.value).isEqualTo(55);
    }

    @Test
    void shouldDeserializeWithAxonMetaData() throws Exception {
        TestEvent payload = new TestEvent("meta-round", 10);
        MetaData metaData = MetaData.with("correlationId", "corr-123");
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        metaData, messageId, timestamp);

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Order-order-1", 0L, eventData.getEventType(),
                UUID.fromString(messageId), timestamp);

        DomainEventMessage<?> result = serializer.deserialize(recorded);

        assertThat(result.getMetaData()).containsKey("correlationId");
        assertThat(result.getMetaData().get("correlationId")).isEqualTo("corr-123");
    }

    @Test
    void shouldDeserializeWithoutPayloadRevision() throws Exception {
        ObjectNode metaNode = objectMapper.createObjectNode();
        metaNode.put("axon-message-id", UUID.randomUUID().toString());
        metaNode.put("axon-payload-type", TestEvent.class.getName());
        metaNode.put("axon-timestamp", Instant.now().toString());
        metaNode.put("axon-aggregate-type", "Order");
        metaNode.put("axon-aggregate-id", "order-1");
        metaNode.put("axon-aggregate-seq", 0);

        byte[] payloadBytes = objectMapper.writeValueAsBytes(new TestEvent("norev", 1));
        byte[] metaBytes = objectMapper.writeValueAsBytes(metaNode);

        RecordedEvent recorded = mockRecordedEvent(
                payloadBytes, metaBytes, "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        DomainEventMessage<?> result = serializer.deserialize(recorded);
        assertThat(result).isNotNull();
        assertThat(result.getPayload()).isInstanceOf(TestEvent.class);
    }

    @Test
    void shouldDeserializeWithPayloadRevision() throws Exception {
        ObjectNode metaNode = objectMapper.createObjectNode();
        metaNode.put("axon-message-id", UUID.randomUUID().toString());
        metaNode.put("axon-payload-type", TestEvent.class.getName());
        metaNode.put("axon-payload-revision", "2");
        metaNode.put("axon-timestamp", Instant.now().toString());
        metaNode.put("axon-aggregate-type", "Order");
        metaNode.put("axon-aggregate-id", "order-1");
        metaNode.put("axon-aggregate-seq", 0);

        byte[] payloadBytes = objectMapper.writeValueAsBytes(new TestEvent("rev", 2));
        byte[] metaBytes = objectMapper.writeValueAsBytes(metaNode);

        RecordedEvent recorded = mockRecordedEvent(
                payloadBytes, metaBytes, "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        DomainEventMessage<?> result = serializer.deserialize(recorded);
        assertThat(result).isNotNull();
    }

    @Test
    void shouldThrowOnDeserializeWithInvalidMetadata() {
        byte[] invalidMeta = "not-json".getBytes();
        byte[] payloadBytes = "{}".getBytes();

        RecordedEvent recorded = mockRecordedEvent(
                payloadBytes, invalidMeta, "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        assertThatThrownBy(() -> serializer.deserialize(recorded))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to deserialize event");
    }

    // ── deserializePayload ──────────────────────────────────────────────

    @Test
    void shouldDeserializePayloadOnly() throws Exception {
        TestEvent payload = new TestEvent("payload-only", 33);
        String messageId = UUID.randomUUID().toString();
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        MetaData.emptyInstance(), messageId, Instant.now());

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Order-order-1", 0L, eventData.getEventType(),
                UUID.fromString(messageId), Instant.now());

        Object result = serializer.deserializePayload(recorded);

        assertThat(result).isInstanceOf(TestEvent.class);
        TestEvent resultPayload = (TestEvent) result;
        assertThat(resultPayload.name).isEqualTo("payload-only");
        assertThat(resultPayload.value).isEqualTo(33);
    }

    @Test
    void shouldDeserializePayloadWithoutRevision() throws Exception {
        ObjectNode metaNode = objectMapper.createObjectNode();
        metaNode.put("axon-payload-type", TestEvent.class.getName());

        byte[] payloadBytes = objectMapper.writeValueAsBytes(new TestEvent("norev-payload", 1));
        byte[] metaBytes = objectMapper.writeValueAsBytes(metaNode);

        RecordedEvent recorded = mockRecordedEvent(
                payloadBytes, metaBytes, "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        Object result = serializer.deserializePayload(recorded);
        assertThat(result).isInstanceOf(TestEvent.class);
    }

    @Test
    void shouldDeserializePayloadWithRevision() throws Exception {
        ObjectNode metaNode = objectMapper.createObjectNode();
        metaNode.put("axon-payload-type", TestEvent.class.getName());
        metaNode.put("axon-payload-revision", "3");

        byte[] payloadBytes = objectMapper.writeValueAsBytes(new TestEvent("rev-payload", 3));
        byte[] metaBytes = objectMapper.writeValueAsBytes(metaNode);

        RecordedEvent recorded = mockRecordedEvent(
                payloadBytes, metaBytes, "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        Object result = serializer.deserializePayload(recorded);
        assertThat(result).isInstanceOf(TestEvent.class);
    }

    @Test
    void shouldThrowOnDeserializePayloadWithInvalidMetadata() {
        RecordedEvent recorded = mockRecordedEvent(
                "{}".getBytes(), "bad-json".getBytes(), "Order-order-1", 0L, "TestEvent",
                UUID.randomUUID(), Instant.now());

        assertThatThrownBy(() -> serializer.deserializePayload(recorded))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to deserialize event payload");
    }

    // ── extractMetadata ─────────────────────────────────────────────────

    @Test
    void shouldExtractMetadataFromRecordedEvent() throws Exception {
        TestEvent payload = new TestEvent("extract", 1);
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        MetaData.emptyInstance(), messageId, timestamp);

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Order-order-1", 0L, eventData.getEventType(),
                UUID.fromString(messageId), timestamp);

        Map<String, String> metadata = serializer.extractMetadata(recorded);

        assertThat(metadata).containsKey(EventStoreDBSerializer.META_MESSAGE_ID);
        assertThat(metadata).containsKey(EventStoreDBSerializer.META_PAYLOAD_TYPE);
        assertThat(metadata).containsKey(EventStoreDBSerializer.META_TIMESTAMP);
        assertThat(metadata).containsKey(EventStoreDBSerializer.META_AGGREGATE_TYPE);
        assertThat(metadata).doesNotContainKey(EventStoreDBSerializer.META_AXON_METADATA);
    }

    @Test
    void shouldReturnEmptyMapOnInvalidMetadata() {
        RecordedEvent recorded = mockRecordedEvent(
                "{}".getBytes(), "not-json".getBytes(), "Order-x", 0L, "X",
                UUID.randomUUID(), Instant.now());

        Map<String, String> metadata = serializer.extractMetadata(recorded);
        assertThat(metadata).isEmpty();
    }

    @Test
    void shouldExcludeAxonMetadataFromExtracted() throws Exception {
        TestEvent payload = new TestEvent("extract-filter", 1);
        MetaData axonMeta = MetaData.with("key", "value");
        String messageId = UUID.randomUUID().toString();
        GenericDomainEventMessage<TestEvent> original =
                new GenericDomainEventMessage<>("Order", "order-1", 0L, payload,
                        axonMeta, messageId, Instant.now());

        EventData eventData = serializer.serialize(original);

        RecordedEvent recorded = mockRecordedEvent(
                eventData.getEventData(), eventData.getUserMetadata(),
                "Order-order-1", 0L, eventData.getEventType(),
                UUID.fromString(messageId), Instant.now());

        Map<String, String> metadata = serializer.extractMetadata(recorded);
        assertThat(metadata).doesNotContainKey(EventStoreDBSerializer.META_AXON_METADATA);
        assertThat(metadata).containsKey(EventStoreDBSerializer.META_AGGREGATE_TYPE);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private RecordedEvent mockRecordedEvent(
            byte[] eventData, byte[] userMetadata, String streamId,
            long revision, String eventType, UUID eventId, Instant created) {
        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventData()).thenReturn(eventData);
        when(recorded.getUserMetadata()).thenReturn(userMetadata);
        when(recorded.getStreamId()).thenReturn(streamId);
        when(recorded.getRevision()).thenReturn(revision);
        when(recorded.getEventType()).thenReturn(eventType);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getCreated()).thenReturn(created);
        return recorded;
    }

    public static class TestEvent {
        public String name;
        public int value;

        TestEvent() {
        }

        TestEvent(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
