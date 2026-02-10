package io.github.bsakweson.axon.eventstoredb.upcasting;

import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;

import java.time.Instant;
import java.util.UUID;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.serialization.SerializedObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventStoreDBDomainEventDataTest {

    @Test
    void shouldExtractFieldsFromMetadata() {
        UUID eventId = UUID.randomUUID();
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.parse("2026-06-01T10:00:00Z");

        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getEventType()).thenReturn("OrderCreated");
        when(recorded.getRevision()).thenReturn(0L);
        when(recorded.getCreated()).thenReturn(timestamp);
        when(recorded.getEventData()).thenReturn("{\"orderId\":\"123\"}".getBytes());

        String metadata = String.format(
                "{\"axon-message-id\":\"%s\","
                        + "\"axon-payload-type\":\"com.example.OrderCreated\","
                        + "\"axon-payload-revision\":\"2\","
                        + "\"axon-timestamp\":\"%s\","
                        + "\"axon-aggregate-type\":\"Order\","
                        + "\"axon-aggregate-id\":\"order-123\","
                        + "\"axon-aggregate-seq\":5,"
                        + "\"axon-metadata\":{\"traceId\":\"abc\"}}",
                messageId, timestamp);
        when(recorded.getUserMetadata()).thenReturn(metadata.getBytes());

        DomainEventData<byte[]> data = new EventStoreDBDomainEventData(recorded);

        assertThat(data.getEventIdentifier()).isEqualTo(messageId);
        assertThat(data.getTimestamp()).isEqualTo(timestamp);
        assertThat(data.getType()).isEqualTo("Order");
        assertThat(data.getAggregateIdentifier()).isEqualTo("order-123");
        assertThat(data.getSequenceNumber()).isEqualTo(5L);

        // Payload
        SerializedObject<byte[]> payload = data.getPayload();
        assertThat(payload.getType().getName()).isEqualTo("com.example.OrderCreated");
        assertThat(payload.getType().getRevision()).isEqualTo("2");
        assertThat(new String(payload.getData())).contains("orderId");

        // Metadata
        SerializedObject<byte[]> meta = data.getMetaData();
        assertThat(new String(meta.getData())).contains("traceId");
    }

    @Test
    void shouldFallbackToRecordedEventFieldsWhenMetadataMissing() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();

        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getEventType()).thenReturn("SomeEvent");
        when(recorded.getRevision()).thenReturn(3L);
        when(recorded.getCreated()).thenReturn(now);
        when(recorded.getEventData()).thenReturn("{}".getBytes());
        // Minimal metadata — no Axon fields
        when(recorded.getUserMetadata()).thenReturn("{}".getBytes());

        DomainEventData<byte[]> data = new EventStoreDBDomainEventData(recorded);

        // Falls back to event ID
        assertThat(data.getEventIdentifier()).isEqualTo(eventId.toString());
        // Falls back to event type
        assertThat(data.getPayload().getType().getName()).isEqualTo("SomeEvent");
        // Falls back to revision
        assertThat(data.getSequenceNumber()).isEqualTo(3L);
        // Empty aggregate fields
        assertThat(data.getType()).isEmpty();
        assertThat(data.getAggregateIdentifier()).isEmpty();
        // No payload revision
        assertThat(data.getPayload().getType().getRevision()).isNull();
        // Empty metadata → empty JSON
        assertThat(new String(data.getMetaData().getData())).isEqualTo("{}");
    }

    @Test
    void shouldConstructFromResolvedEvent() {
        UUID eventId = UUID.randomUUID();
        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getEventType()).thenReturn("TestEvent");
        when(recorded.getRevision()).thenReturn(0L);
        when(recorded.getCreated()).thenReturn(Instant.now());
        when(recorded.getEventData()).thenReturn("{}".getBytes());
        when(recorded.getUserMetadata()).thenReturn("{}".getBytes());

        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(resolved.getOriginalEvent()).thenReturn(recorded);

        DomainEventData<byte[]> data = new EventStoreDBDomainEventData(resolved);
        assertThat(data.getEventIdentifier()).isEqualTo(eventId.toString());
    }

    @Test
    void shouldThrowOnInvalidMetadataJson() {
        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(UUID.randomUUID());
        when(recorded.getUserMetadata()).thenReturn("not valid json".getBytes());

        assertThatThrownBy(() -> new EventStoreDBDomainEventData(recorded))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to parse");
    }
}
