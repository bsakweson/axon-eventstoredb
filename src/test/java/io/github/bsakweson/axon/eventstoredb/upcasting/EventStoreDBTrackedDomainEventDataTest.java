package io.github.bsakweson.axon.eventstoredb.upcasting;

import com.eventstore.dbclient.Position;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken;

import java.time.Instant;
import java.util.UUID;

import org.axonframework.eventhandling.TrackingToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventStoreDBTrackedDomainEventDataTest {

    @Test
    void shouldExtractTrackingToken() {
        UUID eventId = UUID.randomUUID();
        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getEventType()).thenReturn("TestEvent");
        when(recorded.getRevision()).thenReturn(0L);
        when(recorded.getCreated()).thenReturn(Instant.now());
        when(recorded.getEventData()).thenReturn("{}".getBytes());
        when(recorded.getUserMetadata()).thenReturn("{}".getBytes());
        when(recorded.getPosition()).thenReturn(new Position(500L, 500L));

        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(resolved.getOriginalEvent()).thenReturn(recorded);

        EventStoreDBTrackedDomainEventData data =
                new EventStoreDBTrackedDomainEventData(resolved);

        TrackingToken token = data.trackingToken();
        assertThat(token).isInstanceOf(EventStoreDBTrackingToken.class);
        EventStoreDBTrackingToken esdbToken = (EventStoreDBTrackingToken) token;
        assertThat(esdbToken.getCommitPosition()).isEqualTo(500L);
        assertThat(esdbToken.getPreparePosition()).isEqualTo(500L);
    }

    @Test
    void shouldDelegateToDomainEventData() {
        UUID eventId = UUID.randomUUID();
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.parse("2026-06-01T12:00:00Z");

        RecordedEvent recorded = mock(RecordedEvent.class);
        when(recorded.getEventId()).thenReturn(eventId);
        when(recorded.getEventType()).thenReturn("OrderCreated");
        when(recorded.getRevision()).thenReturn(2L);
        when(recorded.getCreated()).thenReturn(timestamp);
        when(recorded.getEventData()).thenReturn("{\"data\":\"test\"}".getBytes());
        when(recorded.getPosition()).thenReturn(new Position(100L, 100L));

        String metadata = String.format(
                "{\"axon-message-id\":\"%s\","
                        + "\"axon-payload-type\":\"com.example.OrderCreated\","
                        + "\"axon-timestamp\":\"%s\","
                        + "\"axon-aggregate-type\":\"Order\","
                        + "\"axon-aggregate-id\":\"order-1\","
                        + "\"axon-aggregate-seq\":2}",
                messageId, timestamp);
        when(recorded.getUserMetadata()).thenReturn(metadata.getBytes());

        ResolvedEvent resolved = mock(ResolvedEvent.class);
        when(resolved.getOriginalEvent()).thenReturn(recorded);

        EventStoreDBTrackedDomainEventData data =
                new EventStoreDBTrackedDomainEventData(resolved);

        assertThat(data.getEventIdentifier()).isEqualTo(messageId);
        assertThat(data.getTimestamp()).isEqualTo(timestamp);
        assertThat(data.getType()).isEqualTo("Order");
        assertThat(data.getAggregateIdentifier()).isEqualTo("order-1");
        assertThat(data.getSequenceNumber()).isEqualTo(2L);
        assertThat(data.getPayload()).isNotNull();
        assertThat(data.getMetaData()).isNotNull();
    }
}
