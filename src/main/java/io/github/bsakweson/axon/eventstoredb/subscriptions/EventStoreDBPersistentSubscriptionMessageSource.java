package io.github.bsakweson.axon.eventstoredb.subscriptions;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBTrackingToken;
import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBSerializer;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.CreatePersistentSubscriptionToAllOptions;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;
import com.eventstore.dbclient.NackAction;
import com.eventstore.dbclient.PersistentSubscription;
import com.eventstore.dbclient.PersistentSubscriptionListener;
import com.eventstore.dbclient.Position;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.SubscribePersistentSubscriptionOptions;
import com.eventstore.dbclient.SubscriptionFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import jakarta.annotation.Nullable;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericTrackedDomainEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Axon event message source backed by EventStoreDB's persistent subscriptions.
 *
 * <p>Unlike polling-based tracking (which reads batches from {@code $all}), persistent
 * subscriptions let EventStoreDB <b>push</b> events to the consumer. EventStoreDB
 * manages the checkpoint position and supports competing consumers out of the box,
 * enabling horizontal scaling without custom token claim logic.
 *
 * <p>This source subscribes to the {@code $all} stream with a server-side filter that
 * excludes system streams ({@code $}, snapshots, tokens), so only user domain events
 * are delivered.
 *
 * <p>Usage with Axon:
 * <pre>
 * // In your configuration:
 * EventProcessingConfigurer configurer = ...;
 * configurer.registerTrackingEventProcessor("myProcessor",
 *     conf -> persistentSubscriptionMessageSource);
 * </pre>
 *
 * @see PersistentSubscription
 * @see EventStoreDBPersistentSubscriptionsClient
 */
public class EventStoreDBPersistentSubscriptionMessageSource {

  private static final Logger log =
      LoggerFactory.getLogger(EventStoreDBPersistentSubscriptionMessageSource.class);

  private final EventStoreDBPersistentSubscriptionsClient subscriptionClient;
  private final EventStoreDBSerializer serializer;
  private final EventStoreDBStreamNaming naming;
  private final String groupName;
  private final int bufferSize;
  @Nullable private final EventStoreDBMetrics metrics;

  private final BlockingQueue<TrackedEventMessage<?>> eventBuffer;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<PersistentSubscription> activeSubscription =
      new AtomicReference<>();

  /**
   * Creates a new persistent subscription message source.
   *
   * @param subscriptionClient the EventStoreDB persistent subscriptions client
   * @param eventSerializer    the Axon serializer for event payloads
   * @param naming             stream naming strategy
   * @param groupName          the persistent subscription group name
   * @param bufferSize         maximum number of events to buffer locally
   * @param metrics            optional Micrometer metrics (may be null)
   */
  public EventStoreDBPersistentSubscriptionMessageSource(
      EventStoreDBPersistentSubscriptionsClient subscriptionClient,
      Serializer eventSerializer,
      EventStoreDBStreamNaming naming,
      String groupName,
      int bufferSize,
      @Nullable EventStoreDBMetrics metrics) {
    if (subscriptionClient == null) {
      throw new AxonConfigurationException(
          "EventStoreDBPersistentSubscriptionsClient must not be null");
    }
    if (eventSerializer == null) {
      throw new AxonConfigurationException("Event Serializer must not be null");
    }
    if (groupName == null || groupName.isBlank()) {
      throw new AxonConfigurationException(
          "Persistent subscription group name must not be blank");
    }
    this.subscriptionClient = subscriptionClient;
    this.serializer = new EventStoreDBSerializer(eventSerializer);
    this.naming = naming != null ? naming : new EventStoreDBStreamNaming();
    this.groupName = groupName;
    this.bufferSize = bufferSize > 0 ? bufferSize : 256;
    this.metrics = metrics;
    this.eventBuffer = new LinkedBlockingQueue<>(this.bufferSize);
  }

  /**
   * Ensures the persistent subscription group exists on EventStoreDB.
   * If it already exists, this is a no-op.
   *
   * <p>The subscription is created on the {@code $all} stream with a filter
   * that excludes system streams (starting with {@code $}) and the library's
   * internal streams (snapshots, tokens).
   */
  public void createSubscriptionIfNotExists() {
    try {
      SubscriptionFilter filter = SubscriptionFilter.newBuilder()
          .withStreamNameRegularExpression("^(?!\\$)(?!__snapshot)(?!__axon-tokens).*")
          .build();

      CreatePersistentSubscriptionToAllOptions options =
          CreatePersistentSubscriptionToAllOptions.get()
              .fromStart()
              .filter(filter)
              .resolveLinkTos();

      subscriptionClient.createToAll(groupName, options).get();
      log.info("Created persistent subscription group '{}' on $all", groupName);

    } catch (Exception e) {
      if (isAlreadyExistsError(e)) {
        log.debug("Persistent subscription group '{}' already exists", groupName);
      } else {
        throw new EventStoreException(
            "Failed to create persistent subscription group '" + groupName + "'", e);
      }
    }
  }

  /**
   * Checks the full exception cause chain for "already exists" indicators.
   */
  private boolean isAlreadyExistsError(Throwable t) {
    Throwable current = t;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null && (msg.contains("EXIST") || msg.contains("already exists"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Starts the persistent subscription, receiving events from EventStoreDB.
   * Events are buffered in an internal queue for consumption by
   * {@link #readEvents(TrackingToken, boolean)}.
   */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      log.warn("Persistent subscription '{}' is already running", groupName);
      return;
    }

    try {
      SubscribePersistentSubscriptionOptions options =
          SubscribePersistentSubscriptionOptions.get()
              .bufferSize(bufferSize);

      PersistentSubscription subscription =
          subscriptionClient.subscribeToAll(groupName, options,
              new PersistentSubscriptionListener() {
                @Override
                public void onEvent(
                    PersistentSubscription sub, int retryCount, ResolvedEvent event) {
                  handleEvent(sub, event);
                }

                @Override
                public void onCancelled(
                    PersistentSubscription sub, Throwable throwable) {
                  if (throwable != null) {
                    log.error("Persistent subscription '{}' cancelled with error: {}",
                        groupName, throwable.getMessage(), throwable);
                    if (metrics != null) {
                      metrics.recordError("persistentSubscription");
                    }
                  } else {
                    log.info("Persistent subscription '{}' was cancelled", groupName);
                  }
                  running.set(false);
                }
              }).get();

      activeSubscription.set(subscription);
      log.info("Started persistent subscription '{}'", groupName);

    } catch (Exception e) {
      running.set(false);
      throw new EventStoreException(
          "Failed to start persistent subscription '" + groupName + "'", e);
    }
  }

  /**
   * Stops the persistent subscription gracefully.
   */
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    PersistentSubscription subscription = activeSubscription.getAndSet(null);
    if (subscription != null) {
      try {
        subscription.stop();
        log.info("Stopped persistent subscription '{}'", groupName);
      } catch (Exception e) {
        log.warn("Error stopping persistent subscription '{}': {}",
            groupName, e.getMessage());
      }
    }
  }

  /**
   * Reads events from the persistent subscription buffer.
   *
   * <p>This method is compatible with Axon's streaming event processor model.
   * When {@code mayBlock} is true, it will wait briefly for events to arrive
   * in the buffer.
   *
   * @param trackingToken the current tracking token (used for positioning; may be null)
   * @param mayBlock      if true, blocks briefly waiting for events
   * @return a stream of tracked event messages
   */
  public Stream<? extends TrackedEventMessage<?>> readEvents(
      @Nullable TrackingToken trackingToken, boolean mayBlock) {
    List<TrackedEventMessage<?>> batch = new ArrayList<>();
    if (mayBlock) {
      try {
        TrackedEventMessage<?> first = eventBuffer.poll(500, TimeUnit.MILLISECONDS);
        if (first != null) {
          batch.add(first);
          eventBuffer.drainTo(batch, bufferSize - 1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      eventBuffer.drainTo(batch, bufferSize);
    }

    if (metrics != null && !batch.isEmpty()) {
      metrics.recordEventsRead(batch.size());
    }
    return batch.stream();
  }

  /**
   * Returns whether the subscription is currently active.
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Returns the subscription group name.
   */
  public String getGroupName() {
    return groupName;
  }

  private void handleEvent(PersistentSubscription sub, ResolvedEvent resolvedEvent) {
    try {
      String streamId = resolvedEvent.getOriginalEvent().getStreamId();

      // Skip system and internal streams
      if (streamId.startsWith("$") || naming.isSystemStream(streamId)) {
        sub.ack(resolvedEvent);
        return;
      }

      DomainEventMessage<?> domainEvent = serializer.deserialize(resolvedEvent);
      Position position = resolvedEvent.getOriginalEvent().getPosition();
      EventStoreDBTrackingToken token =
          EventStoreDBTrackingToken.of(
              position.getCommitUnsigned(), position.getPrepareUnsigned());

      TrackedEventMessage<?> tracked =
          new GenericTrackedDomainEventMessage<>(token, domainEvent);

      if (!eventBuffer.offer(tracked)) {
        log.warn("Event buffer full for subscription '{}', nacking event", groupName);
        sub.nack(NackAction.Retry, "Buffer full", resolvedEvent);
        if (metrics != null) {
          metrics.recordError("persistentSubscription.bufferFull");
        }
        return;
      }

      sub.ack(resolvedEvent);

      if (metrics != null) {
        metrics.recordEventsRead(1);
      }

    } catch (Exception e) {
      log.error("Failed to process event from subscription '{}': {}",
          groupName, e.getMessage(), e);
      sub.nack(NackAction.Retry, e.getMessage(), resolvedEvent);
      if (metrics != null) {
        metrics.recordError("persistentSubscription.processError");
      }
    }
  }

  /**
   * Builder for creating {@link EventStoreDBPersistentSubscriptionMessageSource} instances.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link EventStoreDBPersistentSubscriptionMessageSource}.
   */
  public static class Builder {

    private EventStoreDBPersistentSubscriptionsClient subscriptionClient;
    private Serializer eventSerializer;
    private EventStoreDBStreamNaming naming;
    private String groupName;
    private int bufferSize = 256;
    private EventStoreDBMetrics metrics;

    Builder() {
    }

    public Builder subscriptionClient(
        EventStoreDBPersistentSubscriptionsClient subscriptionClient) {
      this.subscriptionClient = subscriptionClient;
      return this;
    }

    public Builder eventSerializer(Serializer eventSerializer) {
      this.eventSerializer = eventSerializer;
      return this;
    }

    public Builder naming(EventStoreDBStreamNaming naming) {
      this.naming = naming;
      return this;
    }

    public Builder groupName(String groupName) {
      this.groupName = groupName;
      return this;
    }

    public Builder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder metrics(EventStoreDBMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public EventStoreDBPersistentSubscriptionMessageSource build() {
      return new EventStoreDBPersistentSubscriptionMessageSource(
          subscriptionClient, eventSerializer, naming,
          groupName, bufferSize, metrics);
    }
  }
}
