package io.github.bsakweson.axon.eventstoredb.autoconfig;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBEventStorageEngine;
import io.github.bsakweson.axon.eventstoredb.EventStoreDBTokenStore;
import io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics;
import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.resilience.RetryPolicy;
import io.github.bsakweson.axon.eventstoredb.subscriptions.EventStoreDBPersistentSubscriptionMessageSource;
import io.github.bsakweson.axon.eventstoredb.tokenstore.DistributedTokenClaimManager;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.UUID;

import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration for Axon Framework EventStoreDB integration.
 *
 * <p>Activated when:
 * <ul>
 *   <li>{@code EventStoreDBClient} is on the classpath</li>
 *   <li>{@code axon.eventstoredb.enabled=true} is set</li>
 * </ul>
 *
 * <p>Provides:
 * <ul>
 *   <li>{@link EventStoreDBClient} — gRPC client</li>
 *   <li>{@link EventStoreDBStreamNaming} — stream naming conventions</li>
 *   <li>{@link EventStoreDBRetryExecutor} — retry with exponential backoff</li>
 *   <li>{@link EventStoreDBMetrics} — Micrometer instrumentation (when available)</li>
 *   <li>{@link EventStorageEngine} — EventStoreDB-backed event storage</li>
 *   <li>{@link TokenStore} — EventStoreDB-backed tracking token store</li>
 * </ul>
 */
@AutoConfiguration
@AutoConfigureBefore(name = "org.axonframework.springboot.autoconfig.JpaEventStoreAutoConfiguration")
@ConditionalOnClass(EventStoreDBClient.class)
@ConditionalOnProperty(prefix = "axon.eventstoredb", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(EventStoreDBProperties.class)
public class AxonEventStoreDBAutoConfiguration {

  private static final Logger log =
      LoggerFactory.getLogger(AxonEventStoreDBAutoConfiguration.class);

  // ── EventStoreDB Client ────────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean
  public EventStoreDBClient eventStoreDBClient(EventStoreDBProperties properties) {
    String connString = properties.getEffectiveConnectionString();
    log.info("Connecting to EventStoreDB: {}", maskConnectionString(connString));

    EventStoreDBClientSettings settings =
        EventStoreDBConnectionString.parseOrThrow(connString);
    return EventStoreDBClient.create(settings);
  }

  // ── Stream Naming ──────────────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean
  public EventStoreDBStreamNaming eventStoreDBStreamNaming(EventStoreDBProperties properties) {
    return new EventStoreDBStreamNaming(
        properties.getStreamPrefix(),
        properties.getSnapshotStreamPrefix(),
        properties.getTokenStreamPrefix());
  }

  // ── Retry Executor ─────────────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean
  public EventStoreDBRetryExecutor eventStoreDBRetryExecutor(EventStoreDBProperties properties) {
    EventStoreDBProperties.Retry retryConfig = properties.getRetry();
    if (!retryConfig.isEnabled()) {
      log.info("EventStoreDB retry disabled");
      return EventStoreDBRetryExecutor.noRetry();
    }
    RetryPolicy policy = RetryPolicy.builder()
        .maxRetries(retryConfig.getMaxRetries())
        .initialBackoffMs(retryConfig.getInitialBackoffMs())
        .maxBackoffMs(retryConfig.getMaxBackoffMs())
        .multiplier(retryConfig.getMultiplier())
        .build();
    log.info("Configuring EventStoreDB retry: {}", policy);
    return new EventStoreDBRetryExecutor(policy);
  }

  // ── Metrics ─────────────────────────────────────────────────────────────

  @Configuration
  @ConditionalOnClass(MeterRegistry.class)
  static class MetricsAutoConfiguration {

    private static final Logger metricsLog =
        LoggerFactory.getLogger(MetricsAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnProperty(
        prefix = "axon.eventstoredb.metrics", name = "enabled",
        havingValue = "true", matchIfMissing = true)
    public EventStoreDBMetrics eventStoreDBMetrics(MeterRegistry meterRegistry) {
      metricsLog.info("Configuring EventStoreDB Micrometer metrics");
      return new EventStoreDBMetrics(meterRegistry);
    }
  }

  // ── Event Storage Engine ───────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean(EventStorageEngine.class)
  public EventStorageEngine eventStorageEngine(
      EventStoreDBClient client,
      @Qualifier("eventSerializer") Serializer eventSerializer,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties,
      EventStoreDBRetryExecutor retryExecutor,
      @Autowired(required = false) EventUpcaster eventUpcaster,
      @Autowired(required = false) EventStoreDBMetrics metrics) {
    log.info(
        "Configuring EventStoreDB EventStorageEngine (batchSize={}, upcaster={}, metrics={})",
        properties.getBatchSize(),
        eventUpcaster != null ? "yes" : "no",
        metrics != null ? "yes" : "no");
    return new EventStoreDBEventStorageEngine(
        client, eventSerializer, streamNaming, properties.getBatchSize(),
        eventUpcaster, retryExecutor, metrics);
  }

  // ── Persistent Subscriptions ─────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean(EventStoreDBPersistentSubscriptionsClient.class)
  @ConditionalOnProperty(
      prefix = "axon.eventstoredb.subscription", name = "enabled", havingValue = "true")
  public EventStoreDBPersistentSubscriptionsClient eventStoreDBPersistentSubscriptionsClient(
      EventStoreDBProperties properties) {
    String connString = properties.getEffectiveConnectionString();
    EventStoreDBClientSettings settings =
        EventStoreDBConnectionString.parseOrThrow(connString);
    log.info("Creating EventStoreDB persistent subscriptions client");
    return EventStoreDBPersistentSubscriptionsClient.create(settings);
  }

  @Bean
  @ConditionalOnMissingBean(EventStoreDBPersistentSubscriptionMessageSource.class)
  @ConditionalOnProperty(
      prefix = "axon.eventstoredb.subscription", name = "enabled", havingValue = "true")
  public EventStoreDBPersistentSubscriptionMessageSource persistentSubscriptionMessageSource(
      EventStoreDBPersistentSubscriptionsClient subscriptionClient,
      @Qualifier("eventSerializer") Serializer eventSerializer,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties,
      @Autowired(required = false) EventStoreDBMetrics metrics) {

    EventStoreDBProperties.Subscription subProps = properties.getSubscription();
    log.info("Configuring persistent subscription source (group={}, buffer={}, autoCreate={})",
        subProps.getGroupName(), subProps.getBufferSize(), subProps.isCreateIfNotExists());

    EventStoreDBPersistentSubscriptionMessageSource source =
        EventStoreDBPersistentSubscriptionMessageSource.builder()
            .subscriptionClient(subscriptionClient)
            .eventSerializer(eventSerializer)
            .naming(streamNaming)
            .groupName(subProps.getGroupName())
            .bufferSize(subProps.getBufferSize())
            .metrics(metrics)
            .build();

    if (subProps.isCreateIfNotExists()) {
      source.createSubscriptionIfNotExists();
    }

    return source;
  }

  // ── Token Store ────────────────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean(DistributedTokenClaimManager.class)
  @ConditionalOnProperty(
      prefix = "axon.eventstoredb.claims", name = "enabled", havingValue = "true")
  public DistributedTokenClaimManager distributedTokenClaimManager(
      EventStoreDBClient client,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties,
      EventStoreDBRetryExecutor retryExecutor,
      @Autowired(required = false) EventStoreDBMetrics metrics) {

    String nodeId = properties.getNodeId() != null
        ? properties.getNodeId()
        : UUID.randomUUID().toString();
    Duration claimTimeout = Duration.ofSeconds(properties.getClaims().getTimeoutSeconds());

    log.info("Configuring distributed token claim manager (nodeId={}, timeout={}s)",
        nodeId, properties.getClaims().getTimeoutSeconds());

    return new DistributedTokenClaimManager(
        client, streamNaming, nodeId, claimTimeout, retryExecutor, metrics);
  }

  @Bean
  @ConditionalOnMissingBean(TokenStore.class)
  public TokenStore tokenStore(
      EventStoreDBClient client,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties,
      EventStoreDBRetryExecutor retryExecutor,
      @Autowired(required = false) EventStoreDBMetrics metrics,
      @Autowired(required = false) DistributedTokenClaimManager claimManager) {
    String nodeId = properties.getNodeId() != null
        ? properties.getNodeId()
        : UUID.randomUUID().toString();
    return new EventStoreDBTokenStore(
        client, streamNaming, nodeId, retryExecutor, metrics, claimManager);
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  /**
   * Masks credentials in connection strings for safe logging.
   */
  private String maskConnectionString(String connString) {
    return connString.replaceAll("(://[^:]+:)[^@]+(@)", "$1****$2");
  }
}
