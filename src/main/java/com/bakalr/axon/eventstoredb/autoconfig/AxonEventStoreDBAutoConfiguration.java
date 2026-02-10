package com.bakalr.axon.eventstoredb.autoconfig;

import com.bakalr.axon.eventstoredb.EventStoreDBEventStorageEngine;
import com.bakalr.axon.eventstoredb.EventStoreDBTokenStore;
import com.bakalr.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

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
 *   <li>{@code EventStoreDBSerializer} — Axon ↔ EventStoreDB serialization bridge (used internally)</li>
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

  // ── Event Storage Engine ───────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean(EventStorageEngine.class)
  public EventStorageEngine eventStorageEngine(
      EventStoreDBClient client,
      @Qualifier("eventSerializer") Serializer eventSerializer,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties) {
    log.info("Configuring EventStoreDB EventStorageEngine (batchSize={})", properties.getBatchSize());
    return new EventStoreDBEventStorageEngine(
        client, eventSerializer, streamNaming, properties.getBatchSize());
  }

  // ── Token Store ────────────────────────────────────────────────────────

  @Bean
  @ConditionalOnMissingBean(TokenStore.class)
  public TokenStore tokenStore(
      EventStoreDBClient client,
      EventStoreDBStreamNaming streamNaming,
      EventStoreDBProperties properties) {
    String nodeId = properties.getNodeId() != null
        ? properties.getNodeId()
        : UUID.randomUUID().toString();
    return new EventStoreDBTokenStore(client, streamNaming, nodeId);
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  /**
   * Masks credentials in connection strings for safe logging.
   */
  private String maskConnectionString(String connString) {
    if (connString == null) return "null";
    // Mask password in esdb://user:password@host pattern
    return connString.replaceAll("(://[^:]+:)[^@]+(@)", "$1****$2");
  }
}
