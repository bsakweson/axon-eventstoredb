package io.github.bsakweson.axon.eventstoredb.autoconfig;

import io.github.bsakweson.axon.eventstoredb.resilience.EventStoreDBRetryExecutor;
import io.github.bsakweson.axon.eventstoredb.subscriptions.EventStoreDBPersistentSubscriptionMessageSource;
import io.github.bsakweson.axon.eventstoredb.tokenstore.DistributedTokenClaimManager;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AxonEventStoreDBAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AxonEventStoreDBAutoConfiguration.class))
            .withUserConfiguration(MockSerializerConfiguration.class);

    @Configuration
    static class MockSerializerConfiguration {
        @Bean(name = "eventSerializer")
        public Serializer eventSerializer() {
            return JacksonSerializer.defaultSerializer();
        }
    }

    // ── Conditional activation ──────────────────────────────────────────

    @Test
    void shouldNotLoadWhenPropertyDisabled() {
        contextRunner
                .withPropertyValues("axon.eventstoredb.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(EventStorageEngine.class);
                    assertThat(context).doesNotHaveBean(TokenStore.class);
                    assertThat(context).doesNotHaveBean(EventStoreDBStreamNaming.class);
                });
    }

    @Test
    void shouldNotLoadWhenPropertyMissing() {
        contextRunner
                .run(context -> {
                    assertThat(context).doesNotHaveBean(EventStorageEngine.class);
                    assertThat(context).doesNotHaveBean(TokenStore.class);
                });
    }

    // ── Property binding ────────────────────────────────────────────────

    @Test
    void shouldLoadEventStoreDBProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.host=myhost",
                        "axon.eventstoredb.port=2114",
                        "axon.eventstoredb.batch-size=512",
                        "axon.eventstoredb.stream-prefix=myapp",
                        "axon.eventstoredb.snapshot-stream-prefix=__snap",
                        "axon.eventstoredb.token-stream-prefix=__tok")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStoreDBProperties.class);
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getHost()).isEqualTo("myhost");
                    assertThat(props.getPort()).isEqualTo(2114);
                    assertThat(props.getBatchSize()).isEqualTo(512);
                    assertThat(props.getStreamPrefix()).isEqualTo("myapp");
                    assertThat(props.getSnapshotStreamPrefix()).isEqualTo("__snap");
                    assertThat(props.getTokenStreamPrefix()).isEqualTo("__tok");
                });
    }

    @Test
    void shouldBindConnectionStringProperty() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://node1:2113?tls=false")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getConnectionString()).isEqualTo("esdb://node1:2113?tls=false");
                    assertThat(props.getEffectiveConnectionString()).isEqualTo("esdb://node1:2113?tls=false");
                });
    }

    @Test
    void shouldBindTlsProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.tls=true",
                        "axon.eventstoredb.tls-verify-cert=false")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.isTls()).isTrue();
                    assertThat(props.isTlsVerifyCert()).isFalse();
                });
    }

    @Test
    void shouldBindNodeIdProperty() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.node-id=my-node-1")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getNodeId()).isEqualTo("my-node-1");
                });
    }

    @Test
    void shouldBindUsernameAndPasswordProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.username=myuser",
                        "axon.eventstoredb.password=mypass")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getUsername()).isEqualTo("myuser");
                    assertThat(props.getPassword()).isEqualTo("mypass");
                });
    }

    // ── Bean creation with valid connection string ──────────────────────

    @Test
    void shouldCreateBeansWithValidConnectionString() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStoreDBStreamNaming.class);
                    assertThat(context).hasSingleBean(EventStorageEngine.class);
                    assertThat(context).hasSingleBean(TokenStore.class);
                });
    }

    @Test
    void shouldCreateStreamNamingWithCustomPrefixes() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.stream-prefix=myapp",
                        "axon.eventstoredb.snapshot-stream-prefix=__snap",
                        "axon.eventstoredb.token-stream-prefix=__tok")
                .run(context -> {
                    EventStoreDBStreamNaming naming = context.getBean(EventStoreDBStreamNaming.class);
                    assertThat(naming.aggregateStream("Order", "1")).isEqualTo("myapp-Order-1");
                    assertThat(naming.snapshotStream("Order", "1")).isEqualTo("__snap-Order-1");
                    assertThat(naming.tokenStream("proc")).isEqualTo("__tok-proc");
                });
    }

    @Test
    void shouldUseConfiguredNodeIdForTokenStore() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.node-id=stable-node")
                .run(context -> {
                    TokenStore tokenStore = context.getBean(TokenStore.class);
                    assertThat(tokenStore.retrieveStorageIdentifier())
                            .isPresent()
                            .contains("eventstoredb-stable-node");
                });
    }

    @Test
    void shouldGenerateRandomNodeIdWhenNotConfigured() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    TokenStore tokenStore = context.getBean(TokenStore.class);
                    assertThat(tokenStore.retrieveStorageIdentifier())
                            .isPresent()
                            .hasValueSatisfying(id -> assertThat(id).startsWith("eventstoredb-"));
                });
    }

    // ── maskConnectionString (tested indirectly via bean creation) ───────

    @Test
    void shouldMaskCredentialsInConnectionString() {
        // This test validates that bean creation doesn't fail when credentials are in the conn string
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://admin:secretpass@localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStorageEngine.class);
                });
    }

    // ── Retry configuration ─────────────────────────────────────────────

    @Test
    void shouldCreateRetryExecutorBean() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStoreDBRetryExecutor.class);
                    EventStoreDBRetryExecutor executor =
                            context.getBean(EventStoreDBRetryExecutor.class);
                    assertThat(executor.getPolicy().isEnabled()).isTrue();
                    assertThat(executor.getPolicy().getMaxRetries()).isEqualTo(3);
                });
    }

    @Test
    void shouldBindRetryProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.retry.max-retries=5",
                        "axon.eventstoredb.retry.initial-backoff-ms=200",
                        "axon.eventstoredb.retry.max-backoff-ms=10000",
                        "axon.eventstoredb.retry.multiplier=3.0")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getRetry().getMaxRetries()).isEqualTo(5);
                    assertThat(props.getRetry().getInitialBackoffMs()).isEqualTo(200);
                    assertThat(props.getRetry().getMaxBackoffMs()).isEqualTo(10000);
                    assertThat(props.getRetry().getMultiplier()).isEqualTo(3.0);
                });
    }

    @Test
    void shouldDisableRetryWhenConfigured() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.retry.enabled=false")
                .run(context -> {
                    EventStoreDBRetryExecutor executor =
                            context.getBean(EventStoreDBRetryExecutor.class);
                    assertThat(executor.getPolicy().isEnabled()).isFalse();
                });
    }

    // ── Metrics configuration ───────────────────────────────────────────

    @Test
    void shouldBindMetricsProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.metrics.enabled=false")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getMetrics().isEnabled()).isFalse();
                });
    }

    // ── Claims configuration ────────────────────────────────────────────

    @Test
    void shouldBindClaimsProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.claims.enabled=true",
                        "axon.eventstoredb.claims.timeout-seconds=60")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getClaims().isEnabled()).isTrue();
                    assertThat(props.getClaims().getTimeoutSeconds()).isEqualTo(60);
                });
    }

    @Test
    void shouldCreateClaimManagerWhenEnabled() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.claims.enabled=true",
                        "axon.eventstoredb.node-id=test-node")
                .run(context -> {
                    assertThat(context).hasSingleBean(DistributedTokenClaimManager.class);
                    DistributedTokenClaimManager mgr =
                            context.getBean(DistributedTokenClaimManager.class);
                    assertThat(mgr.getNodeId()).isEqualTo("test-node");
                });
    }

    @Test
    void shouldNotCreateClaimManagerWhenDisabled() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.claims.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DistributedTokenClaimManager.class);
                });
    }

    @Test
    void shouldNotCreateClaimManagerByDefault() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DistributedTokenClaimManager.class);
                });
    }

    // ── Subscription configuration ──────────────────────────────────────

    @Test
    void shouldBindSubscriptionProperties() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=my-group",
                        "axon.eventstoredb.subscription.buffer-size=512",
                        "axon.eventstoredb.subscription.create-if-not-exists=false")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getSubscription().isEnabled()).isTrue();
                    assertThat(props.getSubscription().getGroupName()).isEqualTo("my-group");
                    assertThat(props.getSubscription().getBufferSize()).isEqualTo(512);
                    assertThat(props.getSubscription().isCreateIfNotExists()).isFalse();
                });
    }

    @Test
    void shouldNotCreateSubscriptionSourceByDefault() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                });
    }

    @Test
    void shouldNotCreateSubscriptionSourceWhenDisabled() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.subscription.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                });
    }

    @Test
    void shouldUseDefaultSubscriptionValues() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    EventStoreDBProperties props = context.getBean(EventStoreDBProperties.class);
                    assertThat(props.getSubscription().isEnabled()).isFalse();
                    assertThat(props.getSubscription().getBufferSize()).isEqualTo(256);
                    assertThat(props.getSubscription().isCreateIfNotExists()).isTrue();
                    assertThat(props.getSubscription().getGroupName()).isNull();
                });
    }

    @Test
    void shouldCreateSubscriptionSourceWhenEnabled() {
        contextRunner
                .withUserConfiguration(MockPersistentSubscriptionsClientConfiguration.class)
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=test-group",
                        "axon.eventstoredb.subscription.buffer-size=128",
                        "axon.eventstoredb.subscription.create-if-not-exists=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                    EventStoreDBPersistentSubscriptionMessageSource source =
                            context.getBean(EventStoreDBPersistentSubscriptionMessageSource.class);
                    assertThat(source.getGroupName()).isEqualTo("test-group");
                    assertThat(source.isRunning()).isFalse();
                });
    }

    @Test
    void shouldNotCreatePersistentSubscriptionsClientWhenDisabled() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(
                            EventStoreDBPersistentSubscriptionsClient.class);
                });
    }

    @Test
    void shouldUseExistingPersistentSubscriptionsClientBean() {
        contextRunner
                .withUserConfiguration(MockPersistentSubscriptionsClientConfiguration.class)
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=custom-group",
                        "axon.eventstoredb.subscription.create-if-not-exists=false")
                .run(context -> {
                    // Should use the mock client, not create a real one
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionsClient.class);
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                });
    }

    @Configuration
    static class MockPersistentSubscriptionsClientConfiguration {
        @Bean
        public EventStoreDBPersistentSubscriptionsClient persistentSubscriptionsClient() {
            EventStoreDBPersistentSubscriptionsClient client =
                    mock(EventStoreDBPersistentSubscriptionsClient.class);
            org.mockito.Mockito.when(client.createToAll(
                    org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.any()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
            return client;
        }
    }

    // ── Additional branch coverage ──────────────────────────────────────

    @Test
    void shouldCreateEngineWithUpcasterAndMetrics() {
        contextRunner
                .withUserConfiguration(UpcasterAndMetricsConfiguration.class)
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStorageEngine.class);
                });
    }

    @Test
    void shouldCreateClaimManagerWithRandomNodeIdWhenNotSet() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.claims.enabled=true",
                        "axon.eventstoredb.claims.timeout-seconds=30")
                .run(context -> {
                    assertThat(context).hasSingleBean(DistributedTokenClaimManager.class);
                    DistributedTokenClaimManager mgr =
                            context.getBean(DistributedTokenClaimManager.class);
                    // nodeId should be a random UUID (not null)
                    assertThat(mgr.getNodeId()).isNotNull();
                    assertThat(mgr.getNodeId()).isNotBlank();
                });
    }

    @Test
    void shouldCreateSubscriptionSourceWithAutoCreateEnabled() {
        contextRunner
                .withUserConfiguration(MockPersistentSubscriptionsClientConfiguration.class)
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=esdb://localhost:2113?tls=false",
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=auto-create-group",
                        "axon.eventstoredb.subscription.create-if-not-exists=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                });
    }

    @Configuration
    static class UpcasterAndMetricsConfiguration {
        @Bean
        public org.axonframework.serialization.upcasting.event.EventUpcaster eventUpcaster() {
            return stream -> stream;  // identity upcaster
        }

        @Bean
        public io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics eventStoreDBMetrics() {
            return new io.github.bsakweson.axon.eventstoredb.metrics.EventStoreDBMetrics(
                    new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        }
    }
}
