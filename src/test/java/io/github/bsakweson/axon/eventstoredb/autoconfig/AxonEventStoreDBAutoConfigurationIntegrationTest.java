package io.github.bsakweson.axon.eventstoredb.autoconfig;

import io.github.bsakweson.axon.eventstoredb.EventStoreDBEventStorageEngine;
import io.github.bsakweson.axon.eventstoredb.subscriptions.EventStoreDBPersistentSubscriptionMessageSource;
import io.github.bsakweson.axon.eventstoredb.tokenstore.DistributedTokenClaimManager;
import io.github.bsakweson.axon.eventstoredb.util.EventStoreDBStreamNaming;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBPersistentSubscriptionsClient;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests that verify the Spring Boot auto-configuration creates
 * fully functional beans against a real EventStoreDB instance.
 */
@Testcontainers
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AxonEventStoreDBAutoConfigurationIntegrationTest {

    private static final int ESDB_HTTP_PORT = 2113;

    @Container
    private static final GenericContainer<?> EVENT_STORE_DB =
            new GenericContainer<>("eventstore/eventstore:lts")
                    .withExposedPorts(ESDB_HTTP_PORT)
                    .withEnv("EVENTSTORE_INSECURE", "true")
                    .withEnv("EVENTSTORE_MEM_DB", "true")
                    .withEnv("EVENTSTORE_CLUSTER_SIZE", "1")
                    .withEnv("EVENTSTORE_RUN_PROJECTIONS", "All")
                    .withEnv("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "true")
                    .waitingFor(Wait.forHttp("/health/live")
                            .forPort(ESDB_HTTP_PORT)
                            .forStatusCode(204)
                            .withStartupTimeout(Duration.ofSeconds(60)));

    private ApplicationContextRunner contextRunner;

    @BeforeAll
    void setUp() {
        String host = EVENT_STORE_DB.getHost();
        Integer port = EVENT_STORE_DB.getMappedPort(ESDB_HTTP_PORT);
        String connectionString = "esdb://" + host + ":" + port + "?tls=false";

        contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AxonEventStoreDBAutoConfiguration.class))
                .withUserConfiguration(MockSerializerConfiguration.class)
                .withPropertyValues(
                        "axon.eventstoredb.enabled=true",
                        "axon.eventstoredb.connection-string=" + connectionString);
    }

    @AfterAll
    void tearDown() {
        // Container is managed by @Testcontainers
    }

    @Configuration
    static class MockSerializerConfiguration {
        @Bean(name = "eventSerializer")
        public Serializer eventSerializer() {
            return JacksonSerializer.defaultSerializer();
        }
    }

    // ── Core beans ──────────────────────────────────────────────────────

    @Test
    void shouldAutoConfigureEventStoreDBClient() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(EventStoreDBClient.class);
            EventStoreDBClient client = context.getBean(EventStoreDBClient.class);
            assertThat(client).isNotNull();
        });
    }

    @Test
    void shouldAutoConfigureStorageEngine() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(EventStoreDBEventStorageEngine.class);
        });
    }

    @Test
    void shouldAutoConfigureTokenStore() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TokenStore.class);
        });
    }

    @Test
    void shouldAutoConfigureStreamNaming() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(EventStoreDBStreamNaming.class);
        });
    }

    // ── Persistent Subscriptions via auto-config ────────────────────────

    @Test
    void shouldAutoConfigurePersistentSubscriptionBeans() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=auto-config-test-group",
                        "axon.eventstoredb.subscription.buffer-size=64",
                        "axon.eventstoredb.subscription.create-if-not-exists=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionsClient.class);
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);

                    EventStoreDBPersistentSubscriptionMessageSource source =
                            context.getBean(EventStoreDBPersistentSubscriptionMessageSource.class);
                    assertThat(source.getGroupName()).isEqualTo("auto-config-test-group");
                    assertThat(source.isRunning()).isFalse();
                });
    }

    @Test
    void shouldAutoConfigureSubscriptionSourceWithAutoCreate() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=auto-create-group",
                        "axon.eventstoredb.subscription.create-if-not-exists=true")
                .run(context -> {
                    // The source should have been created and the subscription group
                    // should exist on the server (auto-created during bean initialization)
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                    EventStoreDBPersistentSubscriptionMessageSource source =
                            context.getBean(EventStoreDBPersistentSubscriptionMessageSource.class);
                    assertThat(source.getGroupName()).isEqualTo("auto-create-group");
                });
    }

    // ── Distributed Claims via auto-config ──────────────────────────────

    @Test
    void shouldAutoConfigureDistributedClaimManager() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.claims.enabled=true",
                        "axon.eventstoredb.claims.expiry=PT10S",
                        "axon.eventstoredb.claims.extend-interval=PT3S")
                .run(context -> {
                    assertThat(context).hasSingleBean(DistributedTokenClaimManager.class);
                });
    }

    @Test
    void shouldNotCreateClaimManagerWhenDisabled() {
        contextRunner
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DistributedTokenClaimManager.class);
                });
    }

    // ── Full stack ──────────────────────────────────────────────────────

    @Test
    void shouldAutoConfigureFullStackWithSubscriptionsAndClaims() {
        contextRunner
                .withPropertyValues(
                        "axon.eventstoredb.subscription.enabled=true",
                        "axon.eventstoredb.subscription.group-name=full-stack-group",
                        "axon.eventstoredb.subscription.create-if-not-exists=false",
                        "axon.eventstoredb.claims.enabled=true",
                        "axon.eventstoredb.claims.expiry=PT10S")
                .run(context -> {
                    // All beans should be present
                    assertThat(context).hasSingleBean(EventStoreDBClient.class);
                    assertThat(context).hasSingleBean(EventStoreDBEventStorageEngine.class);
                    assertThat(context).hasSingleBean(TokenStore.class);
                    assertThat(context).hasSingleBean(EventStoreDBStreamNaming.class);
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionsClient.class);
                    assertThat(context).hasSingleBean(
                            EventStoreDBPersistentSubscriptionMessageSource.class);
                    assertThat(context).hasSingleBean(DistributedTokenClaimManager.class);
                });
    }
}
