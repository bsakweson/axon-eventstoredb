package io.github.bsakweson.axon.eventstoredb.autoconfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EventStoreDBPropertiesTest {

  @Test
  void shouldReturnExplicitConnectionString() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setConnectionString("esdb://node1:2113,node2:2113?tls=true");

    assertEquals("esdb://node1:2113,node2:2113?tls=true", props.getEffectiveConnectionString());
  }

  @Test
  void shouldBuildConnectionStringFromParts() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setHost("eventstore.local");
    props.setPort(2113);
    props.setTls(false);
    props.setUsername(null); // no credentials
    props.setPassword(null);

    assertEquals("esdb://eventstore.local:2113?tls=false", props.getEffectiveConnectionString());
  }

  @Test
  void shouldBuildConnectionStringWithCredentials() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setHost("eventstore.local");
    props.setPort(2113);
    props.setUsername("admin");
    props.setPassword("changeit");
    props.setTls(false);

    assertEquals(
        "esdb://admin:changeit@eventstore.local:2113?tls=false",
        props.getEffectiveConnectionString());
  }

  @Test
  void shouldBuildConnectionStringWithTlsNoVerify() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setHost("eventstore.local");
    props.setPort(2113);
    props.setUsername("admin");
    props.setPassword("changeit");
    props.setTls(true);
    props.setTlsVerifyCert(false);

    String connString = props.getEffectiveConnectionString();
    assertTrue(connString.contains("tls=true"));
    assertTrue(connString.contains("tlsVerifyCert=false"));
  }

  @Test
  void shouldHaveSensibleDefaults() {
    EventStoreDBProperties props = new EventStoreDBProperties();

    assertFalse(props.isEnabled());
    assertEquals("localhost", props.getHost());
    assertEquals(2113, props.getPort());
    assertFalse(props.isTls());
    assertTrue(props.isTlsVerifyCert());
    assertEquals("admin", props.getUsername());
    assertNull(props.getPassword());
    assertEquals(256, props.getBatchSize());
    assertEquals("", props.getStreamPrefix());
    assertEquals("__snapshot", props.getSnapshotStreamPrefix());
    assertEquals("__axon-tokens", props.getTokenStreamPrefix());
    assertNull(props.getNodeId());
  }

  @Test
  void shouldPreferExplicitConnectionStringOverParts() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setConnectionString("esdb://override:2113");
    props.setHost("should-be-ignored");
    props.setPort(9999);

    assertEquals("esdb://override:2113", props.getEffectiveConnectionString());
  }

  @Test
  void shouldOmitCredentialsWhenUsernameIsNull() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setUsername(null);
    props.setPassword(null);
    props.setHost("localhost");
    props.setPort(2113);
    props.setTls(false);

    String connString = props.getEffectiveConnectionString();
    assertEquals("esdb://localhost:2113?tls=false", connString);
  }

  // ── Claims configuration ──────────────────────────────────────────────

  @Test
  void shouldHaveClaimsDefaults() {
    EventStoreDBProperties props = new EventStoreDBProperties();

    assertNotNull(props.getClaims());
    assertFalse(props.getClaims().isEnabled());
    assertEquals(30, props.getClaims().getTimeoutSeconds());
  }

  @Test
  void shouldConfigureClaimsProperties() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    EventStoreDBProperties.Claims claims = new EventStoreDBProperties.Claims();
    claims.setEnabled(true);
    claims.setTimeoutSeconds(60);
    props.setClaims(claims);

    assertTrue(props.getClaims().isEnabled());
    assertEquals(60, props.getClaims().getTimeoutSeconds());
  }

  // ── Subscription configuration ────────────────────────────────────────

  @Test
  void shouldHaveSubscriptionDefaults() {
    EventStoreDBProperties props = new EventStoreDBProperties();

    assertNotNull(props.getSubscription());
    assertFalse(props.getSubscription().isEnabled());
    assertNull(props.getSubscription().getGroupName());
    assertEquals(256, props.getSubscription().getBufferSize());
    assertTrue(props.getSubscription().isCreateIfNotExists());
  }

  @Test
  void shouldConfigureSubscriptionProperties() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    EventStoreDBProperties.Subscription sub = new EventStoreDBProperties.Subscription();
    sub.setEnabled(true);
    sub.setGroupName("my-group");
    sub.setBufferSize(512);
    sub.setCreateIfNotExists(false);
    props.setSubscription(sub);

    assertTrue(props.getSubscription().isEnabled());
    assertEquals("my-group", props.getSubscription().getGroupName());
    assertEquals(512, props.getSubscription().getBufferSize());
    assertFalse(props.getSubscription().isCreateIfNotExists());
  }

  // ── Additional branch coverage ──────────────────────────────────────

  @Test
  void shouldIgnoreBlankConnectionStringAndBuildFromParts() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setConnectionString("   ");
    props.setHost("eventstore.local");
    props.setPort(2113);
    props.setTls(false);
    props.setUsername(null);
    props.setPassword(null);

    // Blank string should be ignored, falls through to host/port construction
    String connString = props.getEffectiveConnectionString();
    assertEquals("esdb://eventstore.local:2113?tls=false", connString);
  }

  @Test
  void shouldOmitCredentialsWhenPasswordIsNull() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    props.setUsername("admin");
    props.setPassword(null);
    props.setHost("localhost");
    props.setPort(2113);
    props.setTls(false);

    String connString = props.getEffectiveConnectionString();
    assertFalse(connString.contains("@"));
    assertEquals("esdb://localhost:2113?tls=false", connString);
  }

  @Test
  void shouldReturnMetricsDefaults() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    assertNotNull(props.getMetrics());
    assertTrue(props.getMetrics().isEnabled());
  }

  @Test
  void shouldReturnRetryDefaults() {
    EventStoreDBProperties props = new EventStoreDBProperties();
    assertNotNull(props.getRetry());
  }
}
