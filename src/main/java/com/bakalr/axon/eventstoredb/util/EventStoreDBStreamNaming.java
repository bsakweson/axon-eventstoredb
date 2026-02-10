package com.bakalr.axon.eventstoredb.util;

/**
 * Provides consistent stream naming conventions for EventStoreDB.
 *
 * <p>EventStoreDB uses stream names to organize events. This class maps Axon's aggregate type
 * and identifier to EventStoreDB stream names, following EventStoreDB's category convention
 * ({@code {Category}-{Id}}) so built-in category projections ({@code $ce-{Category}}) work
 * automatically.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>Aggregate stream: {@code Order-b2ec5be0-a477-419b-bacf-d85c7e5bbeb1}
 *   <li>Snapshot stream: {@code __snapshot-Order-b2ec5be0-...}
 *   <li>Token stream: {@code __axon-tokens-order-projection}
 * </ul>
 */
public final class EventStoreDBStreamNaming {

  private static final String SNAPSHOT_PREFIX = "__snapshot";
  private static final String TOKEN_PREFIX = "__axon-tokens";

  private final String streamPrefix;
  private final String snapshotPrefix;
  private final String tokenPrefix;

  public EventStoreDBStreamNaming() {
    this("", SNAPSHOT_PREFIX, TOKEN_PREFIX);
  }

  public EventStoreDBStreamNaming(
      String streamPrefix, String snapshotPrefix, String tokenPrefix) {
    this.streamPrefix = streamPrefix != null ? streamPrefix : "";
    this.snapshotPrefix = snapshotPrefix != null ? snapshotPrefix : SNAPSHOT_PREFIX;
    this.tokenPrefix = tokenPrefix != null ? tokenPrefix : TOKEN_PREFIX;
  }

  /**
   * Builds the aggregate event stream name.
   *
   * @param aggregateType the Axon aggregate type (e.g., "Order")
   * @param aggregateIdentifier the aggregate instance ID
   * @return stream name like "Order-b2ec5be0-..."
   */
  public String aggregateStream(String aggregateType, String aggregateIdentifier) {
    String base = aggregateType + "-" + aggregateIdentifier;
    return streamPrefix.isEmpty() ? base : streamPrefix + "-" + base;
  }

  /**
   * Extracts the aggregate type from a stream name.
   *
   * @param streamName the EventStoreDB stream name
   * @return the aggregate type, or null if the stream doesn't follow the naming convention
   */
  public String extractAggregateType(String streamName) {
    String name = streamPrefix.isEmpty() ? streamName : streamName.replaceFirst(streamPrefix + "-", "");
    int dashIndex = name.indexOf('-');
    return dashIndex > 0 ? name.substring(0, dashIndex) : null;
  }

  /**
   * Extracts the aggregate identifier from a stream name.
   *
   * @param streamName the EventStoreDB stream name
   * @return the aggregate identifier, or null if the stream doesn't follow the naming convention
   */
  public String extractAggregateIdentifier(String streamName) {
    String name = streamPrefix.isEmpty() ? streamName : streamName.replaceFirst(streamPrefix + "-", "");
    int dashIndex = name.indexOf('-');
    return dashIndex > 0 ? name.substring(dashIndex + 1) : null;
  }

  /**
   * Builds the snapshot stream name for an aggregate instance.
   *
   * @param aggregateType the Axon aggregate type
   * @param aggregateIdentifier the aggregate instance ID
   * @return snapshot stream name like "__snapshot-Order-b2ec5be0-..."
   */
  public String snapshotStream(String aggregateType, String aggregateIdentifier) {
    return snapshotPrefix + "-" + aggregateType + "-" + aggregateIdentifier;
  }

  /**
   * Builds the token stream name for a tracking processor.
   *
   * @param processorName the Axon processor name
   * @return token stream name like "__axon-tokens-order-projection"
   */
  public String tokenStream(String processorName) {
    return tokenPrefix + "-" + processorName;
  }

  /**
   * Checks whether a stream name is a system/internal stream (snapshot, token, or EventStoreDB
   * system streams starting with $).
   */
  public boolean isSystemStream(String streamName) {
    return streamName.startsWith("$")
        || streamName.startsWith(snapshotPrefix)
        || streamName.startsWith(tokenPrefix);
  }
}
