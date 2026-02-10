package com.eventstore.dbclient;

/**
 * Test utility to create {@link StreamNotFoundException} instances.
 * <p>
 * The constructor of {@code StreamNotFoundException} is package-private,
 * so this factory must reside in the {@code com.eventstore.dbclient} package.
 */
public final class StreamNotFoundExceptionFactory {

    private StreamNotFoundExceptionFactory() {
    }

    public static StreamNotFoundException create(String streamName) {
        return new StreamNotFoundException(streamName);
    }
}
