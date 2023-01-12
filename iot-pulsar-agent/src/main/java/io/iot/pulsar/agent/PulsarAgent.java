package io.iot.pulsar.agent;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Pulsar broker agent, use this interface to provide the standard method interact with pulsar broker.
 */
@ThreadSafe
public interface PulsarAgent {
    void close();

    @Nonnull
    CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters);


}
