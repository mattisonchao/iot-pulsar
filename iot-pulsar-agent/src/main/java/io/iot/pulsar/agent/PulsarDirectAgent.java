package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class PulsarDirectAgent implements PulsarAgent {
    @Override
    public void close() {

    }

    @Nonnull
    @Override
    public CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Nonnull
    @Override
    public Metadata<String, byte[]> getMetadata() {
        throw new UnsupportedOperationException();
    }
}
