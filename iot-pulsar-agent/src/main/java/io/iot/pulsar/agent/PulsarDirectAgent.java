package io.iot.pulsar.agent;

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
}
