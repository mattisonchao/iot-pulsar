package io.iot.pulsar.agent.metadata;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.client.api.PulsarClient;

public class SystemTopicMetadata implements Metadata<String, byte[]> {
    private final PulsarClient client;

    public SystemTopicMetadata(PulsarClient client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<byte[]>> get(@Nonnull String key) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<byte[]>> put(@Nonnull String k, @Nonnull byte[] value) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<byte[]>> delete(@Nonnull String key) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
