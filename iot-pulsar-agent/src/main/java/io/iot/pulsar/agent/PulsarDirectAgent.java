package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.agent.options.SubscribeOptions;
import java.nio.ByteBuffer;
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
    public CompletableFuture<String> publish(@Nonnull String topicName, @Nonnull ByteBuffer buffer) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Nonnull
    @Override
    public Metadata<String, byte[]> getMetadata() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> subscribe(@Nonnull String topicName, @Nonnull SubscribeOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> unSubscribe(@Nonnull String topicName, @Nonnull String subscriptionName) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> disconnect(@Nonnull String topicName, @Nonnull String subscriptionName) {
        return CompletableFuture.completedFuture(null);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> acknowledgement(@Nonnull String topicName, @Nonnull String subscriptionName,
                                                   @Nonnull byte[] messageId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

}
