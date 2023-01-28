package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.agent.options.SubscribeOptions;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Pulsar broker agent, use this interface to provide the standard method interact with pulsar broker.
 */
@ThreadSafe
public interface PulsarAgent {
    void close();

    // todo add exception description
    @Nonnull
    CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters);

    @Nonnull
    CompletableFuture<String> publish(@Nonnull String topicName, @Nonnull ByteBuffer buffer);

    @Nonnull
    Metadata<String, byte[]> getMetadata();

    @Nonnull
    CompletableFuture<Void> subscribe(@Nonnull String topicName, @Nonnull SubscribeOptions options);

    @Nonnull
    CompletableFuture<Void> unSubscribe(@Nonnull String topicName, @Nonnull String subscriptionName);

    @Nonnull
    CompletableFuture<Void> acknowledgement(@Nonnull String topicName, @Nonnull String subscriptionName,
                                            @Nonnull byte[] messageId);
}
