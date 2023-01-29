package io.iot.pulsar.agent;

import static io.iot.pulsar.common.utils.CompletableFutures.composeAsync;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.iot.pulsar.agent.exceptions.IotAgentSubscriptionException;
import io.iot.pulsar.agent.options.SubscribeOptions;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class PulsarConsumerManager {
    private final PulsarClient client;
    private final OrderedExecutor orderedExecutor;
    private final Table<TopicName, String, ConsumerContext> consumerTable =
            HashBasedTable.create();

    public PulsarConsumerManager(@Nonnull PulsarClient client, @Nonnull OrderedExecutor orderedExecutor) {
        this.client = client;
        this.orderedExecutor = orderedExecutor;
    }

    public CompletableFuture<Void> subscribe(@Nonnull TopicName topicName,
                                             @Nonnull SubscribeOptions options) {
        return composeAsync(() -> {
            final ConsumerContext context = consumerTable.get(topicName, options.getSubscriptionName());
            if (context != null) {
                // simply complete here
                // todo to find the good method to solve this.
                return CompletableFuture.completedFuture(null);
            }
            final ConsumerContext newContext = ConsumerContext.create(client, topicName, options);
            consumerTable.put(topicName, options.getSubscriptionName(), newContext);
            return newContext.subscribe().thenApply(__ -> null);
        }, orderedExecutor.chooseThread(topicName));
    }

    public CompletableFuture<Void> unSubscribe(@Nonnull TopicName topicName, @Nonnull String subscriptionName) {
        return composeAsync(() -> {
            final ConsumerContext context = consumerTable.remove(topicName, subscriptionName);
            if (context == null) {
                return CompletableFuture.failedFuture(
                        new IotAgentSubscriptionException.NotSubscribeException(subscriptionName));
            }
            return context.unSubscribe().thenCompose(__ -> context.close());
        }, orderedExecutor.chooseThread(topicName));
    }

    public CompletableFuture<Void> acknowledgement(@Nonnull TopicName topicName, @Nonnull String subscriptionName,
                                                   @Nonnull MessageId messageId) {
        return composeAsync(() -> {
            final ConsumerContext context = consumerTable.get(topicName, subscriptionName);
            if (context == null) {
                return CompletableFuture.failedFuture(
                        new IotAgentSubscriptionException.NotSubscribeException(subscriptionName));
            }
            return context.subscribeFuture.thenCompose(consumer -> consumer.acknowledgeAsync(messageId));
        }, orderedExecutor.chooseThread(topicName));
    }

    @Slf4j
    @NotThreadSafe
    public static class ConsumerContext {
        private final PulsarClient client;
        private final TopicName topicName;
        private final SubscribeOptions options;
        @Getter
        private volatile CompletableFuture<Consumer<ByteBuffer>> subscribeFuture;
        @Getter
        private volatile long lastActiveTime = System.currentTimeMillis();

        private ConsumerContext(@Nonnull PulsarClient client, @Nonnull TopicName topicName,
                                @Nonnull SubscribeOptions options) {
            this.client = client;
            this.topicName = topicName;
            this.options = options;
        }

        public static @Nonnull PulsarConsumerManager.ConsumerContext create(@Nonnull PulsarClient client,
                                                                            @Nonnull TopicName topicName,
                                                                            @Nonnull SubscribeOptions options) {
            return new ConsumerContext(client, topicName, options);
        }

        public @Nonnull CompletableFuture<Void> subscribe() {
            resetTick();
            final ConsumerBuilder<ByteBuffer> consumerBuilder = client.newConsumer(Schema.BYTEBUFFER)
                    .topic(topicName.toString())
                    .subscriptionName(options.getSubscriptionName())
                    .consumerName("iot-agent-consumer")
                    .subscriptionType(SubscriptionType.Shared)
                    .poolMessages(true);
            final MessageConsumer messageConsumer = options.getMessageConsumer();
            if (messageConsumer != null) {
                consumerBuilder.messageListener((consumer, message) -> {
                    final byte[] byteMessageId = message.getMessageId().toByteArray();
                    final ByteBuffer payload = message.getValue();
                    final String messageTopicName = message.getTopicName();
                    try {
                        messageConsumer.out(messageTopicName, byteMessageId, payload);
                    } finally {
                        message.release();
                    }
                });
            }
            final CompletableFuture<Consumer<ByteBuffer>> subscribeFuture = consumerBuilder.subscribeAsync();
            ConsumerContext.this.subscribeFuture = subscribeFuture;
            return subscribeFuture.thenApply(__ -> null);
        }

        public @Nonnull CompletableFuture<Void> unSubscribe() {
            resetTick();
            if (subscribeFuture == null) {
                return CompletableFuture.completedFuture(null);
            }
            return subscribeFuture.thenCompose(Consumer::unsubscribeAsync)
                    .exceptionally(ex -> {
                        // convert exception to agent exception
                        return null;
                    });
        }

        public @Nonnull CompletableFuture<Void> close() {
            resetTick();
            if (subscribeFuture == null) {
                return CompletableFuture.completedFuture(null);
            }
            return subscribeFuture.thenCompose(Consumer::closeAsync)
                    .exceptionally(ex -> {
                        log.warn("[IOT-AGENT][{}][{}] Got an exception while close the consumer.", topicName,
                                options.getSubscriptionName());
                        // todo retry deleting
                        return null;
                    });
        }

        private void resetTick() {
            ConsumerContext.this.lastActiveTime = System.currentTimeMillis();
        }

    }
}
