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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiTopicsReaderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
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
            if (context.getOptions().isReader()) {
                return CompletableFuture.completedFuture(null);
            }
            return context.getSubscribeFuture().thenCompose(consumer -> consumer.acknowledgeAsync(messageId));
        }, orderedExecutor.chooseThread(topicName));
    }

    public CompletableFuture<Void> disconnect(@Nonnull TopicName topicName, @Nonnull String subscriptionName) {
        return composeAsync(() -> {
            final ConsumerContext context = consumerTable.remove(topicName, subscriptionName);
            if (context == null) {
                return CompletableFuture.completedFuture(null);
            }
            return context.close();
        }, orderedExecutor.chooseThread(topicName));
    }

    @Slf4j
    @NotThreadSafe
    public static class ConsumerContext {
        private final PulsarClient client;
        private final TopicName topicName;
        @Getter
        private final SubscribeOptions options;
        @Getter
        private volatile CompletableFuture<Consumer<ByteBuffer>> subscribeFuture;
        @Getter
        private volatile CompletableFuture<Reader<ByteBuffer>> subscribeReaderFuture;
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
            if (options.isReader()) {
                ReaderBuilder<ByteBuffer> readerBuilder = client.newReader(Schema.BYTEBUFFER)
                        .topic(topicName.toString())
                        .subscriptionName(options.getSubscriptionName())
                        .readerName("iot-agent-reader")
                        .poolMessages(true)
                        .startMessageId(MessageId.latest);
                final MessageConsumer messageConsumer = options.getMessageConsumer();
                if (messageConsumer != null) {
                    readerBuilder.readerListener((reader, message) -> {
                        try {
                            // -------
                            // This logic is intended to address a bug in pulsar,
                            // which will subsequently be removed when pulsar is fixed.
                            final CompletableFuture<Void> ackFuture;
                            if (reader instanceof MultiTopicsReaderImpl) {
                                ackFuture = ((MultiTopicsReaderImpl<ByteBuffer>) reader).getMultiTopicsConsumer()
                                        .acknowledgeCumulativeAsync(message);
                            } else {
                                ackFuture = ((ReaderImpl<ByteBuffer>) reader)
                                        .getConsumer().acknowledgeCumulativeAsync(message);
                            }
                            ackFuture.exceptionally(ex -> {
                                log.error("[IOT-AGENT][{}][{}] auto acknowledge message {} cumulative fail. {}",
                                        topicName,
                                        options.getSubscriptionName(),
                                        message.getMessageId(), ex);
                                return null;
                            });
                            // ------
                            extractAndFlushMessage(messageConsumer, message);
                        } finally {
                            message.release();
                        }
                    });
                }
                final CompletableFuture<Reader<ByteBuffer>> subscribeReaderFuture = readerBuilder.createAsync();
                ConsumerContext.this.subscribeReaderFuture = subscribeReaderFuture;
                return subscribeReaderFuture.thenApply(__ -> null);
            } else {
                final ConsumerBuilder<ByteBuffer> consumerBuilder = client.newConsumer(Schema.BYTEBUFFER)
                        .topic(topicName.toString())
                        .subscriptionName(options.getSubscriptionName())
                        .consumerName("iot-agent-consumer")
                        .subscriptionType(SubscriptionType.Shared)
                        .poolMessages(true);
                final MessageConsumer messageConsumer = options.getMessageConsumer();
                if (messageConsumer != null) {
                    consumerBuilder.messageListener((consumer, message) -> {
                        try {
                            extractAndFlushMessage(messageConsumer, message);
                        } finally {
                            message.release();
                        }
                    });
                }
                final CompletableFuture<Consumer<ByteBuffer>> subscribeFuture = consumerBuilder.subscribeAsync();
                ConsumerContext.this.subscribeFuture = subscribeFuture;
                return subscribeFuture.thenApply(__ -> null);
            }
        }

        private static void extractAndFlushMessage(@Nonnull MessageConsumer messageConsumer,
                                                   @Nonnull Message<ByteBuffer> message) {
            final byte[] byteMessageId = message.getMessageId().toByteArray();
            final ByteBuffer payload = message.getValue();
            final String messageTopicName = message.getTopicName();
            messageConsumer.out(messageTopicName, byteMessageId, payload);
        }

        public @Nonnull CompletableFuture<Void> unSubscribe() {
            resetTick();
            if (options.isReader()) {
                if (subscribeReaderFuture == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // convert exception to agent exception
                return subscribeReaderFuture.thenCompose(Reader::closeAsync);
            } else {
                if (subscribeFuture == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // convert exception to agent exception
                return subscribeFuture.thenCompose(Consumer::unsubscribeAsync);
            }
        }

        public @Nonnull CompletableFuture<Void> close() {
            resetTick();
            if (options.isReader()) {
                return subscribeReaderFuture.thenCompose(reader -> {
                    if (reader.isConnected()) {
                        return reader.closeAsync();
                    }
                    return CompletableFuture.completedFuture(null);
                });
            } else {
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
        }

        private void resetTick() {
            ConsumerContext.this.lastActiveTime = System.currentTimeMillis();
        }

    }
}
