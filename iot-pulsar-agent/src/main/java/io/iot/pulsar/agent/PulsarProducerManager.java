package io.iot.pulsar.agent;

import static io.iot.pulsar.common.utils.CompletableFutures.composeAsync;
import io.iot.pulsar.agent.pool.ThreadPools;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@ThreadSafe
@Slf4j
public class PulsarProducerManager {
    private static final int PRODUCER_MAXIMUM_IDLE_TIME = 10;
    private static final int GC_DELAY_IN_SEC = 5;
    private final Map<TopicName, ProducerContext> pool = new ConcurrentHashMap<>();
    private final PulsarClient client;
    private final OrderedExecutor executor;
    private final ScheduledExecutorService scheduledExecutorService;

    public PulsarProducerManager(@Nonnull PulsarClient client, @Nonnull OrderedExecutor executor) {
        this.client = client;
        this.executor = executor;
        this.scheduledExecutorService =
                ThreadPools.createDefaultSingleScheduledExecutor("iot-pulsar-producer-manager-gc");
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                for (TopicName topicName : pool.keySet()) {
                    // submit this task to executor to avoid lock
                    executor.chooseThread(topicName)
                            .execute(() -> {
                                final ProducerContext producerContext = pool.get(topicName);
                                // avoid race condition
                                if (producerContext != null) {
                                    long lastActiveTime = producerContext.getLastActiveTime();
                                    if (System.currentTimeMillis() - lastActiveTime > PRODUCER_MAXIMUM_IDLE_TIME) {
                                        log.info("[IOT-AGENT][{}] internal producer GC being executed.", topicName);
                                        final ProducerContext context = pool.remove(topicName);
                                        context.close().whenComplete((result, ex) -> {
                                            log.info("[IOT-AGENT][{}] internal producer GC is done.", topicName);
                                        });
                                    }
                                }
                            });
                }
            } catch (Throwable ex) {
                log.warn("Got an exception while doing GC check.", ex);
            }
        }, GC_DELAY_IN_SEC, GC_DELAY_IN_SEC, TimeUnit.SECONDS);
    }


    @Nonnull
    public CompletableFuture<MessageId> publish(@Nonnull TopicName topicName, @Nonnull ByteBuffer payload) {
        // single thread to avoid lock
        return composeAsync(() -> pool.computeIfAbsent(topicName, k -> ProducerContext.create(client, topicName))
                .publish(payload), executor.chooseThread(topicName));
    }

    @Slf4j
    @NotThreadSafe
    public static class ProducerContext {
        private final TopicName topicName;
        private final PulsarClient pulsarClient;
        private volatile CompletableFuture<Producer<ByteBuffer>> producerFuture;
        private volatile CompletableFuture<MessageId> lastSentFuture = CompletableFuture.completedFuture(null);
        @Getter
        private volatile long lastActiveTime = System.currentTimeMillis();

        private ProducerContext(PulsarClient client, TopicName topicName) {
            this.topicName = topicName;
            this.pulsarClient = client;
        }

        public static @Nonnull ProducerContext create(@Nonnull PulsarClient client, @Nonnull TopicName topicName) {
            final CompletableFuture<Producer<ByteBuffer>> future = client.newProducer(Schema.BYTEBUFFER)
                    .topic(topicName.toString())
                    .producerName("iot-pulsar-mqtt-producer")
                    .createAsync();
            final ProducerContext producerContext = new ProducerContext(client, topicName);
            producerContext.producerFuture = future;
            return producerContext;
        }

        public @Nonnull CompletableFuture<MessageId> publish(@Nonnull ByteBuffer payload) {
            autoRecovery();
            ProducerContext.this.lastActiveTime = System.currentTimeMillis();
            final CompletableFuture<MessageId> future = lastSentFuture.thenCompose(__ -> producerFuture)
                    .thenCompose(producer -> producer.sendAsync(payload));
            ProducerContext.this.lastSentFuture = future;
            return future;
        }

        private void autoRecovery() {
            if (producerFuture.isCompletedExceptionally()) {
                producerFuture.exceptionally(ex -> {
                    log.info("[IOT-AGENT][{}] Refreshing internal producer", topicName);
                    return null;
                });
                ProducerContext.this.producerFuture = pulsarClient.newProducer(Schema.BYTEBUFFER)
                        .topic(topicName.toString())
                        .producerName("iot-pulsar-mqtt-producer")
                        .createAsync()
                        .thenApply(producer -> {
                            log.info("[IOT-AGENT][{}] Refreshed internal producer", topicName);
                            return producer;
                        });
            } else {
                if (lastSentFuture.isCompletedExceptionally()) {
                    lastSentFuture.exceptionally(ex -> {
                        log.info("[IOT-AGENT][{}] Refreshing internal producer", topicName);
                        return null;
                    });
                    ProducerContext.this.producerFuture = pulsarClient.newProducer(Schema.BYTEBUFFER)
                            .topic(topicName.toString())
                            .producerName("iot-pulsar-mqtt-producer")
                            .createAsync()
                            .thenApply(producer -> {
                                log.info("[IOT-AGENT][{}] Refreshed internal producer", topicName);
                                return producer;
                            });
                }
            }
        }

        public CompletableFuture<Void> close() {
            return producerFuture.thenCompose(Producer::closeAsync)
                    .exceptionally(ex -> {
                        log.warn("[IOT-AGENT][{}] Got an exception while close the producer.", topicName);
                        // todo retry deleting
                        return null;
                    });
        }
    }
}
