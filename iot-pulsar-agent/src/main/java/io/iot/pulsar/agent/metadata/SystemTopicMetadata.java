package io.iot.pulsar.agent.metadata;

import static io.iot.pulsar.common.utils.CompletableFutures.composeAsync;
import com.google.common.base.Throwables;
import io.iot.pulsar.agent.pool.ThreadPools;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
@ThreadSafe
public class SystemTopicMetadata implements Metadata<String, byte[]> {
    private static final String GLOBAL_META_SPACE = "persistent://pulsar/system/__iot_pulsar_system_event";
    private final PulsarClient client;
    private final OrderedExecutor out = ThreadPools.createOrderedExecutor("iot-agent-metadata-out",
            Runtime.getRuntime().availableProcessors());
    private final OrderedExecutor inE = ThreadPools.createOrderedExecutor("iot-agent-metadata-in",
            Runtime.getRuntime().availableProcessors());

    private volatile CompletableFuture<TableView<byte[]>> globalView;
    private volatile CompletableFuture<Producer<byte[]>> globalProducer;
    private volatile CompletableFuture<Void> lastSentFuture = CompletableFuture.completedFuture(null);
    private final Map<String, List<Consumer<byte[]>>> listeners = new ConcurrentHashMap<>();

    public SystemTopicMetadata(@Nonnull PulsarClient client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<byte[]>> get(@Nonnull String key) {
        initView();
        return globalView.thenApplyAsync(view -> Optional.ofNullable(view.get(key)), inE.chooseThread(key));
    }

    private synchronized boolean initView() {
        if (globalView != null) {
            return false;
        }
        synchronized (this) {
            if (globalView != null) {
                return false;
            }
            SystemTopicMetadata.this.globalView = client.newTableViewBuilder(Schema.BYTES)
                    .topic(GLOBAL_META_SPACE)
                    .createAsync()
                    .thenApply(view -> {
                        view.forEachAndListen((innerKey, value) -> {
                            // using concurrent hashmap to avoid concurrent modification exception
                            listeners.compute(innerKey, (k, v) -> {
                                if (v == null) {
                                    return null;
                                }
                                v.forEach(c -> {
                                    try {
                                        out.execute(() -> {
                                            try {
                                                c.accept(value);
                                            } catch (Throwable ex) {
                                                log.warn("[IOT-AGENT] got an exception while invoke listener callback.",
                                                        ex);
                                            }
                                        });
                                    } catch (Throwable ex) {
                                        log.error("[IOT-AGENT] got an exception while "
                                                + "submitting a listener callback to the executor.");
                                    }
                                });
                                return v;
                            });

                        });
                        return view;
                    });
            return true;
        }
    }

    private void initProducer() {
        if (globalProducer != null) {
            return;
        }
        synchronized (this) {
            if (globalProducer != null) {
                return;
            }
            SystemTopicMetadata.this.globalProducer = client.newProducer(Schema.BYTES)
                    .topic(GLOBAL_META_SPACE)
                    .createAsync();
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> put(@Nonnull String key, @Nullable byte[] value) {
        initProducer();
        return composeAsync(() -> {
            final CompletableFuture<Void> lastSent = lastSentFuture.thenCompose(ignore -> globalProducer)
                    .thenCompose(producer -> producer.newMessage().key(key).value(value).sendAsync())
                    .thenApply(ignore -> null);
            SystemTopicMetadata.this.lastSentFuture = lastSent;
            return lastSent;
        }, inE.chooseThread(key));
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> listen(@Nonnull String key, @Nonnull Consumer<byte[]> listener) {
        final CompletableFuture<Void> cleanupFuture = new CompletableFuture<>();
        // ======= cleanup finally
        cleanupFuture.whenComplete((ignore1, ignore2) -> {
            listeners.compute(key, (k, v) -> {
                if (v == null) {
                    return null;
                }
                log.debug("[IOT-AGENT] Unregistered listener for key: " + key);
                v.remove(listener);
                if (v.size() == 0) {
                    return null;
                }
                return v;
            });
        });

        listeners.compute(key, (k, v) -> {
            log.debug("[IOT-AGENT] Registered listener for key: " + key);
            if (v == null) {
                final List<Consumer<byte[]>> listeners = new ArrayList<>();
                listeners.add(listener);
                return listeners;
            }
            v.add(listener);
            return v;
        });
        if (!initView()) {
            // We can check actual events to avoid lost possibility while the view has already inited.
            globalView.thenAccept(view -> {
                final byte[] bytes = view.get(key);
                if (bytes != null) {
                    listener.accept(bytes);
                }
            });
        }
        return cleanupFuture;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> delete(@Nonnull String key) {
        return put(key, null);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> close() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>(2);
        if (globalView != null) {
            futures.add(globalView.thenCompose(TableView::closeAsync));
        }
        if (globalProducer != null) {
            futures.add(globalProducer.thenCompose(Producer::closeAsync));
        }
        inE.shutdown();
        out.shutdown();
        final CompletableFuture<Void> future = FutureUtil.waitForAll(futures);
        future.exceptionally(ex -> {
            log.error("[IOT-AGENT] got an exception while closing the system topic metadata.",
                    Throwables.getRootCause(ex));
            return null;
        });
        return future;
    }
}
