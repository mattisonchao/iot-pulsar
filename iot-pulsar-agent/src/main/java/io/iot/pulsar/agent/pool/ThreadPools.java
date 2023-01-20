package io.iot.pulsar.agent.pool;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.lang3.mutable.MutableBoolean;

/**
 * Use this container to store pointers to all created executors to support easy addition of monitoring/checking.
 */
@ThreadSafe
public class ThreadPools {
    private static final ConcurrentHashMap<String, Executor> executors = new ConcurrentHashMap<>();

    public static @Nonnull ScheduledExecutorService createDefaultSingleScheduledExecutor(@Nonnull String poolName) {
        final MutableBoolean updated = new MutableBoolean();
        final Executor executor = executors.computeIfAbsent(poolName,
                key -> {
                    updated.setTrue();
                    return Executors.newSingleThreadScheduledExecutor(newNettyFastDefaultThreadFactory(poolName));
                });
        if (updated.isFalse()) {
            throw new IllegalArgumentException(String.format("The executor pool name %s is used", poolName));
        }
        return (ScheduledExecutorService) executor;
    }

    public static @Nonnull OrderedExecutor createOrderedExecutor(@Nonnull String poolName, int numThreads) {
        final MutableBoolean updated = new MutableBoolean();
        final Executor executor = executors.computeIfAbsent(poolName,
                key -> {
                    updated.setTrue();
                    return OrderedExecutor.newBuilder()
                            .numThreads(numThreads)
                            .name(poolName)
                            .threadFactory(newNettyFastDefaultThreadFactory(poolName))
                            .build();
                });
        if (updated.isFalse()) {
            throw new IllegalArgumentException(String.format("The executor pool name %s is used", poolName));
        }
        return (OrderedExecutor) executor;
    }

    public static @Nonnull ThreadFactory newNettyFastDefaultThreadFactory(@Nonnull String poolName) {
        // todo add thread blocking time check.
        return new DefaultThreadFactory(poolName);
    }

}
