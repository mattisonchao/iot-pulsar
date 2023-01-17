package io.iot.pulsar.agent.pool;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.mutable.MutableBoolean;

/**
 * Use this container to store pointers to all created executors to support easy addition of monitoring/checking.
 */
@ThreadSafe
public class ThreadPools {
    private static final ConcurrentHashMap<String, Executor> executors = new ConcurrentHashMap<>();

    public static ScheduledExecutorService createDefaultSingleScheduledExecutor(@Nonnull String poolName) {
        final MutableBoolean updated = new MutableBoolean();
        Executor executor = executors.computeIfAbsent(poolName,
                key -> {
                    updated.setTrue();
                    return Executors.newSingleThreadScheduledExecutor(newNettyFastDefaultThreadFactory(poolName));
                });
        if (updated.isFalse()) {
            throw new IllegalArgumentException(String.format("The executor pool name %s is used", poolName));
        }
        return (ScheduledExecutorService) executor;
    }

    public static ThreadFactory newNettyFastDefaultThreadFactory(@Nonnull String poolName) {
        // todo add thread blocking time check.
        return new DefaultThreadFactory(poolName);
    }

}
