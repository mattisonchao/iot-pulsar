package io.iot.pulsar.test.env.mock;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.stats.NullStatsLogger;

public class SameThreadOrderedSafeExecutor extends OrderedExecutor {

    public SameThreadOrderedSafeExecutor() {
        super("same-thread-executor",
                1,
                new DefaultThreadFactory("test"),
                NullStatsLogger.INSTANCE,
                false,
                false,
                100000,
                -1,
                false);
    }

    @Override
    public void execute(Runnable r) {
        r.run();
    }

    @Override
    public void executeOrdered(int orderingKey, SafeRunnable r) {
        r.run();
    }

    @Override
    public void executeOrdered(long orderingKey, SafeRunnable r) {
        r.run();
    }

}
