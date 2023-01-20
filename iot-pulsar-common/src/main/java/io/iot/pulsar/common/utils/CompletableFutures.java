package io.iot.pulsar.common.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class CompletableFutures {

    @Nonnull
    public static <T> CompletableFuture<T> composeAsync(@Nonnull Supplier<CompletionStage<T>> supplier,
                                                        @Nonnull Executor executor) {
        return CompletableFuture.completedFuture(null)
                .thenComposeAsync(__ -> supplier.get(), executor);
    }

    @Nonnull
    public static Throwable unwrap(@Nonnull Throwable throwable) {
        if (throwable instanceof CompletionException) {
            return throwable.getCause();
        } else if (throwable instanceof ExecutionException) {
            return throwable.getCause();
        } else {
            return throwable;
        }
    }
}
