package io.iot.pulsar.mqtt.utils;

import io.netty.channel.ChannelFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

public class CompletableFutures {

    @Nonnull
    public static CompletableFuture<Void> from(@Nonnull ChannelFuture channelFuture) {
        // todo check NPE
        CompletableFuture<Void> future = new CompletableFuture<>();
        channelFuture.addListener(event -> {
            if (!event.isSuccess()) {
                future.completeExceptionally(event.cause());
                return;
            }
            future.complete(null);
        });
        return future;
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
