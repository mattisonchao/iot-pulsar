package io.iot.pulsar.mqtt.utils;

import io.netty.channel.ChannelFuture;
import java.util.concurrent.CompletableFuture;
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
}
