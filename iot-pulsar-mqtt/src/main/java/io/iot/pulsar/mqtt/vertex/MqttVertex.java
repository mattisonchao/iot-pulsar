package io.iot.pulsar.mqtt.vertex;

import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public interface MqttVertex {

    @Nonnull
    default MqttVersion version() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default AuthData authData() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default String authRole() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    default void authRole(@Nonnull String role) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default String identifier() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default MqttVertexProperties properties() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    SocketAddress remoteAddress();

    @Nonnull
    default CompletableFuture<Void> accept(@Nonnull MqttProperties properties) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Unsupported operation"));
    }

    @Nonnull
    CompletableFuture<Void> reject(@Nonnull MqttConnReturnCode code, @Nonnull MqttProperties properties);

    @Nonnull
    CompletableFuture<Void> close();

    @Nonnull
    <T> CompletableFuture<T> commandTraceLog(@Nonnull CompletableFuture<T> future, @Nonnull String command);
}
