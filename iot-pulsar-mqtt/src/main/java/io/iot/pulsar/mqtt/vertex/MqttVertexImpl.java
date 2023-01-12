package io.iot.pulsar.mqtt.vertex;

import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.messages.MqttFixedHeaders;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.iot.pulsar.mqtt.utils.CompletableFutures;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
@ThreadSafe
public class MqttVertexImpl implements MqttVertex {
    private final Channel channel;
    private final String identifier;
    private final MqttVersion version;
    private final AuthData authData;
    private final MqttVertexProperties properties;
    private volatile String authRole;

    @Nonnull
    @Override
    public MqttVersion version() {
        return version;
    }

    @Nonnull
    @Override
    public String identifier() {
        return identifier;
    }

    @Nonnull
    @Override
    public AuthData authData() {
        return authData;
    }

    @Nonnull
    @Override
    public MqttVertexProperties properties() {
        return properties;
    }

    @Nonnull
    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Nonnull
    @Override
    public String authRole() {
        return authRole;
    }

    @Override
    public void authRole(@Nonnull String role) {
        MqttVertexImpl.this.authRole = role;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> reject(@Nonnull MqttConnReturnCode code, @Nonnull MqttProperties properties) {
        if (code == MqttConnReturnCode.ACCEPT) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Reject method don't allow return CONNECTION_ACCEPTED:0"));
        }
        MqttConnAckVariableHeader variableHeader =
                // Due to we don't have protocol version here, send v3 connect return code to client.
                new MqttConnAckVariableHeader(code.getV3(), false, properties);
        MqttMessage connack = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK, variableHeader, null);
        return CompletableFutures.from(channel.writeAndFlush(connack))
                .thenCompose(__ -> close());
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> accept(@Nonnull MqttProperties properties) {
        MqttConnAckVariableHeader variableHeader =
                // Due to we don't have protocol version here, send v3 connect return code to client.
                new MqttConnAckVariableHeader(MqttConnReturnCode.ACCEPT.getNettyCode(version()), false, properties);
        MqttMessage connack = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK, variableHeader, null);
        return CompletableFutures.from(channel.writeAndFlush(connack));
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> close() {
        return CompletableFutures.from(channel.close());
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> commandTraceLog(@Nonnull CompletableFuture<T> future, @Nonnull String command) {
        return future.whenComplete((ignore, ex) -> {
            if (ex != null) {
                log.error("[IOT-MQTT][{}] Do [{}] failed.", remoteAddress(), command, ex);
                return;
            }
            log.info("[IOT-MQTT][{}] Do [{}] succeeded.", remoteAddress(), command);
        });
    }
}
