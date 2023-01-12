package io.iot.pulsar.mqtt.vertex;

import io.iot.pulsar.mqtt.messages.MqttFixedHeaders;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.iot.pulsar.mqtt.utils.CompletableFutures;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class RejectOnlyMqttVertex implements MqttVertex {
    private final Channel channel;

    public RejectOnlyMqttVertex(Channel channel) {
        this.channel = channel;
    }

    @Nonnull
    @Override
    public String identifier() {
        return "unknown";
    }

    @Nonnull
    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
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
    public CompletableFuture<Void> close() {
        return CompletableFutures.from(channel.close());
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> commandTraceLog(@Nonnull CompletableFuture<T> future, @Nonnull String command) {
        return future.whenComplete((ignore, ex) -> {
            if (ex != null) {
                log.error(command);
                return;
            }
            log.info(command);
        });
    }
}
