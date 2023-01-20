package io.iot.pulsar.mqtt.endpoint;

import io.iot.pulsar.mqtt.messages.Identifier;
import io.iot.pulsar.mqtt.messages.MqttFixedHeaders;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.iot.pulsar.mqtt.messages.custom.ConnInternalErrorMessage;
import io.iot.pulsar.mqtt.utils.EnhanceCompletableFutures;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class RejectOnlyMqttEndpoint implements MqttEndpoint {
    private final Channel channel;

    public RejectOnlyMqttEndpoint(Channel channel) {
        this.channel = channel;
    }

    @Nonnull
    @Override
    public Identifier identifier() {
        return Identifier.create("unknow", true);
    }

    @Nonnull
    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> close() {
        return EnhanceCompletableFutures.from(channel.close());
    }

    @Override
    public void swallow(@Nonnull MqttMessage mqttMessage) {
        final MqttMessage ack;
        // Check custom message
        if (mqttMessage instanceof ConnInternalErrorMessage) {
            ack = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                    new MqttConnAckVariableHeader(
                            MqttConnReturnCode.SERVER_INTERNAL_ERROR.getNettyCode(MqttVersion.MQTT_3_1_1),
                            false, MqttProperties.NO_PROPERTIES), null);
        } else {
            final MqttFixedHeader mqttFixedHeader = mqttMessage.fixedHeader();
            if (mqttMessage.decoderResult().isFailure()) {
                // In this case, maybe we don't have message type.
                ack = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                        new MqttConnAckVariableHeader(
                                MqttConnReturnCode.PROTOCOL_ERROR.getNettyCode(MqttVersion.MQTT_3_1_1),
                                false, MqttProperties.NO_PROPERTIES), null);
            } else {
                switch (mqttFixedHeader.messageType()) {
                    // todo we should implement other switch case in the future.
                    case SUBSCRIBE:
                    case UNSUBSCRIBE:
                    case DISCONNECT:
                    default:
                        ack = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                                new MqttConnAckVariableHeader(
                                        MqttConnReturnCode.PROTOCOL_ERROR.getNettyCode(MqttVersion.MQTT_3_1_1),
                                        false, MqttProperties.NO_PROPERTIES), null);
                }
            }
        }
        EnhanceCompletableFutures.from(channel.writeAndFlush(ack))
                .exceptionally(ex -> {
                    // Catch exception
                    log.error("[IOT-MQTT][{}] Failed to send packet connect ack to client.",
                            remoteAddress(),
                            ex);
                    return null;
                })
                .thenCompose(__ -> close());
    }
}
