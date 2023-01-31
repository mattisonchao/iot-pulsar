package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.code.MqttPubReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * We chose method B to implement qos2.
 * See http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349369
 * [4.3.3 QoS 2: Exactly once delivery]
 */
@ThreadSafe
@Slf4j
public class PublishRelProcessorIn implements MqttProcessor {
    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        final MqttPubReplyMessageVariableHeader var =
                (MqttPubReplyMessageVariableHeader) message.variableHeader();
        int packetId = var.messageId();
        final MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false,
                        MqttQoS.AT_MOST_ONCE, false, 0);
        final MqttPubReplyMessageVariableHeader mqttPubAckVariableHeader =
                new MqttPubReplyMessageVariableHeader(packetId,
                        MqttPubReturnCode.ACCEPT.getByte(endpoint.version()),
                        MqttProperties.NO_PROPERTIES);
        return CompletableFuture.completedFuture(new MqttMessage(mqttFixedHeader, mqttPubAckVariableHeader));
    }
}
