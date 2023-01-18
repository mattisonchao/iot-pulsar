package io.iot.pulsar.mqtt.messages;

import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttProperties;
import javax.annotation.Nonnull;

public class MqttMessageTemplate {
    public static @Nonnull MqttMessage connReject(@Nonnull MqttConnectReturnCode connReturnCode,
                                                  @Nonnull MqttProperties properties) {
        if (connReturnCode.equals(MqttConnectReturnCode.CONNECTION_ACCEPTED)) {
            throw new IllegalArgumentException("Reject connect ack is not support return code " + connReturnCode);
        }
        return MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                new MqttConnAckVariableHeader(connReturnCode, false, properties), null);
    }
}
