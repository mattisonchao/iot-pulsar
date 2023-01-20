package io.iot.pulsar.mqtt.messages.code;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QOS_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttVersion;
import javax.annotation.Nonnull;
import lombok.Getter;

public enum MqttPubReturnCode {
    ACCEPT(CONNECTION_ACCEPTED, CONNECTION_ACCEPTED),

    ILLEGAL_TOPIC_NAME(CONNECTION_REFUSED_SERVER_UNAVAILABLE, CONNECTION_REFUSED_TOPIC_NAME_INVALID),
    UNSUPPORTED_QOS(CONNECTION_REFUSED_SERVER_UNAVAILABLE, CONNECTION_REFUSED_QOS_NOT_SUPPORTED),
    SERVER_INTERNAL_ERROR(CONNECTION_REFUSED_UNSPECIFIED_ERROR, CONNECTION_REFUSED_UNSPECIFIED_ERROR);


    MqttPubReturnCode(@Nonnull MqttConnectReturnCode v3, @Nonnull MqttConnectReturnCode v5) {
        this.v3 = v3;
        this.v5 = v5;
    }

    @Getter
    private final MqttConnectReturnCode v3;
    @Getter
    private final MqttConnectReturnCode v5;

    @Nonnull
    public MqttConnectReturnCode getNettyCode(@Nonnull MqttVersion version) {
        if (version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()) {
            return v5;
        }
        return v3;
    }

    @Nonnull
    public byte getByte(@Nonnull MqttVersion version) {
        if (version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()) {
            return v5.byteValue();
        }
        return v3.byteValue();
    }
}
