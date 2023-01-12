package io.iot.pulsar.mqtt.messages.code;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttVersion;
import javax.annotation.Nonnull;
import lombok.Getter;

public enum MqttConnReturnCode {
    ACCEPT(CONNECTION_ACCEPTED, CONNECTION_ACCEPTED),
    PROTOCOL_ERROR(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, CONNECTION_REFUSED_PROTOCOL_ERROR),
    INVALID_CLIENT_IDENTIFIER(CONNECTION_REFUSED_IDENTIFIER_REJECTED, CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID),
    UNSUPPORTED_PROTOCOL_VERSION(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
            CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION);

    MqttConnReturnCode(@Nonnull MqttConnectReturnCode v3, @Nonnull MqttConnectReturnCode v5) {
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
}
