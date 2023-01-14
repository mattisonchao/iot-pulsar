package io.iot.pulsar.mqtt.endpoint;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MqttEndpointProperties {
    @Builder.Default
    private boolean cleanSession = true;
}
