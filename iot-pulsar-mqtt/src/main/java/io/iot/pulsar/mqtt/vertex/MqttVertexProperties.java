package io.iot.pulsar.mqtt.vertex;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MqttVertexProperties {
    @Builder.Default
    private boolean cleanSession = true;
}
