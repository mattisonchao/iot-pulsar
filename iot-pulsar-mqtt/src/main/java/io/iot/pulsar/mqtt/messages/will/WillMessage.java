package io.iot.pulsar.mqtt.messages.will;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Builder;

@Builder
public class WillMessage {
    /**
     * If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00) [MQTT-3.1.2-13].
     * If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00), 1 (0x01), or 2 (0x02).
     * It MUST NOT be 3 (0x03) [MQTT-3.1.2-14].
     */
    private final MqttQoS willQos;
    /**
     * If the Will Flag is set to 0, then the Will Retain Flag MUST be set to 0 [MQTT-3.1.2-15].
     * If the Will Flag is set to 1:
     * If Will Retain is set to 0, the Server MUST publish the Will Message as a non-retained message [MQTT-3.1.2-16].
     * If Will Retain is set to 1, the Server MUST publish the Will Message as a retained message [MQTT-3.1.2-17].
     */
    private final boolean isRetain;

    private final String willTopic;
    private final MqttMessage message;
}
