package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class MqttProcessorController {
    private final Map<MqttMessageType, MqttProcessor> processors = new ConcurrentHashMap<>();

    @Nonnull
    public CompletableFuture<MqttMessage> process(@Nonnull MqttMessageType messageType,
                                                  @Nonnull MqttEndpoint endpoint,
                                                  @Nonnull MqttMessage mqttMessage) {
        MqttProcessor mqttProcessor = processors.get(messageType);
        if (mqttProcessor == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(String.format("Processor [%s] not found.", messageType)));
        }
        switch (messageType) {
            case CONNECT:
                return mqttProcessor.process(endpoint, mqttMessage);
            default:
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException(String.format("Unsupported operation %s", messageType)));
        }
    }

    public void register(@Nonnull MqttMessageType messageType, @Nonnull MqttProcessor processor) {
        processors.put(messageType, processor);
        log.info("[IOT-MQTT] Mqtt message {} processor is registered", messageType);
    }
}
