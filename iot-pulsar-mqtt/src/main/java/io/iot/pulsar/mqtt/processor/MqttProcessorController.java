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
    private final Map<MqttMessageType, MqttProcessor> processorsIn = new ConcurrentHashMap<>();
    private final Map<MqttMessageType, MqttProcessor> processorOut = new ConcurrentHashMap<>();

    public enum Direction {
        IN, OUT
    }


    @Nonnull
    public CompletableFuture<MqttMessage> process(@Nonnull Direction direction,
                                                  @Nonnull MqttMessageType messageType,
                                                  @Nonnull MqttEndpoint endpoint,
                                                  @Nonnull MqttMessage mqttMessage) {
        final MqttProcessor mqttProcessor;
        switch (direction) {
            case IN:
                mqttProcessor = processorsIn.get(messageType);
                break;
            case OUT:
                mqttProcessor = processorOut.get(messageType);
                break;
            default:
                mqttProcessor = null;
        }
        if (mqttProcessor == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(String.format("Processor [%s] not found.", messageType)));
        }
        return mqttProcessor.process(endpoint, mqttMessage);
    }

    public void register(@Nonnull Direction direction,
                         @Nonnull MqttMessageType messageType,
                         @Nonnull MqttProcessor processor) {
        switch (direction) {
            case IN:
                processorsIn.put(messageType, processor);
                log.info("[IOT-MQTT] Mqtt in message {} processor is registered", messageType);
                break;
            case OUT:
                processorOut.put(messageType, processor);
                log.info("[IOT-MQTT] Mqtt out message {} processor is registered", messageType);
                break;
            default:
        }
    }
}
