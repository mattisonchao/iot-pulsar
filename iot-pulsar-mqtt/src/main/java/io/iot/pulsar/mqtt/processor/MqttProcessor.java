package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface MqttProcessor {

    @Nonnull
    CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message);
}
