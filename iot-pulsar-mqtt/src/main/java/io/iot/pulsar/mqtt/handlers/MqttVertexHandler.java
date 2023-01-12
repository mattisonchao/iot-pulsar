package io.iot.pulsar.mqtt.handlers;

import io.iot.pulsar.mqtt.vertex.MqttVertex;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface MqttVertexHandler<T> {

    void handle(@Nonnull MqttVertex vertex, @Nonnull T event);
}
