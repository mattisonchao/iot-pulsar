package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.custom.VoidMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class DisConnectProcessor implements MqttProcessor {
    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        endpoint.close();

        //MUST discard any Will Message associated with the current connection without publishing it,
        // as described in Section 3.1.2.5 [MQTT-3.14.4-3].
        return CompletableFuture.supplyAsync(VoidMessage::create);
    }
}
