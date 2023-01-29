package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.custom.RawPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class PublishProcessorOut implements MqttProcessor {
    private final Mqtt mqtt;

    public PublishProcessorOut(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        if (!(message instanceof RawPublishMessage)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unsupported message type: " + message.getClass()));
        }
        final RawPublishMessage rawPublishMessage = (RawPublishMessage) message;
        // todo wildcard topic name translation
        final String topicName = rawPublishMessage.getMetadata().getTopicName();
        final ByteBuf payload = rawPublishMessage.getPayload();
        final MqttQoS qos = rawPublishMessage.getMetadata().getQos();
        int packetId = endpoint.getPacketId(rawPublishMessage.getMetadata());
        return CompletableFuture.completedFuture(MqttMessageBuilders
                .publish()
                .qos(qos)
                .payload(payload)
                .topicName(topicName)
                .messageId(packetId)
                .retained(false)
                .properties(MqttProperties.NO_PROPERTIES)
                .build());
    }
}
