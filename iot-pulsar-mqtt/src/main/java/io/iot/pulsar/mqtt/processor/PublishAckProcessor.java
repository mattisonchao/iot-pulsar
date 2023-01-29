package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.custom.RawPublishMessage;
import io.iot.pulsar.mqtt.messages.custom.VoidMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PublishAckProcessor implements MqttProcessor {
    private final Mqtt mqtt;

    public PublishAckProcessor(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        final MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) message;
        final MqttMessageIdVariableHeader var = pubAckMessage.variableHeader();
        int packetId = var.messageId();
        final Optional<RawPublishMessage.Metadata> agentMessageMetadata = endpoint.getAgentMessageMetadata(packetId);
        if (agentMessageMetadata.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[IOT-MQTT][{}][{}] receive unknown packet id {}", endpoint.remoteAddress(),
                        endpoint.identifier(), packetId);
            }
            return CompletableFuture.completedFuture(VoidMessage.create());
        }
        final RawPublishMessage.Metadata metadata = agentMessageMetadata.get();
        return mqtt.getPulsarAgent()
                .acknowledgement(metadata.getTopicName(), endpoint.identifier().getIdentifier(),
                        metadata.getAgentMessageId())
                // we can't do anything here.
                .thenApply(__ -> {
                    endpoint.releasePacketId(packetId);
                    return VoidMessage.create();
                });
    }
}
