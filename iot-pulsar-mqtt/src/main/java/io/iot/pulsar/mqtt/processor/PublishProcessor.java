package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.code.MqttPubReturnCode;
import io.iot.pulsar.mqtt.utils.EnhanceCompletableFutures;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class PublishProcessor implements MqttProcessor {

    private final Mqtt mqtt;

    public PublishProcessor(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        MqttPublishMessage publishMessage = (MqttPublishMessage) message;
        final MqttFixedHeader fixed = publishMessage.fixedHeader();
        MqttPublishVariableHeader var = publishMessage.variableHeader();
        int packetId = var.packetId();
        final MqttQoS mqttQoS = fixed.qosLevel();
        // Support at least once only
        if (mqttQoS != MqttQoS.AT_LEAST_ONCE) {
            MqttMessage rejectPubAck = MqttMessageBuilders
                    .pubAck()
                    .packetId(packetId)
                    .reasonCode(MqttPubReturnCode.UNSUPPORTED_QOS.getByte(endpoint.version()))
                    .build();
            return CompletableFuture.completedFuture(rejectPubAck);
        }
        // todo support retain
        boolean retain = fixed.isRetain();
        // todo support dup
        boolean dup = fixed.isDup();
        final String topicName = var.topicName();
        final ByteBuf payload = publishMessage.payload();
        return mqtt.getPulsarAgent().publish(topicName, payload.nioBuffer())
                .thenApply(messageId -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[IOT-MQTT][{}][{}] has sent a message {} to topic {}. qos: {}",
                                endpoint.remoteAddress(), endpoint.identifier(), packetId, topicName, mqttQoS);
                    }
                    return MqttMessageBuilders.pubAck()
                            .packetId(packetId)
                            .reasonCode(MqttPubReturnCode.ACCEPT.getByte(endpoint.version()))
                            .build();
                })
                .exceptionally(ex -> {
                    final Throwable rc = EnhanceCompletableFutures.unwrap(ex);
                    log.error("[IOT-MQTT][{}][{}][{}] Got an exception while publishing message.",
                            endpoint.remoteAddress(), endpoint.identifier(), topicName, ex);
                    if (rc instanceof IllegalArgumentException) {
                        return MqttMessageBuilders.pubAck()
                                .packetId(packetId)
                                .reasonCode(MqttPubReturnCode.ILLEGAL_TOPIC_NAME.getByte(endpoint.version()))
                                .build();
                    }
                    return MqttMessageBuilders.pubAck()
                            .packetId(packetId)
                            .reasonCode(MqttPubReturnCode.SERVER_INTERNAL_ERROR.getByte(endpoint.version()))
                            .build();
                });
    }
}
