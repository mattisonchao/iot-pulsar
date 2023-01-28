package io.iot.pulsar.mqtt.messages.custom;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

public class RawPublishMessage extends MqttMessage {
    @Getter
    private final Metadata metadata;
    @Getter
    private final ByteBuf payload;

    private RawPublishMessage(@Nonnull Metadata metadata, @Nonnull ByteBuf payload) {
        super(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.FAILURE, false, 0));
        this.metadata = metadata;
        this.payload = payload;
    }

    public static RawPublishMessage create(@Nonnull String topicName,
                                           @Nonnull String agentTopicName,
                                           @Nonnull MqttQoS qos,
                                           @Nonnull byte[] agentMessageId,
                                           @Nonnull ByteBuf payload) {
        final Metadata meta = Metadata.builder()
                .agentMessageId(agentMessageId)
                .agentTopicName(agentTopicName)
                .topicName(topicName)
                .qos(qos).build();
        return new RawPublishMessage(meta, payload);
    }

    @Data
    @Builder
    public static class Metadata {
        @Getter
        private final byte[] agentMessageId;
        private final String agentTopicName;
        @Getter
        private final String topicName;
        @Getter
        final MqttQoS qos;
    }
}
