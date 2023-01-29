package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * The Topic Filters (whether they contain wildcards or not) supplied in an UNSUBSCRIBE packet MUST be compared
 * character-by-character with the current set of Topic Filters held by the Server for the Client.
 * If any filter matches exactly then its owning Subscription is deleted,
 * otherwise no additional processing occurs [MQTT-3.10.4-1].
 * <p>
 * If a Server deletes a Subscription:
 * <p>
 * It MUST stop adding any new messages for delivery to the Client [MQTT-3.10.4-2].
 * It MUST complete the delivery of any QoS 1 or QoS 2 messages which
 * it has started to send to the Client [MQTT-3.10.4-3].
 * It MAY continue to deliver any existing messages buffered for delivery to the Client.
 * <p>
 * The Server MUST respond to an UNSUBSUBCRIBE request by sending an UNSUBACK packet.
 * The UNSUBACK Packet MUST have the same Packet Identifier as the UNSUBSCRIBE Packet [MQTT-3.10.4-4].
 * Even where no Topic Subscriptions are deleted, the Server MUST respond with an UNSUBACK [MQTT-3.10.4-5].
 * <p>
 * <p>
 * <p>
 * If a Server receives an UNSUBSCRIBE packet that contains multiple Topic Filters it MUST handle that
 * packet as if it had received a sequence of multiple UNSUBSCRIBE packets,
 * except that it sends just one UNSUBACK response [MQTT-3.10.4-6].
 */
@ThreadSafe
@Slf4j
public class UnsubscribeProcessor implements MqttProcessor {
    private final Mqtt mqtt;

    public UnsubscribeProcessor(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        final MqttUnsubscribeMessage unsubscribeMessage = (MqttUnsubscribeMessage) message;
        final MqttMessageIdAndPropertiesVariableHeader var =
                unsubscribeMessage.idAndPropertiesVariableHeader();
        final MqttUnsubscribePayload payload = unsubscribeMessage.payload();
        int packetId = var.messageId();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topic : payload.topics()) {
            final CompletableFuture<Void> future =
                    mqtt.getPulsarAgent().unSubscribe(topic, endpoint.identifier().getIdentifier());
            futures.add(future);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(__ -> (MqttMessage) MqttMessageBuilders.unsubAck()
                        .packetId(packetId)
                        .build())
                .exceptionally(ex -> {
                    log.error("[IOT-MQTT][{}][{}] Got an exception while unsubscribing.",
                            endpoint.remoteAddress(), endpoint.identifier(), ex);
                    return MqttMessageBuilders.unsubAck()
                            .packetId(packetId)
                            .build();
                });
    }
}
