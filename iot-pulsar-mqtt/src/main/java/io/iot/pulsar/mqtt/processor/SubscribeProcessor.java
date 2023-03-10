package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.agent.MessageConsumer;
import io.iot.pulsar.agent.options.SubscribeOptions;
import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.custom.RawPublishMessage;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class SubscribeProcessor implements MqttProcessor {
    private final Mqtt mqtt;

    public SubscribeProcessor(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage message) {
        final MqttSubscribeMessage subscribeMessage = (MqttSubscribeMessage) message;
        final MqttMessageIdVariableHeader var = subscribeMessage.variableHeader();
        final MqttSubscribePayload payload = subscribeMessage.payload();
        int packetId = var.messageId();
        payload.topicSubscriptions().forEach(endpoint::addConnectedSubscription);
        return mqtt.getMetadataDelegator().updateSession(endpoint)
                .thenCompose(infoSubscriptions -> {
                    final List<CompletableFuture<MqttTopicSubscription>> futures = new ArrayList<>();
                    final List<MqttQoS> mqttQoses = new ArrayList<>();
                    for (MqttTopicSubscription subscription : payload.topicSubscriptions()) {
                        final String topicName = subscription.topicName();
                        final MqttQoS qos = subscription.option().qos();
                        final String subscriptionName = endpoint.identifier().getIdentifier();
                        mqttQoses.add(qos);
                        final MessageConsumer messageConsumer = (agentTopicName, agentMessageId, messagePayload) -> {
                            final RawPublishMessage rawPublishMessage =
                                    RawPublishMessage.create(topicName, agentTopicName, qos, agentMessageId,
                                            Unpooled.wrappedBuffer(messagePayload));
                            endpoint.processMessage(MqttProcessorController.Direction.OUT, rawPublishMessage);
                        };
                        final SubscribeOptions.SubscribeOptionsBuilder subscribeOptionsBuilder =
                                SubscribeOptions.builder()
                                        .subscriptionName(subscriptionName)
                                        .messageConsumer(messageConsumer);
                        if (qos == MqttQoS.AT_MOST_ONCE) {
                            subscribeOptionsBuilder.reader(true);
                        }
                        final SubscribeOptions options = subscribeOptionsBuilder.build();
                        final CompletableFuture<MqttTopicSubscription> subFuture =
                                mqtt.getPulsarAgent().subscribe(topicName, options)
                                        .thenApply(__ -> subscription);
                        // register hook when endpoint disconnect
                        endpoint.onClose(() -> mqtt.getPulsarAgent().disconnect(topicName, subscriptionName));
                        futures.add(subFuture);
                    }
                    // todo support multiple topic filter
                    final CompletableFuture<MqttMessage> future =
                            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                    .thenApply(__ -> MqttMessageBuilders.subAck()
                                            .packetId(packetId)
                        // The payload contains a list of return codes. Each return code corresponds to a Topic Filter
                        // in the SUBSCRIBE Packet being acknowledged. The order of return codes in the SUBACK Packet
                        // MUST match the order of Topic Filters in the SUBSCRIBE Packet [MQTT-3.9.3-1].
                                            .addGrantedQoses(mqttQoses.toArray(new MqttQoS[0]))
                                            .build());
                    return future.exceptionally(ex -> null).thenCompose(ack -> {
                        if (ack == null) {
                            payload.topicSubscriptions().forEach(endpoint::removeConnectedSubscription);
                            return mqtt.getMetadataDelegator().updateSession(endpoint)
                                    .thenApply(__ -> {
                                        // got exception and throw it.
                                        future.join();
                                        // never reached
                                        return null;
                                    });
                        }
                        return CompletableFuture.completedFuture(ack);
                    });
                }).exceptionally(ex -> {
                    log.error("[IOT-MQTT][{}][{}] Got an exception while subscribing.",
                            endpoint.remoteAddress(), endpoint.identifier(), ex);
                    return MqttMessageBuilders.subAck()
                            .packetId(packetId)
                            .addGrantedQos(MqttQoS.FAILURE)
                            .build();
                });
    }
}