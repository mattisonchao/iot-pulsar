package io.iot.pulsar.mqtt.metadata;

import com.google.protobuf.InvalidProtocolBufferException;
import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.mqtt.api.proto.MqttMetadataEvent;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class MqttMetadataDelegator {
    private final Metadata<String, byte[]> agentMetadata;
    private static final String CONNECT_EVENT_PREFIX = "connect_";

    public MqttMetadataDelegator(@Nonnull PulsarAgent agent) {
        this.agentMetadata = agent.getMetadata();
    }

    public @Nonnull CompletableFuture<Void> updateSession(
            @Nonnull MqttEndpoint endpoint) {
        final String infoKey = endpoint.identifier().getIdentifier();
        final MqttMetadataEvent.Session session = MqttMetadataEvent.Session.newBuilder()
                .addAllSubscriptions(
                        endpoint.subscriptions().stream().map(sub -> MqttMetadataEvent.Subscription.newBuilder()
                                .setQosValue(sub.qualityOfService().value())
                                .setTopicFilter(sub.topicName())
                                .build()).collect(Collectors.toList()))
                .build();
        final MqttMetadataEvent.ClientInfo info = MqttMetadataEvent.ClientInfo.newBuilder()
                .setAddress(endpoint.remoteAddress().toString())
                .setConnectTime(endpoint.connectTime())
                .setSession(session)
                .build();
        return agentMetadata.put(infoKey, info.toByteArray());
    }

    /**
     * If the ClientId represents a Client already connected to the Server
     * then the Server MUST disconnect the existing Client [MQTT-3.1.4-2].
     */
    public @Nonnull CompletableFuture<Void> listenAndSendConnect(@Nonnull MqttEndpoint endpoint) {
        listenSession(endpoint);
        return listenAndSendConnect0(endpoint);
    }

    private void listenSession(@Nonnull MqttEndpoint endpoint) {
        final String infoKey = endpoint.identifier().getIdentifier();
        final CompletableFuture<Void> cleanupListener = agentMetadata.listen(infoKey, (value) -> {
            try {
                final MqttMetadataEvent.ClientInfo info =
                        MqttMetadataEvent.ClientInfo.parseFrom(value);
                endpoint.updateInfo(info);
            } catch (InvalidProtocolBufferException ex) {
                throw new IllegalArgumentException(ex);
            }
        });
        endpoint.onClose(() -> {
            // cleanup listener
            cleanupListener.complete(null);
            return cleanupListener;
        });
    }

    private @Nonnull CompletableFuture<Void> listenAndSendConnect0(@Nonnull MqttEndpoint endpoint) {
        final String connectEventKey = getConnectEventKey(endpoint);
        return agentMetadata.put(connectEventKey,
                        MqttMetadataEvent.ConnectEvent.newBuilder()
                                .setAddress(endpoint.remoteAddress().toString())
                                .setConnectTime(endpoint.connectTime())
                                .build().toByteArray())
                .thenAccept(__ -> {
                    final CompletableFuture<Void> cleanupListener = agentMetadata.listen(connectEventKey, (value) -> {
                        try {
                            final MqttMetadataEvent.ConnectEvent unsureNewOrOldInfo =
                                    MqttMetadataEvent.ConnectEvent.parseFrom(value);
                            if (unsureNewOrOldInfo.getConnectTime() >= endpoint.initTime()
                                    && !unsureNewOrOldInfo.getAddress().equals(endpoint.remoteAddress().toString())) {
                                // kick out itself
                                endpoint.close();
                            }
                        } catch (InvalidProtocolBufferException ex) {
                            throw new IllegalArgumentException(ex);
                        }
                    });
                    endpoint.onClose(() -> {
                        // cleanup listener
                        cleanupListener.complete(null);
                        return cleanupListener;
                    });
                });
    }

    @Nonnull
    private static String getConnectEventKey(@Nonnull MqttEndpoint endpoint) {
        return CONNECT_EVENT_PREFIX + endpoint.identifier().getIdentifier();
    }
}
