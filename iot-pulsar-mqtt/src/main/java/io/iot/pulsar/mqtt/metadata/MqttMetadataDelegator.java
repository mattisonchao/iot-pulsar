package io.iot.pulsar.mqtt.metadata;

import com.google.protobuf.InvalidProtocolBufferException;
import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.mqtt.api.proto.MqttMetadataEvent;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import javax.annotation.Nonnull;

public class MqttMetadataDelegator {
    private final Metadata<String, byte[]> agentMetadata;

    public MqttMetadataDelegator(@Nonnull PulsarAgent agent) {
        this.agentMetadata = agent.getMetadata();
    }

    /**
     * If the ClientId represents a Client already connected to the Server
     * then the Server MUST disconnect the existing Client [MQTT-3.1.4-2].
     */
    public void registerAndListenKickOut(MqttEndpoint endpoint) {
        agentMetadata.listen(endpoint.identifier().getIdentifier(), (value) -> {
            try {
                final MqttMetadataEvent.ClientInfo unsureNewOrOldInfo = MqttMetadataEvent.ClientInfo.parseFrom(value);
                if (unsureNewOrOldInfo.getConnectTime() >= endpoint.initTime()
                        && !unsureNewOrOldInfo.getAddress().equals(endpoint.remoteAddress().toString())) {
                    // kick out itself
                    endpoint.close();
                }
            } catch (InvalidProtocolBufferException ex) {
                throw new IllegalArgumentException(ex);
            }
        });
        agentMetadata.put(endpoint.identifier().getIdentifier(), MqttMetadataEvent.ClientInfo.newBuilder()
                .setAddress(endpoint.remoteAddress().toString())
                .setConnectTime(System.currentTimeMillis())
                .build()
                .toByteArray());
    }
}
