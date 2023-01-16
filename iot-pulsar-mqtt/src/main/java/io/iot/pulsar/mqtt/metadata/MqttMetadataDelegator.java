package io.iot.pulsar.mqtt.metadata;

import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.agent.metadata.Metadata;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

// todo This delegator will use protobuf to serialize/deserialize
public class MqttMetadataDelegator {
    private final Metadata<String, byte[]> agentMetadata;
    private static final String CLIENT_IDENTIFIER_KEY = "MQTT_CLIENT_IDENTIFIER";

    public MqttMetadataDelegator(@Nonnull PulsarAgent agent) {
        this.agentMetadata = agent.getMetadata();
    }

    @Nonnull
    public CompletableFuture<Boolean> clientIdentifierExist(@Nonnull String identifier) {
        return agentMetadata.get(CLIENT_IDENTIFIER_KEY)
                .thenApply(identifiers -> {
                    // todo
                    return false;
                });
    }

}
