package io.iot.pulsar.mqtt.endpoint;

import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.messages.Identifier;
import io.iot.pulsar.mqtt.messages.custom.RawPublishMessage;
import io.iot.pulsar.mqtt.processor.MqttProcessorController;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public interface MqttEndpoint {

    default long initTime() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default MqttVersion version() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default AuthData authData() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default String authRole() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    default void authRole(@Nonnull String role) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    default void processorController(@Nonnull MqttProcessorController controller) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default Identifier identifier() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default MqttEndpointProperties properties() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    SocketAddress remoteAddress();

    @Nonnull
    CompletableFuture<Void> close();

    default int getPacketId(@Nonnull RawPublishMessage.Metadata agentMessageMetadata) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    default Optional<RawPublishMessage.Metadata> getAgentMessageMetadata(int packetId) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    default void releasePacketId(int packetId) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    void processMessage(@Nonnull MqttProcessorController.Direction direction, @Nonnull MqttMessage mqttMessage);

    default void setKeepAlive(int keepAliveTimeSeconds) {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
