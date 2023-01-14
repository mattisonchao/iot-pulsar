package io.iot.pulsar.mqtt.processor;

import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.common.options.IotPulsarMqttOptions;
import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.endpoint.MqttEndpoint;
import io.iot.pulsar.mqtt.messages.MqttFixedHeaders;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.iot.pulsar.mqtt.utils.CompletableFutures;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
public class ConnectProcessor implements MqttProcessor {
    private final Mqtt mqtt;

    public ConnectProcessor(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Nonnull
    @Override
    public CompletableFuture<MqttMessage> process(@Nonnull MqttEndpoint endpoint, @Nonnull MqttMessage event) {
        if (!(event instanceof MqttConnectMessage)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    String.format("Connect processor can't process %s message", event.getClass())));
        }
        final IotPulsarMqttOptions options = mqtt.getOptions();
        final PulsarAgent agent = mqtt.getPulsarAgent();
        final MqttConnectMessage connectMessage = (MqttConnectMessage) event;
        // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
        // In cases where the ClientID is assigned by the Server, return the assigned ClientID.
        // This also lifts the restriction that Server assigned ClientIDs can only be used with Clean Session=1.
        if (!endpoint.properties().isCleanSession()
                && endpoint.identifier().isAssigned()) {
            final MqttMessage rejectAck = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                    new MqttConnAckVariableHeader(
                            MqttConnReturnCode.INVALID_CLIENT_IDENTIFIER.getNettyCode(endpoint.version()),
                            false, MqttProperties.NO_PROPERTIES), null);
            return CompletableFuture.completedFuture(rejectAck);
        }
        endpoint.setKeepAlive(connectMessage.variableHeader().keepAliveTimeSeconds());

        final CompletableFuture<Void> authFuture;
        if (options.isEnableAuthentication()) {
            AuthData authData = endpoint.authData();
            authFuture = agent.doAuthentication(authData.getMethod(), authData.getParameters())
                    .thenAccept(endpoint::authRole);
        } else {
            authFuture = CompletableFuture.completedFuture(null);
        }
        return authFuture.thenApply(__ -> MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                        new MqttConnAckVariableHeader(MqttConnReturnCode.ACCEPT.getNettyCode(endpoint.version()), false,
                                MqttProperties.NO_PROPERTIES), null))
                .exceptionally(ex -> {
                    Throwable realCause = CompletableFutures.unwrap(ex);
                    log.error("[IOT-MQTT][{}] Got an error when processor process connect messages.",
                            endpoint.remoteAddress(), realCause);
                    return MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                            new MqttConnAckVariableHeader(
                                    MqttConnReturnCode.SERVER_INTERNAL_ERROR.getNettyCode(endpoint.version()),
                                    false, MqttProperties.NO_PROPERTIES), null);
                });
    }
}
