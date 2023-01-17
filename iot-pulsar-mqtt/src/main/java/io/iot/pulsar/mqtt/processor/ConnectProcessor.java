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
import io.netty.handler.codec.mqtt.MqttVersion;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * If the Server accepts a connection with CleanSession set to 1, the Server MUST set Session Present to 0 in
 * the CONNACK packet in addition to setting a zero return code in the CONNACK packet [MQTT-3.2.2-1].
 * <p>
 * If the Server accepts a connection with CleanSession set to 0, the value set in Session Present depends on whether
 * the Server already has stored Session state for the supplied client ID. If the Server has stored Session state,
 * it MUST set Session Present to 1 in the CONNACK packet [MQTT-3.2.2-2]. If the Server does not have stored Session
 * state, it MUST set Session Present to 0 in the CONNACK packet. This is in addition to setting a zero return code
 * in the CONNACK packet [MQTT-3.2.2-3].
 * <p>
 * The Session Present flag enables a Client to establish whether the Client and Server have a consistent view about
 * whether there is already stored Session state.
 * <p>
 * Once the initial setup of a Session is complete, a Client with stored Session state will expect the Server to
 * maintain its stored Session state. In the event that the value of Session Present received by the Client from the
 * Server is not as expected, the Client can choose whether to proceed with the Session or to disconnect.
 * The Client can discard the Session state on both Client and Server by disconnecting, connecting with Clean Session
 * set to 1 and then disconnecting again.
 * <p>
 * If a server sends a CONNACK packet containing a non-zero return code it MUST set Session Present to 0 [MQTT-3.2.2-4].
 */
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
        // We don't support mqtt 5 yet.
        if (connectMessage.variableHeader().version() > MqttVersion.MQTT_3_1_1.protocolLevel()) {
            //The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01
            // (unacceptable protocol level) and then disconnect the Client if the Protocol
            // Level is not supported by the Server [MQTT-3.1.2-2].
            final MqttMessage rejectAck = MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                    new MqttConnAckVariableHeader(
                            MqttConnReturnCode.UNSUPPORTED_PROTOCOL_VERSION.getNettyCode(endpoint.version()),
                            false, MqttProperties.NO_PROPERTIES), null);
            return CompletableFuture.completedFuture(rejectAck);
        }
        // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
        // In cases where the ClientID is assigned by the Server, return the assigned ClientID.
        // This also lifts the restriction that Server assigned ClientIDs can only be used with Clean Session=1.
        if (!endpoint.properties().isCleanSession() && endpoint.identifier().isAssigned()) {
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
        CompletableFuture<MqttMessage> sendFuture =
                authFuture.thenApply(__ -> MqttMessageFactory.newMessage(MqttFixedHeaders.CONN_ACK,
                        new MqttConnAckVariableHeader(MqttConnReturnCode.ACCEPT.getNettyCode(endpoint.version()), false,
                                MqttProperties.NO_PROPERTIES), null));
        sendFuture.thenAccept(__ -> mqtt.getMetadataDelegator().registerAndListenKickOut(endpoint));
        return sendFuture
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
