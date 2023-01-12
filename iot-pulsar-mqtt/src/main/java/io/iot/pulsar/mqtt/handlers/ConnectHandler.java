package io.iot.pulsar.mqtt.handlers;

import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.common.options.IotPulsarMqttOptions;
import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.vertex.MqttVertex;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConnectHandler implements MqttVertexHandler<MqttConnectMessage> {
    private final PulsarAgent agent;
    private final IotPulsarMqttOptions options;

    public ConnectHandler(@Nonnull Mqtt mqtt) {
        this.agent = mqtt.getPulsarAgent();
        this.options = mqtt.getOptions();
    }

    @Override
    public void handle(@Nonnull MqttVertex vertex, @Nonnull MqttConnectMessage event) {
        final CompletableFuture<Void> authFuture;
        if (options.isEnableAuthentication()) {
            AuthData authData = vertex.authData();
            authFuture = agent.doAuthentication(authData.getMethod(), authData.getParameters())
                    .thenAccept(vertex::authRole);
        } else {
            authFuture = CompletableFuture.completedFuture(null);
        }
        authFuture.thenCompose(__ ->
                vertex.commandTraceLog(vertex.accept(MqttProperties.NO_PROPERTIES), "ACCEPT"));
    }
}
