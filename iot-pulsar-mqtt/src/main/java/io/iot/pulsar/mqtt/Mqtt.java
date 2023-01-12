package io.iot.pulsar.mqtt;

import static java.util.Objects.requireNonNull;
import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.common.options.IotPulsarMqttOptions;
import io.iot.pulsar.mqtt.handlers.ConnectHandler;
import javax.annotation.Nonnull;
import lombok.Getter;

public class Mqtt {
    @Getter
    private final PulsarAgent pulsarAgent;
    @Getter
    private final IotPulsarMqttOptions options;

    @Getter
    private final ConnectHandler connectHandler;

    private Mqtt(@Nonnull PulsarAgent pulsarAgent, @Nonnull IotPulsarMqttOptions options) {
        this.options = options;
        this.pulsarAgent = pulsarAgent;
        this.connectHandler = new ConnectHandler(this);
    }

    @Nonnull
    public static Mqtt create(@Nonnull PulsarAgent pulsarAgent, @Nonnull IotPulsarMqttOptions options) {
        requireNonNull(pulsarAgent, "Argument [pulsar agent] can not be null");
        requireNonNull(pulsarAgent, "Argument [options] can not be null");
        return new Mqtt(pulsarAgent, options);
    }

    @Nonnull
    public MqttInboundHandler createInboundHandler() {
        return new MqttInboundHandler(Mqtt.this);
    }

    public void close() {

    }
}
