package io.iot.pulsar.mqtt;

import static java.util.Objects.requireNonNull;
import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.common.options.IotPulsarMqttOptions;
import io.iot.pulsar.mqtt.metadata.MqttMetadataDelegator;
import io.iot.pulsar.mqtt.processor.ConnectProcessor;
import io.iot.pulsar.mqtt.processor.DisconnectProcessor;
import io.iot.pulsar.mqtt.processor.MqttProcessorController;
import io.iot.pulsar.mqtt.processor.PingProcessor;
import io.iot.pulsar.mqtt.processor.PublishAckProcessor;
import io.iot.pulsar.mqtt.processor.PublishProcessorIn;
import io.iot.pulsar.mqtt.processor.PublishProcessorOut;
import io.iot.pulsar.mqtt.processor.PublishRelProcessorIn;
import io.iot.pulsar.mqtt.processor.SubscribeProcessor;
import io.iot.pulsar.mqtt.processor.UnsubscribeProcessor;
import io.netty.handler.codec.mqtt.MqttMessageType;
import javax.annotation.Nonnull;
import lombok.Getter;

public class Mqtt {
    @Getter
    private final PulsarAgent pulsarAgent;
    @Getter
    private final IotPulsarMqttOptions options;
    @Getter
    private final MqttMetadataDelegator metadataDelegator;

    @Getter
    private final MqttProcessorController processorController = new MqttProcessorController();

    private Mqtt(@Nonnull PulsarAgent pulsarAgent, @Nonnull IotPulsarMqttOptions options) {
        this.options = options;
        this.pulsarAgent = pulsarAgent;
        this.metadataDelegator = new MqttMetadataDelegator(pulsarAgent);
    }

    {
        // Automatic register
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.PINGREQ,
                new PingProcessor());
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.CONNECT,
                new ConnectProcessor(this));
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.PUBLISH,
                new PublishProcessorIn(this));
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.SUBSCRIBE,
                new SubscribeProcessor(this));
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.UNSUBSCRIBE,
                new UnsubscribeProcessor(this));
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.PUBACK,
                new PublishAckProcessor(this));
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.DISCONNECT,
                new DisconnectProcessor());
        processorController.register(MqttProcessorController.Direction.IN, MqttMessageType.PUBREL,
                new PublishRelProcessorIn());

        processorController.register(MqttProcessorController.Direction.OUT, MqttMessageType.PUBLISH,
                new PublishProcessorOut(this));
    }

    @Nonnull
    public static Mqtt create(@Nonnull PulsarAgent pulsarAgent, @Nonnull IotPulsarMqttOptions options) {
        requireNonNull(pulsarAgent, "Argument [pulsar agent] can not be null");
        requireNonNull(pulsarAgent, "Argument [options] can not be null");
        return new Mqtt(pulsarAgent, options);
    }

    @Nonnull
    public MqttInboundHandler createInboundHandler() {
        return new MqttInboundHandler(this);
    }

    public void close() {

    }
}
