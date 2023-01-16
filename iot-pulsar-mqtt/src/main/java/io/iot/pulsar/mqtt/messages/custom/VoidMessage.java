package io.iot.pulsar.mqtt.messages.custom;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;

public class VoidMessage extends MqttMessage {
    private VoidMessage() {
        super(null);
    }

    public static VoidMessage create() {
        return new VoidMessage();
    }

    @Override
    public MqttFixedHeader fixedHeader() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Object variableHeader() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Object payload() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public DecoderResult decoderResult() {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
