package io.iot.pulsar.mqtt.exceptions;

public class MqttException extends RuntimeException {
    public MqttException(String message) {
        super(message);
    }
}
