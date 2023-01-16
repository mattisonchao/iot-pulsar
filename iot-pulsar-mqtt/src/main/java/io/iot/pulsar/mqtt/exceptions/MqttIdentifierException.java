package io.iot.pulsar.mqtt.exceptions;

import lombok.Getter;

public class MqttIdentifierException extends MqttException {
    public MqttIdentifierException(String message) {
        super(message);
    }

    public static class MqttIdentifierConflict extends MqttIdentifierException {
        @Getter
        private final String identifier;

        public MqttIdentifierConflict(String identifier) {
            super("Conflicting client identifier:" + identifier);
            this.identifier = identifier;
        }
    }
}
