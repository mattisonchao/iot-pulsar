package io.iot.pulsar.mqtt.auth;

import lombok.Builder;
import lombok.Getter;

@Builder
public class AuthData {
    @Getter
    private String method;
    @Getter
    private String parameters;
}
