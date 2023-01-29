package io.iot.pulsar.agent.exceptions;

import javax.annotation.Nonnull;

public class IotAgentException extends RuntimeException {
    public IotAgentException(@Nonnull String message) {
        super(message);
    }
}
