package io.iot.pulsar.agent.exceptions;

import javax.annotation.Nonnull;

public class IotAgentSubscriptionException extends IotAgentException {
    public IotAgentSubscriptionException(@Nonnull String message) {
        super(message);
    }

    public static class NotSubscribeException extends IotAgentSubscriptionException {

        public NotSubscribeException(@Nonnull String subscriptionName) {
            super(String.format("Subscription %s is not subscribed yet.", subscriptionName));
        }
    }
}
