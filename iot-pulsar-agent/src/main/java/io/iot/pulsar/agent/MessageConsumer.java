package io.iot.pulsar.agent;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

@FunctionalInterface
public interface MessageConsumer {
    void out(@Nonnull String topicName, @Nonnull byte[] messageId, @Nonnull ByteBuffer payload);
}
