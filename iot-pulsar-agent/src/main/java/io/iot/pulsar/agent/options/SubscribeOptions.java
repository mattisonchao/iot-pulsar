package io.iot.pulsar.agent.options;

import io.iot.pulsar.agent.MessageConsumer;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SubscribeOptions {
    @Nonnull
    private final String subscriptionName;
    // todo support automatic clean up consumer
    @Builder.Default
    private final boolean agentHosted = false;
    @Builder.Default
    private final boolean reader = false;
    private final Map<String, String> properties;
    @Nullable
    @Builder.Default
    private final MessageConsumer messageConsumer = null;
}
