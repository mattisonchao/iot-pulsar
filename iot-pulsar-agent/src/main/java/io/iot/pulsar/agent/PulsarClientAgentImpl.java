package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.agent.metadata.SystemTopicMetadata;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * The pulsar client version of the pulsar agent.
 * It uses the pulsar standard client api and supports sending messages to brokers without the need to look them up.
 * <p>
 * Select it when the external protocol cannot support the lookup semantics.
 */
@ThreadSafe
public class PulsarClientAgentImpl implements PulsarAgent {
    private final Metadata<String, byte[]> metadata;

    public PulsarClientAgentImpl(BrokerService service) throws PulsarServerException {
        this.metadata = new SystemTopicMetadata(service.getPulsar().getClient());
    }

    @Override
    public void close() {
        metadata.close();
    }

    @Nonnull
    @Override
    public CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters) {
        return CompletableFuture.completedFuture("");
    }

    @Nonnull
    @Override
    public Metadata<String, byte[]> getMetadata() {
        return this.metadata;
    }
}
