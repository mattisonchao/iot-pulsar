package io.iot.pulsar.agent;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * The pulsar client version of the pulsar agent.
 * It uses the pulsar standard client api and supports sending messages to brokers without the need to look them up.
 * <p>
 * Select it when the external protocol cannot support the lookup semantics.
 */
@ThreadSafe
public class PulsarClientAgentImpl implements PulsarAgent {
    public PulsarClientAgentImpl(BrokerService service) {

    }

    @Override
    public void close() {

    }

    @Nonnull
    @Override
    public CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters) {
        return CompletableFuture.completedFuture("");
    }
}
