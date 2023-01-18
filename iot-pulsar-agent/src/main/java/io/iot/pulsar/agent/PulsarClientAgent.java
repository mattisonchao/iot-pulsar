package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.agent.metadata.SystemTopicMetadata;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * The pulsar client version of the pulsar agent.
 * It uses the pulsar standard client api and supports sending messages to brokers without the need to look them up.
 * <p>
 * Select it when the external protocol cannot support the lookup semantics.
 */
@ThreadSafe
public class PulsarClientAgent implements PulsarAgent {
    private final Metadata<String, byte[]> metadata;
    private final AuthenticationService authenticationService;
    private final AuthorizationService authorizationService;

    public PulsarClientAgent(BrokerService service) throws PulsarServerException {
        this.metadata = new SystemTopicMetadata(service.getPulsar().getClient());
        // todo Maybe we need pulsar to enable authentication even when iot is not enabled, though.
        // But that would introduce more complexity and I think maybe we can wait for users.
        this.authenticationService = service.getAuthenticationService();
        this.authorizationService = service.getAuthorizationService();
    }

    @Override
    public void close() {
        metadata.close();
    }

    @Nonnull
    @Override
    public CompletableFuture<String> doAuthentication(@Nonnull String method, @Nonnull String parameters) {
        try {
            final String role = authenticationService.getAuthenticationProvider(method)
                    .authenticate(new AuthenticationDataCommand(parameters));
            return CompletableFuture.completedFuture(role);
        } catch (AuthenticationException ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    @Nonnull
    @Override
    public Metadata<String, byte[]> getMetadata() {
        return this.metadata;
    }
}
