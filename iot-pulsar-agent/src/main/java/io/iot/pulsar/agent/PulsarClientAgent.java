package io.iot.pulsar.agent;

import io.iot.pulsar.agent.metadata.Metadata;
import io.iot.pulsar.agent.metadata.SystemTopicMetadata;
import io.iot.pulsar.agent.options.SubscribeOptions;
import io.iot.pulsar.agent.pool.ThreadPools;
import io.iot.pulsar.agent.utils.TopicNameUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.naming.AuthenticationException;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;


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
    private final OrderedExecutor orderedExecutor;
    private final PulsarProducerManager producerManager;
    private final PulsarConsumerManager consumerManager;

    public PulsarClientAgent(@Nonnull BrokerService service) throws PulsarServerException {
        final PulsarClient client = service.getPulsar().getClient();
        this.metadata = new SystemTopicMetadata(client);
        // todo Maybe we need pulsar to enable authentication even when iot is not enabled, though.
        // But that would introduce more complexity and I think maybe we can wait for users.
        this.authenticationService = service.getAuthenticationService();
        this.authorizationService = service.getAuthorizationService();
        this.orderedExecutor =
                ThreadPools.createOrderedExecutor("iot-agent-ordered",
                        Runtime.getRuntime().availableProcessors());
        this.producerManager = new PulsarProducerManager(client, orderedExecutor);
        this.consumerManager = new PulsarConsumerManager(client, orderedExecutor);
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
    public CompletableFuture<String> publish(@Nonnull String topicName, @Nonnull ByteBuffer payload) {
        final TopicName pulsarTopicName;
        try {
            pulsarTopicName = TopicNameUtils.convertToPulsarName(topicName);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return producerManager.publish(pulsarTopicName, payload)
                .thenApply(Object::toString);
    }

    @Nonnull
    @Override
    public Metadata<String, byte[]> getMetadata() {
        return this.metadata;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> subscribe(@Nonnull String topicName, @Nonnull SubscribeOptions options) {
        final TopicName pulsarTopicName;
        try {
            pulsarTopicName = TopicNameUtils.convertToPulsarName(topicName);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return consumerManager.subscribe(pulsarTopicName, options);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> unSubscribe(@Nonnull String topicName, @Nonnull String subscriptionName) {
        final TopicName pulsarTopicName;
        try {
            pulsarTopicName = TopicNameUtils.convertToPulsarName(topicName);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return consumerManager.unSubscribe(pulsarTopicName, subscriptionName);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> disconnect(@Nonnull String topicName, @Nonnull String subscriptionName) {
        final TopicName pulsarTopicName;
        try {
            pulsarTopicName = TopicNameUtils.convertToPulsarName(topicName);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return consumerManager.disconnect(pulsarTopicName, subscriptionName);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> acknowledgement(@Nonnull String topicName, @Nonnull String subscriptionName,
                                                   @Nonnull byte[] messageId) {
        final TopicName pulsarTopicName;
        try {
            pulsarTopicName = TopicNameUtils.convertToPulsarName(topicName);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        final MessageId pulsarMessageId;
        try {
            pulsarMessageId = MessageId.fromByteArray(messageId);
        } catch (IOException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return consumerManager.acknowledgement(pulsarTopicName, subscriptionName, pulsarMessageId);
    }

}
