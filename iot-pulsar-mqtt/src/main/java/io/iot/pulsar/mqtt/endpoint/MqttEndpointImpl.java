package io.iot.pulsar.mqtt.endpoint;

import com.google.common.base.Throwables;
import io.iot.pulsar.mqtt.MqttChannelInitializer;
import io.iot.pulsar.mqtt.api.proto.MqttMetadataEvent;
import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.messages.Identifier;
import io.iot.pulsar.mqtt.messages.custom.RawPublishMessage;
import io.iot.pulsar.mqtt.messages.custom.VoidMessage;
import io.iot.pulsar.mqtt.messages.will.WillMessage;
import io.iot.pulsar.mqtt.processor.MqttProcessorController;
import io.iot.pulsar.mqtt.utils.EnhanceCompletableFutures;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
@Builder
@ThreadSafe
public class MqttEndpointImpl implements MqttEndpoint {
    private final MqttProcessorController processorController;
    private final ChannelHandlerContext ctx;
    private final Identifier identifier;
    private final MqttVersion version;
    private final AuthData authData;
    private final MqttEndpointProperties properties;
    private volatile String authRole;
    private final boolean willFlag;
    private final WillMessage willMessage;
    private final RoaringBitmap availablePacketIds = new RoaringBitmap();
    private final Map<Integer, RawPublishMessage.Metadata> packetIdPair = new ConcurrentHashMap<>();
    private volatile long connectTime;
    private final List<Supplier<CompletableFuture<Void>>> closeCallbacks =
            Collections.synchronizedList(new ArrayList<>());
    private final Map<String, MqttTopicSubscription> subscriptions = new ConcurrentHashMap<>();
    private volatile Status status = Status.INIT;
    private static final VarHandle STATUS;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATUS = l.findVarHandle(MqttEndpointImpl.class, "status", Status.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
        // Reduce the risk of rare disastrous classloading in first call to
        // LockSupport.park: https://bugs.openjdk.java.net/browse/JDK-8074773
        Class<?> ensureLoaded = LockSupport.class;
    }

    private volatile CompletableFuture<Void> closeFuture;

    enum Status {
        INIT,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED
    }

    @Nonnull
    @Override
    public MqttVersion version() {
        return version;
    }

    @Nonnull
    @Override
    public Identifier identifier() {
        return identifier;
    }

    @Nonnull
    @Override
    public AuthData authData() {
        return authData;
    }

    @Nonnull
    @Override
    public MqttEndpointProperties properties() {
        return properties;
    }

    @Nonnull
    @Override
    public SocketAddress remoteAddress() {
        return ctx.channel().remoteAddress();
    }

    @Nonnull
    @Override
    public String authRole() {
        return authRole;
    }

    @Override
    public void authRole(@Nonnull String role) {
        MqttEndpointImpl.this.authRole = role;
    }

    @Nonnull
    @Override
    public synchronized CompletableFuture<Void> close() {
        if (stopWorking()) {
            return closeFuture;
        }
        CompletableFuture<Void> cbFuture =
                CompletableFuture.allOf(closeCallbacks.stream().map(Supplier::get)
                                .collect(Collectors.toList()).toArray(CompletableFuture[]::new))
                        .exceptionally(ex -> {
                            log.warn("[IOT-MQTT][{}] Got exception while invoke callback.", remoteAddress(),
                                    Throwables.getRootCause(ex));
                            return null;
                        });
        closeFuture = cbFuture.thenCompose(__ -> {
            STATUS.set(this, Status.CLOSING);
            return EnhanceCompletableFutures.from(ctx.channel().close());
        }).thenAccept(__ -> STATUS.set(this, Status.CLOSED));
        return closeFuture;
    }

    @Override
    public int getPacketId(@Nonnull RawPublishMessage.Metadata agentMessageMetadata) {
        synchronized (availablePacketIds) {
            // 0 is invalid message id
            int nextPacketId = (int) availablePacketIds.nextAbsentValue(1);
            availablePacketIds.add(nextPacketId);
            packetIdPair.put(nextPacketId, agentMessageMetadata);
            return nextPacketId;
        }
    }

    @Nonnull
    @Override
    public Optional<RawPublishMessage.Metadata> getAgentMessageMetadata(int packetId) {
        return Optional.ofNullable(packetIdPair.get(packetId));
    }

    @Override
    public void releasePacketId(int packetId) {
        synchronized (availablePacketIds) {
            packetIdPair.remove(packetId);
            availablePacketIds.remove(packetId);
        }
    }

    @Override
    public long initTime() {
        return initTime;
    }

    @Override
    public void processMessage(@Nonnull MqttProcessorController.Direction direction, @Nonnull MqttMessage mqttMessage) {
        if (stopWorking()) {
            return;
        }
        processorController.process(direction, mqttMessage.fixedHeader().messageType(), this,
                        mqttMessage)
                // todo: Improve the writing process. Not all messages need to flush immediately.
                .whenComplete((ack, processError) -> {
                    if (processError != null) {
                        final Throwable rc = EnhanceCompletableFutures.unwrap(processError);
                        log.error("[IOT-MQTT][{}] Got exception while process message {}", remoteAddress(),
                                mqttMessage, rc);
                        // Unexpected exception, close the endpoint.
                        close();
                        return;
                    }
                    // We don't need to do anything with void message.
                    if (ack instanceof VoidMessage) {
                        return;
                    }
                    EnhanceCompletableFutures.from(ctx.channel().writeAndFlush(ack))
                            .whenComplete((__, ex) -> {
                                if (ex != null) {
                                    log.error("[IOT-MQTT][{}] Failed to send packet [{}] to client.", remoteAddress(),
                                            ack.fixedHeader().messageType(), ex);
                                    return;
                                }
                                if (ack instanceof MqttConnAckMessage) {
                                    STATUS.set(this, Status.CONNECTED);
                                }
                                if (log.isDebugEnabled()) {
                                    log.debug("[IOT-MQTT][{}] Successfully sent packet [{}] to client.",
                                            remoteAddress(), ack.fixedHeader().messageType());
                                }
                            });
                });
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public void setKeepAlive(int keepAliveTimeSeconds) {

        ctx.pipeline().remove(MqttChannelInitializer.CONNECT_IDLE_NAME);
        ctx.pipeline().remove(MqttChannelInitializer.CONNECT_TIMEOUT_NAME);
        /*
          If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the
          Client within one and a half times the Keep Alive time period,
          it MUST disconnect the Network Connection to the Client as if the network had failed [MQTT-3.1.2-24].
         */
        if (keepAliveTimeSeconds > 0) {
            ctx.pipeline().addLast("keepAliveIdle",
                    new IdleStateHandler(keepAliveTimeSeconds + (keepAliveTimeSeconds / 2), 0, 0));
            ctx.pipeline().addLast("keepAliveHandler", new ChannelDuplexHandler() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                    if (event instanceof IdleStateEvent
                            && ((IdleStateEvent) event).state() == IdleState.READER_IDLE) {
                        MqttEndpointImpl.this.close();
                    }
                }
            });
        }
    }

    @Override
    public void connectTime(long currentTime) {
        if (STATUS.compareAndSet(this, Status.INIT, Status.CONNECTING)) {
            this.connectTime = currentTime;
        }
    }

    @Override
    public void onClose(@Nonnull Supplier<CompletableFuture<Void>> callback) {
        closeCallbacks.add(callback);
    }

    @Override
    public synchronized void updateInfo(@Nonnull MqttMetadataEvent.ClientInfo info) {
        if (stopWorking()) {
            return;
        }
        if (properties().isCleanSession()) {
            return;
        }
        final MqttMetadataEvent.Session session = info.getSession();
        final List<MqttMetadataEvent.Subscription> old = session.getSubscriptionsList();
        for (MqttMetadataEvent.Subscription subscription : old) {
            if (subscriptions.containsKey(subscription.getTopicFilter())) {
                continue;
            }
            final MqttSubscribeMessage subRequest = MqttMessageBuilders.subscribe()
                    .messageId(1)
                    .addSubscription(MqttQoS.valueOf(subscription.getQos().getNumber()), subscription.getTopicFilter())
                    .build();
            processorController.process(MqttProcessorController.Direction.IN, MqttMessageType.SUBSCRIBE, this,
                            subRequest)
                    .thenAccept(__ -> {
                        log.info("[IOT-MQTT][{}][{}][{}] auto recovered session subscription.",
                                remoteAddress(), subscription.getTopicFilter(), subscription.getQos());
                    }).exceptionally(ex -> {
                        log.error("[IOT-MQTT][{}][{}][{}] Got exception while auto recover session subscription.",
                                remoteAddress(), subscription.getTopicFilter(), subscription.getQos(),
                                Throwables.getRootCause(ex));
                        return null;
                    });
        }
    }

    @Override
    public void addConnectedSubscription(@Nonnull MqttTopicSubscription subscription) {
        subscriptions.put(subscription.topicName(), subscription);
    }

    @Override
    public Collection<MqttTopicSubscription> subscriptions() {
        return subscriptions.values();
    }

    private boolean stopWorking() {
        final Status status = (Status) STATUS.getVolatile(this);
        return status == Status.CLOSING || status == Status.CLOSED;
    }

    @Builder.Default
    private final long initTime = System.currentTimeMillis();
}
