package io.iot.pulsar.mqtt.endpoint;

import io.iot.pulsar.mqtt.MqttChannelInitializer;
import io.iot.pulsar.mqtt.auth.AuthData;
import io.iot.pulsar.mqtt.messages.Identifier;
import io.iot.pulsar.mqtt.messages.custom.VoidMessage;
import io.iot.pulsar.mqtt.messages.will.WillMessage;
import io.iot.pulsar.mqtt.processor.MqttProcessorController;
import io.iot.pulsar.mqtt.utils.CompletableFutures;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

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
    public CompletableFuture<Void> close() {
        return CompletableFutures.from(ctx.channel().close());
    }

    @Override
    public long initTime() {
        return initTime;
    }

    @Override
    public void swallow(@Nonnull MqttMessage mqttMessage) {
        processorController.process(mqttMessage.fixedHeader().messageType(), this, mqttMessage)
                // todo: Improve the writing process. Not all messages need to flush immediately.
                .whenComplete((ack, processError) -> {
                    if (processError != null) {
                        Throwable rc = CompletableFutures.unwrap(processError);
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
                    CompletableFutures.from(ctx.channel().writeAndFlush(ack))
                            .whenComplete((__, ex) -> {
                                if (ex != null) {
                                    log.error("[IOT-MQTT][{}] Failed to send packet [{}] to client.", remoteAddress(),
                                            ack.fixedHeader().messageType(), ex);
                                    return;
                                }
                                if (log.isDebugEnabled()) {
                                    log.debug("[IOT-MQTT][{}] Successfully sent packet [{}] to client.",
                                            remoteAddress(), ack.fixedHeader().messageType());
                                }
                            });
                });
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

    @Builder.Default
    private final long initTime = System.currentTimeMillis();
}
