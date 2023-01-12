package io.iot.pulsar.mqtt;

import com.google.common.base.Strings;
import io.iot.pulsar.mqtt.messages.code.MqttConnReturnCode;
import io.iot.pulsar.mqtt.vertex.MqttVertex;
import io.iot.pulsar.mqtt.vertex.MqttVertexImpl;
import io.iot.pulsar.mqtt.vertex.MqttVertexProperties;
import io.iot.pulsar.mqtt.vertex.RejectOnlyMqttVertex;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
public class MqttInboundHandler extends ChannelInboundHandlerAdapter {
    private final Mqtt mqtt;
    private MqttVertex mqttVertex;
    private ChannelHandlerContext ctx;

    public MqttInboundHandler(@Nonnull Mqtt mqtt) {
        this.mqtt = mqtt;
    }

    @Override
    public void channelRegistered(@Nonnull ChannelHandlerContext ctx) throws Exception {
        MqttInboundHandler.this.ctx = ctx;
        super.channelRegistered(ctx);
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {
        if (!(msg instanceof MqttMessage)) {
            ctx.fireExceptionCaught(new UnsupportedMessageTypeException(msg, MqttMessage.class));
            return;
        }
        // --- Check codec
        final DecoderResult result = ((MqttMessage) msg).decoderResult();
        if (result.isFailure()) {
            MqttInboundHandler.this.mqttVertex = new RejectOnlyMqttVertex(ctx.channel());
            mqttVertex.commandTraceLog(
                    mqttVertex.reject(MqttConnReturnCode.PROTOCOL_ERROR, MqttProperties.NO_PROPERTIES),
                    String.format("REJECT(%s)", MqttConnReturnCode.PROTOCOL_ERROR));
            return;
        }
        MqttFixedHeader fixed = ((MqttMessage) msg).fixedHeader();
        switch (fixed.messageType()) {
            case CONNECT:
                MqttConnectMessage connectMessage = (MqttConnectMessage) msg;
                prepareVertex(connectMessage);
                mqtt.getConnectHandler().handle(mqttVertex, connectMessage);
                break;
            case SUBSCRIBE:
                break;
            case UNSUBSCRIBE:
                break;
            case PUBLISH:
                break;
            case PUBACK:
                break;
            case PUBREC:
                break;
            case PUBREL:
                break;
            case PUBCOMP:
                break;
            case PINGREQ:
                break;
            case DISCONNECT:
                break;
            default:
                ctx.fireExceptionCaught(
                        new UnsupportedMessageTypeException(fixed.messageType(), MqttMessageType.class));
        }
    }

    private void prepareVertex(@Nonnull MqttConnectMessage connectMessage) {
        if (this.mqttVertex != null) {
            mqttVertex.commandTraceLog(mqttVertex.close(), "CLOSE(duplicated connect)");
            return;
        }
        final MqttConnectVariableHeader var = connectMessage.variableHeader();
        final MqttConnectPayload payload = connectMessage.payload();
        final boolean assignedIdentifier = Strings.isNullOrEmpty(payload.clientIdentifier());
        final String identifier;
        if (assignedIdentifier) {
            identifier = UUID.randomUUID().toString();
        } else {
            identifier = payload.clientIdentifier();
        }
        // preparing vertex
        final MqttVertexProperties properties = MqttVertexProperties
                .builder()
                .cleanSession(var.isCleanSession())
                .build();
        MqttInboundHandler.this.mqttVertex = MqttVertexImpl.builder()
                .identifier(identifier)
                .channel(ctx.channel())
                .version(MqttVersion.fromProtocolNameAndLevel(var.name(), (byte) var.version()))
                .properties(properties)
                .build();
        // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
        // In cases where the ClientID is assigned by the Server, return the assigned ClientID.
        // This also lifts the restriction that Server assigned ClientIDs can only be used with Clean Session=1.
        if (!mqttVertex.properties().isCleanSession()
                && assignedIdentifier) {
            mqttVertex.commandTraceLog(
                    mqttVertex.reject(MqttConnReturnCode.INVALID_CLIENT_IDENTIFIER, MqttProperties.NO_PROPERTIES),
                    String.format("REJECT(%s)", MqttConnReturnCode.INVALID_CLIENT_IDENTIFIER));
            return;
        }
        // config keep alive
        ctx.pipeline().remove(MqttChannelInitializer.CONNECT_IDLE_NAME);
        ctx.pipeline().remove(MqttChannelInitializer.CONNECT_TIMEOUT_NAME);

        int keepAlive = var.keepAliveTimeSeconds();
        if (keepAlive > 0) {
            ctx.pipeline().addLast("keepAliveIdle",
                    new IdleStateHandler(keepAlive, 0, 0));
            ctx.pipeline().addLast("keepAliveHandler", new ChannelDuplexHandler() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                    if (event instanceof IdleStateEvent
                            && ((IdleStateEvent) event).state() == IdleState.READER_IDLE) {
                        mqttVertex.commandTraceLog(mqttVertex.close(), "CLOSE(keepalive)");
                    }
                }
            });
        }
    }
}
