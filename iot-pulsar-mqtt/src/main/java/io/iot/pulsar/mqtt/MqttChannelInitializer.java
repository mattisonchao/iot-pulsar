package io.iot.pulsar.mqtt;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    public static final String CONNECT_IDLE_NAME = "connectIdle";
    public static final String CONNECT_TIMEOUT_NAME = "connectTimeout";
    private final Supplier<MqttInboundHandler> newInboundHandlerSupplier;
    private final boolean ssl;

    public MqttChannelInitializer(@Nonnull Supplier<MqttInboundHandler> newInboundHandlerSupplier, boolean ssl) {
        this.newInboundHandlerSupplier = newInboundHandlerSupplier;
        this.ssl = ssl;
    }

    @Override
    protected void initChannel(@Nonnull SocketChannel socketChannel) {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast(CONNECT_IDLE_NAME, new IdleStateHandler(90, 0, 0));
        pipeline.addLast(CONNECT_TIMEOUT_NAME, new ChannelDuplexHandler() {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
                if (event instanceof IdleStateEvent) {
                    IdleStateEvent e = (IdleStateEvent) event;
                    if (e.state() == IdleState.READER_IDLE) {
                        ctx.channel().close();
                    }
                }
                super.userEventTriggered(ctx, event);
            }
        });
        pipeline.addLast("mqtt", newInboundHandlerSupplier.get());
    }
}
