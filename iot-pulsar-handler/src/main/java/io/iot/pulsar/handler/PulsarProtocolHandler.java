package io.iot.pulsar.handler;

import com.google.common.collect.ImmutableMap;
import io.iot.pulsar.agent.PulsarAgent;
import io.iot.pulsar.agent.PulsarClientAgentImpl;
import io.iot.pulsar.common.Protocols;
import io.iot.pulsar.common.options.IotPulsarMqttOptions;
import io.iot.pulsar.common.options.IotPulsarOptions;
import io.iot.pulsar.common.utils.LogoUtils;
import io.iot.pulsar.mqtt.Mqtt;
import io.iot.pulsar.mqtt.MqttChannelInitializer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;

@Slf4j
public class PulsarProtocolHandler implements ProtocolHandler {
    private IotPulsarOptions iotPulsarOptions;
    private PulsarAgent pulsarAgent;
    private Mqtt mqtt;

    @Override
    @Nonnull
    public String protocolName() {
        return "iot";
    }

    @Override
    public boolean accept(@Nonnull String protocol) {
        if (protocol.equals("iot")) {
            return true;
        }
        return Arrays.stream(Protocols.values()).anyMatch(p -> p.name().equalsIgnoreCase(protocol));
    }

    @Override
    public void initialize(@Nonnull ServiceConfiguration conf) {
        PulsarProtocolHandler.this.iotPulsarOptions = IotPulsarOptions.parseFromProperties(conf.getProperties());
    }

    @Override
    @Nonnull
    public String getProtocolDataToAdvertise() {
        final StringBuilder advertiseBuilder = new StringBuilder();
        for (Protocols protocol : iotPulsarOptions.getProtocols()) {
            switch (protocol) {
                case MQTT:
                    if (advertiseBuilder.length() != 0) {
                        advertiseBuilder.append(",");
                    }
                    advertiseBuilder.append(iotPulsarOptions.getMqttOptions().getAdvertisedListeners());
                    break;
            }
        }
        return advertiseBuilder.toString();
    }

    @SneakyThrows
    @Override
    public void start(@Nonnull BrokerService service) {
        List<Protocols> protocols = iotPulsarOptions.getProtocols();
        if (!protocols.isEmpty()) {
            PulsarProtocolHandler.this.pulsarAgent = new PulsarClientAgentImpl(service);
            for (Protocols protocol : protocols) {
                if (Objects.requireNonNull(protocol) == Protocols.MQTT) {
                    PulsarProtocolHandler.this.mqtt =
                            Mqtt.create(pulsarAgent, iotPulsarOptions.getMqttOptions());
                } else {
                    log.warn("[IOT] Can't load unsupported protocol {}", protocol);
                }
            }
        }
        String protocolsStr = protocols.stream().map(Enum::name).collect(Collectors.joining(","));
        LogoUtils.printLogo("2.10.3.1-SNAPSHOT", protocolsStr);
        log.info("[IOT] Iot Pulsar is started. loaded protocols [{}] ", protocolsStr);
    }

    @Override
    @Nonnull
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> initializerBuilder =
                ImmutableMap.builder();
        if (mqtt != null) {
            IotPulsarMqttOptions options = mqtt.getOptions();
            for (URI advertisedListener : options.getAdvertisedListeners()) {
                initializerBuilder.put(
                        new InetSocketAddress(advertisedListener.getHost(), advertisedListener.getPort()),
                        new MqttChannelInitializer(mqtt.createInboundHandler(),
                                advertisedListener.getScheme().equalsIgnoreCase("mqtt+ssl")));
            }
        }
        return initializerBuilder.build();
    }

    @Override
    public void close() {
        if (mqtt != null) {
            mqtt.close();
        }
        if (pulsarAgent != null) {
            pulsarAgent.close();
        }
    }
}
