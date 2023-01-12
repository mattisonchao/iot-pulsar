package io.iot.pulsar.common.options;

import io.iot.pulsar.common.Protocols;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
@Builder
public class IotPulsarOptions {
    private final List<Protocols> protocols;
    private final IotPulsarMqttOptions mqttOptions;
    private final IotPulsarCoapOptions coapOptions;
    private final IotPulsarLwm2mOptions lwm2mOptions;

    /**
     * Parse properties to IoT Pulsar options, this method does not check the logic of the configuration.
     * It just does the parsing.
     *
     * @param properties Configuration properties
     * @return Iot pulsar options
     * @throws NullPointerException     If some configuration is null
     * @throws IllegalArgumentException If some configuration is illegal
     */
    @Nonnull
    public static IotPulsarOptions parseFromProperties(@Nonnull Properties properties) {
        Objects.requireNonNull(properties, "Argument [properties] can not be null");
        final IotPulsarOptionsBuilder iotPulsarOptionsBuilder = IotPulsarOptions.builder();

        final String iotProtocols = properties.getProperty("iotProtocols");
        final List<Protocols> protocols = Arrays.stream(iotProtocols.split(","))
                .map(Protocols::valueOf)
                .collect(Collectors.toList());
        iotPulsarOptionsBuilder.protocols(protocols);

        if (protocols.contains(Protocols.MQTT)) {
            iotPulsarOptionsBuilder.mqttOptions(IotPulsarMqttOptions.parseFromProperties(properties));
        }
        return iotPulsarOptionsBuilder
                .build();
    }

}
