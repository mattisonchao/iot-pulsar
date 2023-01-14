package io.iot.pulsar.common.options;

import com.google.common.base.Strings;
import io.iot.pulsar.common.Protocols;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    /**
     * This configuration can set which protocols are enabled.
     * <p>
     * #{@link Protocols} to see which protocols we support.
     */
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
        final List<Protocols> protocols;
        if (!Strings.isNullOrEmpty(iotProtocols)) {
            protocols = Arrays.stream(iotProtocols.split(","))
                    .map(protocol -> protocol.toUpperCase(Locale.ROOT))
                    .map(Protocols::valueOf)
                    .collect(Collectors.toList());
        } else {
            protocols = Collections.emptyList();
        }
        iotPulsarOptionsBuilder.protocols(protocols);

        if (protocols.contains(Protocols.MQTT)) {
            iotPulsarOptionsBuilder.mqttOptions(IotPulsarMqttOptions.parseFromProperties(properties));
        }
        return iotPulsarOptionsBuilder
                .build();
    }

}
