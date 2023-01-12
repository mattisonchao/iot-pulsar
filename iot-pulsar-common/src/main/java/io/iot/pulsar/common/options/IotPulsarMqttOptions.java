package io.iot.pulsar.common.options;

import com.google.common.base.Strings;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
public class IotPulsarMqttOptions {
    private final List<URI> advertisedListeners;
    private final boolean enableAuthentication;

    /**
     * Parse properties to IoT Pulsar mqtt options, this method does not check the logic of the configuration.
     * It just does the parsing.
     *
     * @param properties Configuration properties
     * @return Iot pulsar mqtt options
     * @throws NullPointerException     If some configuration is null
     * @throws IllegalArgumentException If some configuration is illegal
     */
    @Nonnull
    public static IotPulsarMqttOptions parseFromProperties(@Nonnull Properties properties) {
        Objects.requireNonNull(properties, "Argument [properties] can not be null");
        final IotPulsarMqttOptions.IotPulsarMqttOptionsBuilder mqttOptionsBuilder = IotPulsarMqttOptions.builder();

        final String advertisedListeners = properties.getProperty("mqttAdvertisedListeners");
        if (Strings.isNullOrEmpty(advertisedListeners)) {
            throw new IllegalArgumentException("Configuration [mqttAdvertisedListeners] can not be null");
        }
        final List<URI> uris = Arrays.stream(advertisedListeners.split(","))
                .map(advertisedListener -> {
                    try {
                        final URI advertisedListenerUri = new URI(advertisedListener);
                        final String scheme = advertisedListenerUri.getScheme();
                        if (scheme == null
                                || (!scheme.equalsIgnoreCase("mqtt")
                                        && !scheme.equalsIgnoreCase("mqtt+ssl"))) {
                            throw new IllegalArgumentException("[IOT-MQTT] Illegal advertised listeners."
                                    + " example: mqtt://127.0.0.1:1884 or mqtt+ssl://127.0.0.1:8883");
                        }
                        return advertisedListenerUri;
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                })
                .collect(Collectors.toList());
        mqttOptionsBuilder.advertisedListeners(uris);

        boolean authenticationEnabled = Boolean.parseBoolean(
                Optional.ofNullable(properties.getProperty("mqttEnableAuthentication")).orElse("false"));

        return mqttOptionsBuilder
                .enableAuthentication(authenticationEnabled)
                .build();
    }
}
