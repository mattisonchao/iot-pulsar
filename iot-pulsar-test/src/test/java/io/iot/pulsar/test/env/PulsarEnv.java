package io.iot.pulsar.test.env;

import javax.annotation.Nonnull;
import org.apache.pulsar.broker.ServiceConfiguration;

public interface PulsarEnv {

    @Nonnull
    String getBrokerUrl();

    @Nonnull
    String getServiceUrl();

    @Nonnull
    ServiceConfiguration getDefaultConfiguration();

    void init(@Nonnull ServiceConfiguration serviceConfiguration);

    void cleanup();
}
