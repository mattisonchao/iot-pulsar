package io.iot.pulsar.test.env;

import io.iot.pulsar.test.env.container.DockerPulsarStandalone;
import io.iot.pulsar.test.env.mock.MockedPulsarService;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class IotPulsarBase {
    private PulsarEnv env;
    protected PulsarAdmin pulsarAdmin;
    protected PulsarClient pulsarClient;

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    protected void setUp() {
        if (container()) {
            this.env = new DockerPulsarStandalone();
        } else {
            this.env = new MockedPulsarService();
        }
        final ServiceConfiguration defaultConfiguration = env.getDefaultConfiguration();
        prepare(defaultConfiguration);
        env.init(defaultConfiguration);
        String brokerUrl = env.getBrokerUrl();
        String serviceUrl = env.getServiceUrl();
        this.pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceUrl)
                .build();
        this.pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl)
                .build();
    }

    protected boolean container() {
        return false;
    }

    protected abstract void prepare(@Nonnull ServiceConfiguration serviceConfiguration);

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    protected void cleanUp() {
        if (env != null) {
            env.cleanup();
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }
}
