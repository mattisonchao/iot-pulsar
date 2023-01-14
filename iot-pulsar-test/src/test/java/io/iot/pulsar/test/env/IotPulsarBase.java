package io.iot.pulsar.test.env;

import io.iot.pulsar.test.env.container.DockerPulsarStandalone;
import io.iot.pulsar.test.env.mock.MockedPulsarService;
import java.net.URI;
import java.net.URL;
import java.util.Set;
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
    protected String brokerHost;
    protected int mqttPort;

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    protected void setUp() {
        if (container()) {
            IotPulsarBase.this.env = new DockerPulsarStandalone();
        } else {
            IotPulsarBase.this.env = new MockedPulsarService();
        }
        final ServiceConfiguration defaultConfiguration = env.getDefaultConfiguration();
        defaultConfiguration.setMessagingProtocols(Set.of("iot"));
        URL resource = IotPulsarBase.this.getClass().getClassLoader().getResource("iot-pulsar-handler.nar");
        defaultConfiguration.setProtocolHandlerDirectory(resource.getPath().toString()
                .replace("iot-pulsar-handler.nar", ""));
        prepare(defaultConfiguration);
        env.init(defaultConfiguration);
        String brokerUrl = env.getBrokerUrl();
        String serviceUrl = env.getServiceUrl();
        IotPulsarBase.this.brokerHost = URI.create(brokerUrl).getHost();
        IotPulsarBase.this.pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceUrl)
                .build();
        IotPulsarBase.this.pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl)
                .build();
    }

    protected int getMappedPort(int originalPort) {
        return env.getMappedPort(originalPort);
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
