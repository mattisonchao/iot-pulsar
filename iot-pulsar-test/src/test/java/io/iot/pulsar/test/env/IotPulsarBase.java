package io.iot.pulsar.test.env;

import io.iot.pulsar.test.env.container.DockerPulsarStandalone;
import io.iot.pulsar.test.env.mock.MockedPulsarService;
import java.net.URI;
import java.net.URL;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

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
        assert resource != null;
        String replacePath = resource.getPath().replace("iot-pulsar-handler.nar", "");
        if (StringUtils.containsIgnoreCase(System.getProperty("os.name"), "windows")) {
            replacePath = replacePath.substring(1);
        }
        defaultConfiguration.setProtocolHandlerDirectory(replacePath);

        prepare(defaultConfiguration);
        env.init(defaultConfiguration);
        String brokerUrl = env.getBrokerUrl();
        String serviceUrl = env.getServiceUrl();
        IotPulsarBase.this.brokerHost = URI.create(brokerUrl).getHost();
        IotPulsarBase.this.pulsarAdmin = PulsarAdmin.builder()
                .authentication(defaultConfiguration.getBrokerClientAuthenticationPlugin(),
                        defaultConfiguration.getBrokerClientAuthenticationParameters())
                .serviceHttpUrl(serviceUrl)
                .build();
        IotPulsarBase.this.pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl)
                .authentication(defaultConfiguration.getBrokerClientAuthenticationPlugin(),
                        defaultConfiguration.getBrokerClientAuthenticationParameters())
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

    // --- data providers
    @DataProvider(name = "topicNames")
    public Object[][] topicNames() {
        return new Object[][]{
                {"persistent://public/default/" + UUID.randomUUID()},
                {UUID.randomUUID() + ""},
                {"/a/b/c"},
                {"a/b/c"},
        };
    }
}
