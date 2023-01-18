/**
 * Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.
 */
package io.iot.pulsar.test.env.container;

import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.iot.pulsar.test.env.PulsarEnv;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class DockerPulsarStandalone implements PulsarEnv {

    private static final String VERSION = "2.10.3";
    private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:" + VERSION);
    private static final String ADMIN_CLUSTERS_ENDPOINT = "/admin/v2/clusters";
    private final ObjectMapper mapper = new ObjectMapper();
    private PulsarContainer pulsar;
    private String brokerURL;
    private String webServiceURL;

    @Nonnull
    @Override
    public String getBrokerUrl() {
        return brokerURL;
    }

    @Nonnull
    @Override
    public String getServiceUrl() {
        return webServiceURL;
    }

    @Override
    public int getMappedPort(int originalPort) {
        return pulsar.getMappedPort(originalPort);
    }

    @Nonnull
    @Override
    public ServiceConfiguration getDefaultConfiguration() {
        return new ServiceConfiguration();
    }

    @SneakyThrows
    @Override
    public void init(@Nonnull ServiceConfiguration serviceConfiguration) {
        // --------- Start pulsar standalone container
        this.pulsar = new PulsarContainer(PULSAR_IMAGE);
        // load authentication
        final String parameters = serviceConfiguration.getBrokerClientAuthenticationParameters();
        if (serviceConfiguration.getBrokerClientAuthenticationPlugin().contains("AuthenticationBasic")) {
            JsonNode node = mapper.readTree(parameters);
            // override wait strategy
            pulsar.setWaitStrategy(new WaitAllStrategy().withStrategy(
                    Wait.forHttp(ADMIN_CLUSTERS_ENDPOINT)
                            .withBasicCredentials(node.get("userId").asText(), node.get("password").asText())
                            .forStatusCode(200).forPort(BROKER_HTTP_PORT)
            ));
        }

        String protocolHandlerDirectory = serviceConfiguration.getProtocolHandlerDirectory();
        pulsar.withFileSystemBind(protocolHandlerDirectory,
                protocolHandlerDirectory, BindMode.READ_ONLY);
        // Set broker configuration
        pulsar.withEnv("PULSAR_PREFIX_" + "protocolHandlerDirectory", protocolHandlerDirectory);
        pulsar.withEnv("PULSAR_PREFIX_" + "messagingProtocols", "iot");
        // Set plugin configuration
        for (Map.Entry<Object, Object> property : serviceConfiguration.getProperties().entrySet()) {
            String key = (String) property.getKey();
            String value = (String) property.getValue();
            pulsar.withEnv("PULSAR_PREFIX_" + key, value);
        }
        // Expose 1883 for MQTT
        pulsar.addExposedPort(1883);
        pulsar.start();
        log.info("====== Start Apache Pulsar success. ======");
        this.brokerURL = pulsar.getPulsarBrokerUrl();
        this.webServiceURL = pulsar.getHttpServiceUrl();
    }

    @Override
    public void cleanup() {
        if (pulsar != null) {
            pulsar.close();
            log.info("====== Close Apache Pulsar success. ======");
        }
    }
}
