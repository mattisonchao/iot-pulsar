package io.iot.pulsar.test.mqtt.v3.hivemq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.exceptions.Mqtt3ConnAckException;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import io.iot.pulsar.test.env.IotPulsarBase;
import io.iot.pulsar.test.mqtt.AuthTest;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class V3HivemqAuthTest extends IotPulsarBase implements AuthTest {
    @Test
    @Override
    public void testBasicAuthentication() {
        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        try {
            client1.connectWith()
                    .cleanSession(true)
                    .send();
            fail("expect NOT_AUTHORIZED exception!");
        } catch (Mqtt3ConnAckException ex) {
            assertEquals(ex.getMqttMessage().getReturnCode(), Mqtt3ConnAckReturnCode.NOT_AUTHORIZED);
        }
        //-----
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3ConnAck ack = client2.connectWith()
                .simpleAuth()
                .username("superuser")
                .password("admin".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .cleanSession(true)
                .send();
        assertEquals(ack.getReturnCode(), Mqtt3ConnAckReturnCode.SUCCESS);
        client2.disconnect();
    }

    @Test
    @Override
    public void testTokenAuthentication() {
        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        try {
            client1.connectWith()
                    .cleanSession(true)
                    .send();
            fail("expect NOT_AUTHORIZED exception!");
        } catch (Mqtt3ConnAckException ex) {
            assertEquals(ex.getMqttMessage().getReturnCode(), Mqtt3ConnAckReturnCode.NOT_AUTHORIZED);
        }
        //-----
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3ConnAck ack = client2.connectWith()
                .simpleAuth()
                .username(":token")
                .password("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.EN0bokGp8EMz00EdKg2c3f1wD-27o2EQh99NaCIxy7k".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .cleanSession(true)
                .send();
        assertEquals(ack.getReturnCode(), Mqtt3ConnAckReturnCode.SUCCESS);
        client2.disconnect();
    }

    @Override
    protected void prepare(@Nonnull ServiceConfiguration serviceConfiguration) {
        serviceConfiguration.setAuthenticationEnabled(true);
        serviceConfiguration.setAuthenticationProviders(
                Set.of("org.apache.pulsar.broker.authentication.AuthenticationProviderBasic",
                        "org.apache.pulsar.broker.authentication.AuthenticationProviderToken"));
        serviceConfiguration.setBrokerClientAuthenticationPlugin(
                "org.apache.pulsar.client.impl.auth.AuthenticationBasic");
        serviceConfiguration.setBrokerClientAuthenticationParameters(
                "{\"userId\":\"superuser\",\"password\":\"admin\"}");
        final Properties properties = serviceConfiguration.getProperties();
        properties.put("basicAuthConf", "c3VwZXJ1c2VyOiRhcHIxJGgubkl5bGQzJFhGMVNpaWhuRXdIcjBCdXA0S2F2SS4K");

        // duplicate for container
        properties.put("authenticationEnabled", "true");
        properties.put("authenticationProviders",
                "org.apache.pulsar.broker.authentication.AuthenticationProviderBasic," +
                        "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
        properties.put("brokerClientAuthenticationPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationBasic");
        properties.put("brokerClientAuthenticationParameters", "{\"userId\":\"superuser\",\"password\":\"admin\"}");
        // config token
        properties.put("tokenSecretKey", "data:;base64,Rp16YOnd2TdTPYGByHI98DrFBkIe5u8DBBjAa9zcb9k=");
        // enable mqtt
        properties.put("iotProtocols", "mqtt");
        properties.put("mqttAdvertisedListeners", "mqtt://0.0.0.0:1883");
        properties.put("mqttEnableAuthentication", "true");
    }

    @Test
    public static final class MockTest extends V3HivemqAuthTest {
        @Override
        protected boolean container() {
            return false;
        }
    }

    @Test
    public static final class ContainerTest extends V3HivemqAuthTest {
        @Override
        protected boolean container() {
            return true;
        }
    }
}
