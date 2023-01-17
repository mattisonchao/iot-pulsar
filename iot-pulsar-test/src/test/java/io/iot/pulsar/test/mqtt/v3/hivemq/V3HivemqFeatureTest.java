package io.iot.pulsar.test.mqtt.v3.hivemq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.iot.pulsar.test.env.IotPulsarBase;
import io.iot.pulsar.test.mqtt.FeatureTest;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public abstract class V3HivemqFeatureTest extends IotPulsarBase implements FeatureTest {

    @Override
    protected void prepare(@Nonnull ServiceConfiguration serviceConfiguration) {
        Properties properties = serviceConfiguration.getProperties();
        properties.put("iotProtocols", "mqtt");
        properties.put("mqttAdvertisedListeners", "mqtt://0.0.0.0:1883");
    }


    @Test
    @Override
    public void testSimpleConnect() {
        Mqtt3BlockingClient client = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3ConnAck connAck = client.connectWith()
                .cleanSession(true)
                .send();
        assertEquals(connAck.getReturnCode(), Mqtt3ConnAckReturnCode.SUCCESS);
        client.disconnect();
    }

    @Test
    @Override
    public void testUnsupportedVersion() {
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        try {
            client.connect();
            // cleanup resources
            client.disconnect();
            fail("Expect unsupported protocol version exception");
        } catch (Mqtt5ConnAckException exception) {
            assertEquals(exception.getMqttMessage().getReasonCode(),
                    Mqtt5ConnAckReasonCode.UNSUPPORTED_PROTOCOL_VERSION);
        }
    }

    @Test
    @Override
    public void testUniqueClientIdentifier() {
        String identifier = UUID.randomUUID().toString();
        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(identifier)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(identifier)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client1.connect();
        assertTrue(client1.getState().isConnected());
        client2.connect();
        Awaitility.await().untilAsserted(() -> assertFalse(client1.getState().isConnected()));
        assertTrue(client2.getState().isConnected());
        Mqtt3BlockingClient client3 = Mqtt3Client.builder()
                .identifier(identifier)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client3.connect();
        Awaitility.await().untilAsserted(() -> assertFalse(client1.getState().isConnected()));
        Awaitility.await().untilAsserted(() -> assertFalse(client2.getState().isConnected()));
        assertTrue(client3.getState().isConnected());
        client3.disconnect();
    }

    @Test
    @Override
    public void testCleanSession() {

    }

    @Test
    public static final class MockFeatureTest extends V3HivemqFeatureTest {
        @Override
        protected boolean container() {
            return false;
        }
    }

    @Test
    public static final class ContainerFeatureTest extends V3HivemqFeatureTest {
        @Override
        protected boolean container() {
            return true;
        }
    }

}
