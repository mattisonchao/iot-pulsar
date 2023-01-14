package io.iot.pulsar.test.mqtt.v3.hivemq;

import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import io.iot.pulsar.test.env.IotPulsarBase;
import io.iot.pulsar.test.mqtt.FeatureTest;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
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
        Assert.assertEquals(connAck.getReturnCode(), Mqtt3ConnAckReturnCode.SUCCESS);
        client.disconnect();
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
