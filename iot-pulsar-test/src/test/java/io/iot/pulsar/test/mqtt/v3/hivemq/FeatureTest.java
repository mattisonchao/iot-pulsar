package io.iot.pulsar.test.mqtt.v3.hivemq;

import io.iot.pulsar.test.env.IotPulsarBase;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class FeatureTest extends IotPulsarBase {

    @Override
    protected void prepare(@Nonnull ServiceConfiguration serviceConfiguration) {
    }

    @Test
    public void smock() throws PulsarAdminException {
        List<String> list = pulsarAdmin.clusters().getClusters();
        Assert.assertEquals(list.size(), 1);
    }
}
