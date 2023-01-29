package io.iot.pulsar.test.mqtt.v3.hivemq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAckReturnCode;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.iot.pulsar.test.env.IotPulsarBase;
import io.iot.pulsar.test.mqtt.FeatureTest;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.awaitility.Awaitility;
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

    @Test(dataProvider = "topicNames")
    @Override
    @SneakyThrows
    public void testQos0(String topicName) {
        int messageNum = 100;
        final TopicName tp;
        if (topicName.startsWith(TopicDomain.persistent.value())) {
            tp = TopicName.get(topicName);
        } else {
            tp = TopicName.get(Codec.encode(topicName));
        }
        String identifier = UUID.randomUUID().toString();

        // ^^ preparing
        // using mqtt client produce and pulsar client consume
        Set<String> receivedMessage = new HashSet<>();

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(tp.toString())
                .startMessageId(MessageId.latest)
                .subscriptionName("qos-0-reader")
                .create();

        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(identifier)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client1.connect();
        // Send messages to topic
        for (int i = 0; i < messageNum; i++) {
            client1.publishWith()
                    .topic(topicName)
                    .qos(MqttQos.AT_MOST_ONCE)
                    .payload((i + "").getBytes(StandardCharsets.UTF_8))
                    .send();
        }
        for (int i = 0; i < messageNum; i++) {
            Message<byte[]> message = reader.readNext(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            String payload = new String(message.getValue());
            assertEquals(payload, i + "");
            // ensure not duplicated
            assertFalse(receivedMessage.contains(payload));
            receivedMessage.add(payload);
        }
        // no more message
        assertNull(reader.readNext(2, TimeUnit.SECONDS));

        // verify acknowledgement
        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        String lastConfirmedEntry = internalStats.lastConfirmedEntry;
        ManagedLedgerInternalStats.CursorStats cursorStats = internalStats.cursors.get("qos-0-reader");
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // cleanup the resource
        reader.close();
        client1.disconnect();
        // waiting for producer GC
        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    TopicStats stats = pulsarAdmin.topics().getStats(tp.toString());
                    assertEquals(stats.getPublishers().size(), 0);
                });
        pulsarAdmin.topics().delete(tp.toString());

        // ======= section ===============================================
        // using pulsar client produce and mqtt client consume
        receivedMessage = new HashSet<>();
        String identifier2 = UUID.randomUUID().toString();
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(tp.toString())
                .create();
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(identifier2)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client2.connect();
        Mqtt3SubAck subAck = client2.subscribeWith()
                .topicFilter(topicName)
                .qos(MqttQos.AT_MOST_ONCE)
                .send();
        Assert.assertEquals(subAck.getReturnCodes(), List.of(Mqtt3SubAckReturnCode.SUCCESS_MAXIMUM_QOS_0));
        Mqtt3BlockingClient.Mqtt3Publishes publishes =
                client2.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);
        for (int i = 0; i < messageNum; i++) {
            // pulsar client default is at-least-once
            producer2.send((i + "").getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < messageNum; i++) {
            Optional<Mqtt3Publish> publishOptional = publishes.receive(2, TimeUnit.SECONDS);
            // We need to make sure we don't lose anything.
            if (publishOptional.isEmpty()) {
                fail("Unexpected message number");
            }
            // verify the message order
            Mqtt3Publish publishPacket = publishOptional.get();
            String payload = new String(publishPacket.getPayloadAsBytes());
            assertEquals(payload, i + "");
            // ensure not duplicated
            assertFalse(receivedMessage.contains(payload));
            receivedMessage.add(payload);
        }
        // no more message
        assertTrue(publishes.receive(2, TimeUnit.SECONDS).isEmpty());
        // verify acknowledgement
        internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        lastConfirmedEntry = internalStats.lastConfirmedEntry;
        cursorStats = internalStats.cursors.get(identifier2);
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // cleanup the resource
        producer2.close();
        // unsubscribe consumer
        client2.unsubscribeWith().topicFilter(topicName).send();
        client2.disconnect();
        // check subscription
        TopicStats stats = pulsarAdmin.topics().getStats(tp.toString());
        assertEquals(stats.getSubscriptions().size(), 0);
        pulsarAdmin.topics().delete(tp.toString());

        // ======= section ===============================================
        // using mqtt client produce and mqtt client consume
        String identifier3 = UUID.randomUUID().toString();
        String identifier4 = UUID.randomUUID().toString();
        Mqtt3BlockingClient client3 = Mqtt3Client.builder()
                .identifier(identifier3)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3BlockingClient client4 = Mqtt3Client.builder()
                .identifier(identifier4)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client3.connect();
        client4.connect();
        subAck = client3.subscribeWith()
                .topicFilter(topicName)
                .qos(MqttQos.AT_MOST_ONCE)
                .send();
        Assert.assertEquals(subAck.getReturnCodes(), List.of(Mqtt3SubAckReturnCode.SUCCESS_MAXIMUM_QOS_0));
        publishes = client3.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);
        // Send messages to topic
        for (int i = 0; i < messageNum; i++) {
            client4.publishWith()
                    .topic(topicName)
                    .qos(MqttQos.AT_MOST_ONCE)
                    .payload((i + "").getBytes(StandardCharsets.UTF_8))
                    .send();
        }
        for (int i = 0; i < messageNum; i++) {
            Optional<Mqtt3Publish> publishOptional = publishes.receive(2, TimeUnit.SECONDS);
            // We need to make sure we don't lose anything.
            if (publishOptional.isEmpty()) {
                fail("Unexpected message number");
            }
            // verify the message order
            Mqtt3Publish publishPacket = publishOptional.get();
            assertEquals(new String(publishPacket.getPayloadAsBytes()), i + "");
        }
        // no more message
        assertTrue(publishes.receive(2, TimeUnit.SECONDS).isEmpty());
        // verify acknowledgement
        internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        lastConfirmedEntry = internalStats.lastConfirmedEntry;
        cursorStats = internalStats.cursors.get(identifier3);
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // unsubscribe consumer
        client3.unsubscribeWith().topicFilter(topicName).send();
        client3.disconnect();
        // check subscription
        stats = pulsarAdmin.topics().getStats(tp.toString());
        assertEquals(stats.getSubscriptions().size(), 0);
        client4.disconnect();
        // waiting for producer GC
        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertEquals(pulsarAdmin.topics().getStats(tp.toString()).getPublishers().size(), 0));
        pulsarAdmin.topics().delete(tp.toString());
    }

    @Test(dataProvider = "topicNames")
    @Override
    @SneakyThrows
    public void testQos1(String topicName) {
        int messageNum = 100;
        final TopicName tp;
        if (topicName.startsWith(TopicDomain.persistent.value())) {
            tp = TopicName.get(topicName);
        } else {
            tp = TopicName.get(Codec.encode(topicName));
        }
        String identifier = UUID.randomUUID().toString();
        // ^^ preparing
        // using mqtt client produce and pulsar client consume
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(tp.toString())
                .subscriptionName("qos-1")
                .subscribe();

        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(identifier)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client1.connect();
        // Send messages to topic
        for (int i = 0; i < messageNum; i++) {
            client1.publishWith()
                    .topic(topicName)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload((i + "").getBytes(StandardCharsets.UTF_8))
                    .send();
        }
        for (int i = 0; i < messageNum; i++) {
            Message<byte[]> message = consumer1.receive(2, TimeUnit.SECONDS);
            // We need to make sure we don't lose anything.
            if (message == null) {
                fail("Unexpected message number");
            }
            // verify the message order
            assertEquals(new String(message.getValue()), i + "");
            consumer1.acknowledge(message);
        }
        // no more message
        assertNull(consumer1.receive(2, TimeUnit.SECONDS));
        // verify acknowledgement
        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        String lastConfirmedEntry = internalStats.lastConfirmedEntry;
        ManagedLedgerInternalStats.CursorStats cursorStats = internalStats.cursors.get("qos-1");
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // cleanup the resource
        consumer1.close();
        client1.disconnect();
        // waiting for producer GC
        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    TopicStats stats = pulsarAdmin.topics().getStats(tp.toString());
                    assertEquals(stats.getPublishers().size(), 0);
                });
        pulsarAdmin.topics().delete(tp.toString());

        // ======= section ===============================================
        // using pulsar client produce and mqtt client consume
        String identifier2 = UUID.randomUUID().toString();
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(tp.toString())
                .create();
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(identifier2)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client2.connect();
        Mqtt3SubAck subAck = client2.subscribeWith()
                .topicFilter(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Assert.assertEquals(subAck.getReturnCodes(), List.of(Mqtt3SubAckReturnCode.SUCCESS_MAXIMUM_QOS_1));
        Mqtt3BlockingClient.Mqtt3Publishes publishes =
                client2.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);
        for (int i = 0; i < messageNum; i++) {
            // pulsar client default is at-least-once
            producer2.send((i + "").getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < messageNum; i++) {
            Optional<Mqtt3Publish> publishOptional = publishes.receive(2, TimeUnit.SECONDS);
            // We need to make sure we don't lose anything.
            if (publishOptional.isEmpty()) {
                fail("Unexpected message number");
            }
            // verify the message order
            Mqtt3Publish publishPacket = publishOptional.get();
            assertEquals(new String(publishPacket.getPayloadAsBytes()), i + "");
            publishPacket.acknowledge();
        }
        // no more message
        assertTrue(publishes.receive(2, TimeUnit.SECONDS).isEmpty());
        // verify acknowledgement
        internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        lastConfirmedEntry = internalStats.lastConfirmedEntry;
        cursorStats = internalStats.cursors.get(identifier2);
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // cleanup the resource
        producer2.close();
        // unsubscribe consumer
        client2.unsubscribeWith().topicFilter(topicName).send();
        client2.disconnect();
        // check subscription
        TopicStats stats = pulsarAdmin.topics().getStats(tp.toString());
        assertEquals(stats.getSubscriptions().size(), 0);
        pulsarAdmin.topics().delete(tp.toString());

        // ======= section ===============================================
        // using mqtt client produce and mqtt client consume
        String identifier3 = UUID.randomUUID().toString();
        String identifier4 = UUID.randomUUID().toString();
        Mqtt3BlockingClient client3 = Mqtt3Client.builder()
                .identifier(identifier3)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        Mqtt3BlockingClient client4 = Mqtt3Client.builder()
                .identifier(identifier4)
                .serverHost(brokerHost)
                .serverPort(getMappedPort(1883))
                .buildBlocking();
        client3.connect();
        client4.connect();
        subAck = client3.subscribeWith()
                .topicFilter(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Assert.assertEquals(subAck.getReturnCodes(), List.of(Mqtt3SubAckReturnCode.SUCCESS_MAXIMUM_QOS_1));
        publishes = client3.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);
        // Send messages to topic
        for (int i = 0; i < messageNum; i++) {
            client4.publishWith()
                    .topic(topicName)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload((i + "").getBytes(StandardCharsets.UTF_8))
                    .send();
        }
        for (int i = 0; i < messageNum; i++) {
            Optional<Mqtt3Publish> publishOptional = publishes.receive(2, TimeUnit.SECONDS);
            // We need to make sure we don't lose anything.
            if (publishOptional.isEmpty()) {
                fail("Unexpected message number");
            }
            // verify the message order
            Mqtt3Publish publishPacket = publishOptional.get();
            assertEquals(new String(publishPacket.getPayloadAsBytes()), i + "");
            publishPacket.acknowledge();
        }
        // no more message
        assertTrue(publishes.receive(2, TimeUnit.SECONDS).isEmpty());
        // verify acknowledgement
        internalStats = pulsarAdmin.topics().getInternalStats(tp.toString());
        lastConfirmedEntry = internalStats.lastConfirmedEntry;
        cursorStats = internalStats.cursors.get(identifier3);
        assertEquals(cursorStats.markDeletePosition, lastConfirmedEntry);
        // unsubscribe consumer
        client3.unsubscribeWith().topicFilter(topicName).send();
        client3.disconnect();
        // check subscription
        stats = pulsarAdmin.topics().getStats(tp.toString());
        assertEquals(stats.getSubscriptions().size(), 0);
        client4.disconnect();
        // waiting for producer GC
        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertEquals(pulsarAdmin.topics().getStats(tp.toString()).getPublishers().size(), 0));
        pulsarAdmin.topics().delete(tp.toString());
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
