package io.iot.pulsar.test.mqtt;

import org.testng.annotations.Test;

public interface FeatureTest {


    //todo add comment
    @Test
    void testSimpleConnect();

    @Test
    void testUnsupportedVersion();

    @Test
    void testCleanSession();

    @Test
    void testUniqueClientIdentifier();

    @Test
    void testQos0(String topicName);
    @Test
    void testQos1(String topicName);
}
