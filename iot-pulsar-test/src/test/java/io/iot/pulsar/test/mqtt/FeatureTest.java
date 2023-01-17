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
}
