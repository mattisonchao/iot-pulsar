package io.iot.pulsar.test.mqtt;

import org.testng.annotations.Test;

public interface AuthTest {

    @Test
    void testBasicAuthentication();

    @Test
    void testTokenAuthentication();
}
