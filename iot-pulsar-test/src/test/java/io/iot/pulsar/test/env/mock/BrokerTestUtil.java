package io.iot.pulsar.test.env.mock;

import java.util.UUID;
import org.mockito.Mockito;

/**
 * Holds util methods used in test.
 */
public class BrokerTestUtil {
    // Generate unique name for different test run.
    public static String newUniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy is stub-only which does not record method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgs(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }

    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy records method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgsRecordingInvocations(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    /**
     * Create a Mockito spy that is stub-only which does not record method invocations,
     * thus saving memory but disallowing verification of invocations.
     *
     * @param object to spy on
     * @return a spy of the real object
     * @param <T> type of object
     */
    public static <T> T spyWithoutRecordingInvocations(T object) {
        return Mockito.mock((Class<T>) object.getClass(), Mockito.withSettings()
                .spiedInstance(object)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }
}
