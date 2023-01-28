package io.iot.pulsar.agent.utils;

import javax.annotation.Nonnull;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;

public class TopicNameUtils {

    public static @Nonnull TopicName convertToPulsarName(@Nonnull String topicName) {
        // If start with full quality pulsar topic name, convert it directly.
        if (topicName.startsWith(TopicDomain.persistent.value())) {
            return TopicName.get(topicName);
        }
        // URL encoded.
        return TopicName.get(Codec.encode(topicName));
    }
}
