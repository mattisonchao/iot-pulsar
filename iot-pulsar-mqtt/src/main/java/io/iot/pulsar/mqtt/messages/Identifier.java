package io.iot.pulsar.mqtt.messages;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class Identifier {
    private final String identifier;
    private final boolean assigned;


    private Identifier(@Nullable String identifier, boolean assigned) {
        this.identifier = identifier;
        this.assigned = assigned;
    }

    @Nonnull
    public static Identifier create(@Nonnull String identifier, boolean assigned) {
        return new Identifier(identifier, assigned);
    }
}
