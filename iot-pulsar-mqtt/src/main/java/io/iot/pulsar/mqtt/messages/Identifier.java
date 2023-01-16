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
    public static Identifier create(@Nullable String identifier, boolean assigned) {
        if (identifier == null) {
            return empty();
        }
        return new Identifier(identifier, assigned);
    }

    @Nonnull
    public static Identifier empty() {
        return new Identifier(null, false);
    }

    public boolean isEmpty() {
        return identifier == null;
    }
}
