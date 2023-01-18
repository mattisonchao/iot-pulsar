package io.iot.pulsar.mqtt.auth;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;

public class AuthData {
    private static final String TOKEN_KEYWORD = ":token";
    private static final String DEFAULT_METHOD_BASIC = "basic";
    private static final String DEFAULT_METHOD_TOKEN = "token";
    @Getter
    private final String method;
    @Getter
    private final String userName;
    @Getter
    private final byte[] parameters;

    public @Nonnull String getStringParameters() {
        return new String(parameters);
    }

    private AuthData(@Nullable String method, @Nullable String userName, @Nullable byte[] parameters) {
        this.method = method;
        this.userName = userName;
        this.parameters = parameters;
    }

    public static @Nonnull AuthData createByUserName(@Nonnull String userName, @Nonnull byte[] parameters) {
        if (userName.equals(TOKEN_KEYWORD)) {
            return new AuthData(DEFAULT_METHOD_TOKEN, null, parameters);
        }
        return new AuthData(DEFAULT_METHOD_BASIC, userName, parameters);
    }

    public static @Nonnull AuthData createByMethod(@Nonnull String method, @Nonnull byte[] parameters) {
        return new AuthData(method, null, parameters);
    }

    public static @Nonnull AuthData empty() {
        return new AuthData(null, null, null);
    }

    public boolean isEmpty() {
        return Strings.isBlank(method);
    }

    public boolean isBasic() {
        return Objects.equals(method, DEFAULT_METHOD_BASIC);
    }

    public String mergeUserNameAndPassword(char separator) {
        return userName + separator + getStringParameters();
    }
}
