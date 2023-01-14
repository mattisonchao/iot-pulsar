package io.iot.pulsar.common.utils;

import static java.util.Objects.requireNonNull;
import javax.annotation.Nonnull;

public class LogoUtils {
    public static void printLogo(@Nonnull String version, @Nonnull String protocols) {
        requireNonNull(version, "Argument [version] can not be null");
        requireNonNull(version, "Argument [protocols] can not be null");

        System.out.println();
        System.out.println();
        String logo = String.format(" \n"
                + "  ******       ******     ************                                ***\n"
                + "    **        **    **        ****                                **     **\n"
                + "    **        **    **        ****                 ****************       *****************\n"
                + "    **        **    **        ****                               **         **\n"
                + "    **        **    **        ****                     ********               *************\n"
                + "    **        **    **        ****                 ****                 *****\n"
                + "    **        **    **        ****                           ***********             Version: %s \n"
                + "  ******       ******         ****                 *******                         Protocols: %s \n"
                + "\n", version, protocols);

        System.out.println(logo);
    }
}
