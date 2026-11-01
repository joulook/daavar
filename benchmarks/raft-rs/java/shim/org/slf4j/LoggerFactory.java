package org.slf4j;

/** The part of SLF4J's {@code LoggerFactory} the vendored core calls; one stateless logger. */
public final class LoggerFactory {

    private static final Logger LOGGER = new Logger() {};

    private LoggerFactory() {}

    public static Logger getLogger(Class<?> c) {
        return LOGGER;
    }

    public static Logger getLogger(String name) {
        return LOGGER;
    }
}
