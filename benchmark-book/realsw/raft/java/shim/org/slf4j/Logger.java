package org.slf4j;

/**
 * The part of the SLF4J 1.7 {@code Logger} interface that the vendored MicroRaft core calls
 * ({@code debug}, {@code info}, {@code warn}, {@code error}, {@code isDebugEnabled}), written for
 * this benchmark: the JMC classpath the nodes compile against has no SLF4J, and the core's only
 * external runtime dependency is this logging facade. Not a copy of SLF4J; see
 * {@code source/PROVENANCE.md}. Debug, info and warn are dropped; error lines go to stderr, so
 * that a failure the core catches and logs is still seen in a run's output.
 */
public interface Logger {

    default boolean isDebugEnabled() {
        return false;
    }

    default void debug(String msg) {}

    default void debug(String format, Object arg) {}

    default void debug(String format, Object arg1, Object arg2) {}

    default void debug(String format, Object... args) {}

    default void debug(String msg, Throwable t) {}

    default void info(String msg) {}

    default void info(String format, Object arg) {}

    default void info(String format, Object arg1, Object arg2) {}

    default void info(String format, Object... args) {}

    default void info(String msg, Throwable t) {}

    default void warn(String msg) {}

    default void warn(String format, Object arg) {}

    default void warn(String format, Object arg1, Object arg2) {}

    default void warn(String format, Object... args) {}

    default void warn(String msg, Throwable t) {}

    default void error(String msg) {
        System.err.println("microraft ERROR " + msg);
    }

    default void error(String format, Object arg) {
        System.err.println("microraft ERROR " + format + " " + arg);
    }

    default void error(String format, Object arg1, Object arg2) {
        System.err.println("microraft ERROR " + format + " " + arg1 + " " + arg2);
    }

    default void error(String format, Object... args) {
        System.err.println("microraft ERROR " + format + " " + java.util.Arrays.toString(args));
    }

    default void error(String msg, Throwable t) {
        System.err.println("microraft ERROR " + msg + " " + t);
    }
}
