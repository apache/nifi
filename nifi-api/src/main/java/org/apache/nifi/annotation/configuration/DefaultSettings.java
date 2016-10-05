package org.apache.nifi.annotation.configuration;

import org.apache.nifi.logging.LogLevel;

import java.lang.annotation.*;

/**
 * <p>
 * Marker interface that a Processor can use to configure the yield duration, the  penalty duration and the bulletin log level.
 * Note that the number of Concurrent tasks will be ignored if the annotion @TriggerSerialy is used
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DefaultSettings {
    String YieldDuration() default "1 sec";
    String PenaltyDuration() default "30 sec";
    LogLevel LogLevel() default LogLevel.WARN;
}
