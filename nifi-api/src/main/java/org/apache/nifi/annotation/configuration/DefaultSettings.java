package org.apache.nifi.annotation.configuration;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Inherited;
import org.apache.nifi.logging.LogLevel;

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
