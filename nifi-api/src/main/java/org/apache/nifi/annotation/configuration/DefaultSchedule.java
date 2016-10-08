package org.apache.nifi.annotation.configuration;

import org.apache.nifi.scheduling.SchedulingStrategy;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Inherited;

/**
 * <p>
 * Marker interface that a Processor can use to configure the schedule strategy, the  period and the number of concurrent tasks.
 * Note that the number of Concurrent tasks will be ignored if the annotion @TriggerSerialy is used
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DefaultSchedule {

    SchedulingStrategy strategy() default  SchedulingStrategy.TIMER_DRIVEN;
    String period() default "0 sec";
    int concurrentTasks() default 1;

}
