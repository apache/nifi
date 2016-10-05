package org.apache.nifi.annotation.configuration;

import org.apache.nifi.scheduling.SchedulingStrategy;

import java.lang.annotation.*;

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

    SchedulingStrategy Strategy() default  SchedulingStrategy.TIMER_DRIVEN;
    String Period() default "0 sec";
    int ConcurrentTasks() default 1;

}
