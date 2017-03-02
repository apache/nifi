/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Marker interface that a Processor can use to configure default settings for the schedule strategy, the period and the number of concurrent tasks.
 * Marker interface that a ReportingTask can use to configure default settings the  schedule strategy and the period.
 * Note that the number of Concurrent tasks will be ignored if the annotation @TriggerSerialy is used
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