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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.LinkedHashMap;
import java.util.Map;

public class SchedulingDefaultsFactory {

    public static SchedulingDefaults getNifiSchedulingDefaults() {
        final Map<String, Integer> defaultConcurrentTasks = new LinkedHashMap<>(3);
        defaultConcurrentTasks.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks());
        defaultConcurrentTasks.put(SchedulingStrategy.EVENT_DRIVEN.name(), SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks());
        defaultConcurrentTasks.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks());

        final Map<String, String> defaultSchedulingPeriods = new LinkedHashMap<>(2);
        defaultSchedulingPeriods.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod());
        defaultSchedulingPeriods.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod());

        final SchedulingDefaults schedulingDefaults = new SchedulingDefaults();
        schedulingDefaults.setDefaultSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        schedulingDefaults.setDefaultSchedulingPeriodMillis(0);
        schedulingDefaults.setPenalizationPeriodMillis(30000);
        schedulingDefaults.setYieldDurationMillis(1000);
        schedulingDefaults.setDefaultRunDurationNanos(0);
        schedulingDefaults.setDefaultMaxConcurrentTasks("1");
        schedulingDefaults.setDefaultConcurrentTasksBySchedulingStrategy(defaultConcurrentTasks);
        schedulingDefaults.setDefaultSchedulingPeriodsBySchedulingStrategy(defaultSchedulingPeriods);
        return schedulingDefaults;
    }
}
