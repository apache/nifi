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
package org.apache.nifi.scheduling;

/**
 * Defines a Scheduling Strategy to use when scheduling Components (Ports,
 * Funnels, Processors) to run
 */
public enum SchedulingStrategy {

    /**
     * Components should be scheduled to run on a periodic interval that is
     * user-defined with a user-defined number of concurrent tasks. All
     * Components support Timer-Driven mode.
     */
    TIMER_DRIVEN(1, "0 sec"),
    /**
     * Indicates that the component will be scheduled to run according to a
     * Cron-style expression
     */
    CRON_DRIVEN(1, "* * * * * ?");

    private final int defaultConcurrentTasks;
    private final String defaultSchedulingPeriod;

    private SchedulingStrategy(final int defaultConcurrentTasks, final String defaultSchedulingPeriod) {
        this.defaultConcurrentTasks = defaultConcurrentTasks;
        this.defaultSchedulingPeriod = defaultSchedulingPeriod;
    }

    public int getDefaultConcurrentTasks() {
        return defaultConcurrentTasks;
    }

    public String getDefaultSchedulingPeriod() {
        return defaultSchedulingPeriod;
    }
}
