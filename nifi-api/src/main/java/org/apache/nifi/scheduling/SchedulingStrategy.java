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
     * Components should be scheduled to run whenever a relevant Event occurs.
     * Examples of "relevant Events" are:
     *
     * <ul>
     * <li>A FlowFile is added to one of the Component's incoming
     * Connections</li>
     * <li>A FlowFile is removed from one of the Component's outgoing
     * Connections</li>
     * <li>The Component is scheduled to run (started)</li>
     * </ul>
     *
     * <p>
     * When using this mode, the user will be unable to configure the scheduling
     * period. Instead, the framework will manage this.
     * </p>
     *
     * <p>
     * When using this mode, the maximum number of concurrent tasks can be set
     * to 0, indicating no maximum.
     * </p>
     *
     * <p>
     * Not all Components support Event-Driven mode.
     * </p>
     */
    EVENT_DRIVEN(0, null),
    /**
     * Components should be scheduled to run on a periodic interval that is
     * user-defined with a user-defined number of concurrent tasks. All
     * Components support Timer-Driven mode.
     */
    TIMER_DRIVEN(1, "0 sec"),
    /**
     * NOTE: This option has been deprecated with the addition of the
     * execution-node combo box.  It still exists for backward compatibility
     * with existing flows that still have this value for schedulingStrategy.
     **
     * Indicates that the component will be scheduled via timer only on the
     * Primary Node. If the instance is not part of a cluster and this
     * Scheduling Strategy is used, the component will be scheduled in the same
     * manner as if {@link TIMER_DRIVEN} were used.
     */
    @Deprecated
    PRIMARY_NODE_ONLY(1, "0 sec"),
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
