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
package org.apache.nifi.controller;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;

public interface ReportingTaskNode extends ConfiguredComponent {

    Availability getAvailability();

    void setAvailability(Availability availability);

    void setSchedulingStrategy(SchedulingStrategy schedulingStrategy);

    SchedulingStrategy getSchedulingStrategy();

    /**
     * @return a string representation of the time between each scheduling
     * period
     */
    String getSchedulingPeriod();

    long getSchedulingPeriod(TimeUnit timeUnit);

    /**
     * Updates how often the ReportingTask should be triggered to run
     * @param schedulingPeriod
     */
    void setScheduldingPeriod(String schedulingPeriod);

    ReportingTask getReportingTask();

    ReportingContext getReportingContext();

    ConfigurationContext getConfigurationContext();

    boolean isRunning();
    
    /**
     * Indicates the {@link ScheduledState} of this <code>ReportingTask</code>. A
     * value of stopped does NOT indicate that the <code>ReportingTask</code> has
     * no active threads, only that it is not currently scheduled to be given
     * any more threads. To determine whether or not the
     * <code>ReportingTask</code> has any active threads, see
     * {@link ProcessScheduler#getActiveThreadCount(ReportingTask)}.
     *
     * @return
     */
    ScheduledState getScheduledState();
    
    void setScheduledState(ScheduledState state);
    
    void verifyCanStart();
    void verifyCanStop();
    void verifyCanDisable();
    void verifyCanEnable();
    void verifyCanDelete();
}
