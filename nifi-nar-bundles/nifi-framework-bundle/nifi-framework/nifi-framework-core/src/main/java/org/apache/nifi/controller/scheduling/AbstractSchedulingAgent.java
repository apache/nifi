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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.engine.FlowEngine;

/**
 * Base implementation of the {@link SchedulingAgent} which encapsulates the
 * updates to the {@link ScheduleState} based on invoked operation and then
 * delegates to the corresponding 'do' methods. For example; By invoking
 * {@link #schedule(Connectable, ScheduleState)} the
 * {@link ScheduleState#setScheduled(boolean)} with value 'true' will be
 * invoked.
 *
 * @see EventDrivenSchedulingAgent
 * @see TimerDrivenSchedulingAgent
 * @see QuartzSchedulingAgent
 */
abstract class AbstractSchedulingAgent implements SchedulingAgent {

    protected final FlowEngine flowEngine;

    protected AbstractSchedulingAgent(FlowEngine flowEngine) {
        this.flowEngine = flowEngine;
    }

    @Override
    public void schedule(Connectable connectable, ScheduleState scheduleState) {
        scheduleState.setScheduled(true);
        this.doSchedule(connectable, scheduleState);
    }

    @Override
    public void unschedule(Connectable connectable, ScheduleState scheduleState) {
        scheduleState.setScheduled(false);
        this.doUnschedule(connectable, scheduleState);
    }

    @Override
    public void schedule(ReportingTaskNode taskNode, ScheduleState scheduleState) {
        scheduleState.setScheduled(true);
        this.doSchedule(taskNode, scheduleState);
    }

    @Override
    public void unschedule(ReportingTaskNode taskNode, ScheduleState scheduleState) {
        scheduleState.setScheduled(false);
        this.doUnschedule(taskNode, scheduleState);
    }

    /**
     * Schedules the provided {@link Connectable}. Its {@link ScheduleState}
     * will be set to <i>true</i>
     *
     * @param connectable
     *            the instance of {@link Connectable}
     * @param scheduleState
     *            the instance of {@link ScheduleState}
     */
    protected abstract void doSchedule(Connectable connectable, ScheduleState scheduleState);

    /**
     * Unschedules the provided {@link Connectable}. Its {@link ScheduleState}
     * will be set to <i>false</i>
     *
     * @param connectable
     *            the instance of {@link Connectable}
     * @param scheduleState
     *            the instance of {@link ScheduleState}
     */
    protected abstract void doUnschedule(Connectable connectable, ScheduleState scheduleState);

    /**
     * Schedules the provided {@link ReportingTaskNode}. Its
     * {@link ScheduleState} will be set to <i>true</i>
     *
     * @param connectable
     *            the instance of {@link ReportingTaskNode}
     * @param scheduleState
     *            the instance of {@link ScheduleState}
     */
    protected abstract void doSchedule(ReportingTaskNode connectable, ScheduleState scheduleState);

    /**
     * Unschedules the provided {@link ReportingTaskNode}. Its
     * {@link ScheduleState} will be set to <i>false</i>
     *
     * @param connectable
     *            the instance of {@link ReportingTaskNode}
     * @param scheduleState
     *            the instance of {@link ScheduleState}
     */
    protected abstract void doUnschedule(ReportingTaskNode connectable, ScheduleState scheduleState);
}
