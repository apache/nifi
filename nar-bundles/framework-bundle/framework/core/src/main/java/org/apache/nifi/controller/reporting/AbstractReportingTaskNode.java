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
package org.apache.nifi.controller.reporting;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.Availability;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;

public abstract class AbstractReportingTaskNode extends AbstractConfiguredComponent implements ReportingTaskNode {

    private final ReportingTask reportingTask;
    private final ProcessScheduler processScheduler;
    private final ControllerServiceLookup serviceLookup;

    private final AtomicReference<SchedulingStrategy> schedulingStrategy = new AtomicReference<>(SchedulingStrategy.TIMER_DRIVEN);
    private final AtomicReference<String> schedulingPeriod = new AtomicReference<>("5 mins");
    private final AtomicReference<Availability> availability = new AtomicReference<>(Availability.NODE_ONLY);

    public AbstractReportingTaskNode(final ReportingTask reportingTask, final String id,
            final ControllerServiceProvider controllerServiceProvider, final ProcessScheduler processScheduler,
            final ValidationContextFactory validationContextFactory) {
        super(reportingTask, id, validationContextFactory, controllerServiceProvider);
        this.reportingTask = reportingTask;
        this.processScheduler = processScheduler;
        this.serviceLookup = controllerServiceProvider;
    }

    @Override
    public Availability getAvailability() {
        return availability.get();
    }

    @Override
    public void setAvailability(final Availability availability) {
        this.availability.set(availability);
    }

    @Override
    public void setSchedulingStrategy(final SchedulingStrategy schedulingStrategy) {
        this.schedulingStrategy.set(schedulingStrategy);
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return schedulingStrategy.get();
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod.get();
    }

    @Override
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(schedulingPeriod.get(), timeUnit);
    }

    @Override
    public void setScheduldingPeriod(final String schedulingPeriod) {
        this.schedulingPeriod.set(schedulingPeriod);
    }

    @Override
    public ReportingTask getReportingTask() {
        return reportingTask;
    }

    @Override
    public boolean isRunning() {
        return processScheduler.isScheduled(this) || processScheduler.getActiveThreadCount(this) > 0;
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup);
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Reporting Task while the Reporting Task is running");
        }
    }

}
