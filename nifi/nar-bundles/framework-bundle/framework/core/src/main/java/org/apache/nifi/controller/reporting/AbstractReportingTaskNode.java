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

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.Availability;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.controller.exception.ProcessorLifeCycleException;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;

public abstract class AbstractReportingTaskNode extends AbstractConfiguredComponent implements ReportingTaskNode {

    private final ReportingTask reportingTask;
    private final ProcessScheduler processScheduler;
    private final ControllerServiceLookup serviceLookup;

    private final AtomicReference<SchedulingStrategy> schedulingStrategy = new AtomicReference<>(SchedulingStrategy.TIMER_DRIVEN);
    private final AtomicReference<String> schedulingPeriod = new AtomicReference<>("5 mins");
    private final AtomicReference<Availability> availability = new AtomicReference<>(Availability.NODE_ONLY);

    private volatile ScheduledState scheduledState = ScheduledState.STOPPED;
    
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

    @Override
    public ScheduledState getScheduledState() {
        return scheduledState;
    }
    
    @Override
    public void setScheduledState(final ScheduledState state) {
        this.scheduledState = state;
    }
    
    @Override
    public void setProperty(final String name, final String value) {
        super.setProperty(name, value);
        
        onConfigured();
    }
    
    @Override
    public boolean removeProperty(String name) {
        final boolean removed = super.removeProperty(name);
        if ( removed ) {
            onConfigured();
        }
        
        return removed;
    }
    
    private void onConfigured() {
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, serviceLookup);
            ReflectionUtils.invokeMethodsWithAnnotation(OnConfigured.class, reportingTask, configContext);
        } catch (final Exception e) {
            throw new ProcessorLifeCycleException("Failed to invoke On-Configured Lifecycle methods of " + reportingTask, e);
        }
    }
    
    @Override
    public void verifyCanDelete() {
        if (isRunning()) {
            throw new IllegalStateException(this + " is running");
        }
    }
    
}
