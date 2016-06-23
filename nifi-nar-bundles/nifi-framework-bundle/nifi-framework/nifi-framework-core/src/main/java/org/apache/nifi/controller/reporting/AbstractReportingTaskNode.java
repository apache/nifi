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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;

public abstract class AbstractReportingTaskNode extends AbstractConfiguredComponent implements ReportingTaskNode {

    private final ReportingTask reportingTask;
    private final ProcessScheduler processScheduler;
    private final ControllerServiceLookup serviceLookup;

    private final AtomicReference<SchedulingStrategy> schedulingStrategy = new AtomicReference<>(SchedulingStrategy.TIMER_DRIVEN);
    private final AtomicReference<String> schedulingPeriod = new AtomicReference<>("5 mins");

    private volatile String comment;
    private volatile ScheduledState scheduledState = ScheduledState.STOPPED;

    protected final VariableRegistry variableRegistry;

    public AbstractReportingTaskNode(final ReportingTask reportingTask, final String id,
        final ControllerServiceProvider controllerServiceProvider, final ProcessScheduler processScheduler,
        final ValidationContextFactory validationContextFactory, final VariableRegistry variableRegistry) {

        this(reportingTask, id, controllerServiceProvider, processScheduler, validationContextFactory,
            reportingTask.getClass().getSimpleName(), reportingTask.getClass().getCanonicalName(),variableRegistry);
    }


    public AbstractReportingTaskNode(final ReportingTask reportingTask, final String id,
            final ControllerServiceProvider controllerServiceProvider, final ProcessScheduler processScheduler,
        final ValidationContextFactory validationContextFactory,
        final String componentType, final String componentCanonicalClass, VariableRegistry variableRegistry) {

        super(reportingTask, id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass);
        this.reportingTask = reportingTask;
        this.processScheduler = processScheduler;
        this.serviceLookup = controllerServiceProvider;
        this.variableRegistry = variableRegistry;
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
    public int getActiveThreadCount() {
        return processScheduler.getActiveThreadCount(this);
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup, getSchedulingPeriod(), variableRegistry);
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
    }

    @Override
    public boolean removeProperty(String name) {
        return super.removeProperty(name);
    }


    public boolean isDisabled() {
        return scheduledState == ScheduledState.DISABLED;
    }

    @Override
    public String getComments() {
        return comment;
    }

    @Override
    public void setComments(final String comment) {
        this.comment = comment;
    }

    @Override
    public void verifyCanDelete() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot delete " + reportingTask + " because it is currently running");
        }
    }

    @Override
    public void verifyCanDisable() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot disable " + reportingTask + " because it is currently running");
        }

        if (isDisabled()) {
            throw new IllegalStateException("Cannot disable " + reportingTask + " because it is already disabled");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (!isDisabled()) {
            throw new IllegalStateException("Cannot enable " + reportingTask + " because it is not disabled");
        }
    }

    @Override
    public void verifyCanStart() {
        if (isDisabled()) {
            throw new IllegalStateException("Cannot start " + reportingTask + " because it is currently disabled");
        }

        if (isRunning()) {
            throw new IllegalStateException("Cannot start " + reportingTask + " because it is already running");
        }
    }

    @Override
    public void verifyCanStop() {
        if (!isRunning()) {
            throw new IllegalStateException("Cannot stop " + reportingTask + " because it is not running");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot update " + reportingTask + " because it is currently running");
        }
    }

    @Override
    public void verifyCanClearState() {
        verifyCanUpdate();
    }

    @Override
    public void verifyCanStart(final Set<ControllerServiceNode> ignoredReferences) {
        switch (getScheduledState()) {
            case DISABLED:
                throw new IllegalStateException(this + " cannot be started because it is disabled");
            case RUNNING:
                throw new IllegalStateException(this + " cannot be started because it is already running");
            case STOPPED:
                break;
        }
        final int activeThreadCount = getActiveThreadCount();
        if (activeThreadCount > 0) {
            throw new IllegalStateException(this + " cannot be started because it has " + activeThreadCount + " active threads already");
        }

        final Set<String> ids = new HashSet<>();
        for (final ControllerServiceNode node : ignoredReferences) {
            ids.add(node.getIdentifier());
        }

        final Collection<ValidationResult> validationResults = getValidationErrors(ids);
        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                throw new IllegalStateException(this + " cannot be started because it is not valid: " + result);
            }
        }
    }

    @Override
    public String toString() {
        return "ReportingTask[id=" + getIdentifier() + ", name=" + getName() + "]";
    }

    @Override
    protected String getProcessGroupIdentifier() {
        return null;
    }
}
