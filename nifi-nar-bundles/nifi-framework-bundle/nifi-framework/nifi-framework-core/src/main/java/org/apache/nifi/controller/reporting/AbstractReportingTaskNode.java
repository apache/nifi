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

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

public abstract class AbstractReportingTaskNode extends AbstractConfiguredComponent implements ReportingTaskNode {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractReportingTaskNode.class);

    private final AtomicReference<ReportingTaskDetails> reportingTaskRef;
    private final ProcessScheduler processScheduler;
    private final ControllerServiceLookup serviceLookup;

    private final AtomicReference<SchedulingStrategy> schedulingStrategy = new AtomicReference<>(SchedulingStrategy.TIMER_DRIVEN);
    private final AtomicReference<String> schedulingPeriod = new AtomicReference<>("5 mins");

    private volatile String comment;
    private volatile ScheduledState scheduledState = ScheduledState.STOPPED;

    public AbstractReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id,
                                     final ControllerServiceProvider controllerServiceProvider, final ProcessScheduler processScheduler,
                                     final ValidationContextFactory validationContextFactory, final ComponentVariableRegistry variableRegistry,
                                     final ReloadComponent reloadComponent) {

        this(reportingTask, id, controllerServiceProvider, processScheduler, validationContextFactory,
                reportingTask.getComponent().getClass().getSimpleName(), reportingTask.getComponent().getClass().getCanonicalName(),
                variableRegistry, reloadComponent, false);
    }


    public AbstractReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id, final ControllerServiceProvider controllerServiceProvider,
                                     final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                     final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                                     final ReloadComponent reloadComponent, final boolean isExtensionMissing) {

        super(id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent, isExtensionMissing);
        this.reportingTaskRef = new AtomicReference<>(new ReportingTaskDetails(reportingTask));
        this.processScheduler = processScheduler;
        this.serviceLookup = controllerServiceProvider;

        final Class<?> reportingClass = reportingTask.getComponent().getClass();

        DefaultSchedule dsc = AnnotationUtils.findAnnotation(reportingClass, DefaultSchedule.class);
        if(dsc != null) {
            try {
                this.setSchedulingStrategy(dsc.strategy());
            } catch (Throwable ex) {
                LOG.error(String.format("Error while setting scheduling strategy from DefaultSchedule annotation: %s", ex.getMessage()), ex);
            }
            try {
                this.setSchedulingPeriod(dsc.period());
            } catch (Throwable ex) {
                this.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
                LOG.error(String.format("Error while setting scheduling period from DefaultSchedule annotation: %s", ex.getMessage()), ex);
            }
        }
    }

    @Override
    public ConfigurableComponent getComponent() {
        return reportingTaskRef.get().getReportingTask();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return reportingTaskRef.get().getBundleCoordinate();
    }

    @Override
    public ComponentLog getLogger() {
        return reportingTaskRef.get().getComponentLog();
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
    public void setSchedulingPeriod(final String schedulingPeriod) {
        this.schedulingPeriod.set(schedulingPeriod);
    }

    @Override
    public ReportingTask getReportingTask() {
        return reportingTaskRef.get().getReportingTask();
    }

    @Override
    public void setReportingTask(final LoggableComponent<ReportingTask> reportingTask) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Reporting Task configuration while Reporting Task is running");
        }
        this.reportingTaskRef.set(new ReportingTaskDetails(reportingTask));
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws ReportingTaskInstantiationException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot reload Reporting Task while Reporting Task is running");
        }

        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
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
        return new StandardConfigurationContext(this, serviceLookup, getSchedulingPeriod(), getVariableRegistry());
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

    public boolean isDisabled() {
        return scheduledState == ScheduledState.DISABLED;
    }

    @Override
    public String getComments() {
        return comment;
    }

    @Override
    public void setComments(final String comment) {
        this.comment = CharacterFilterUtils.filterInvalidXmlCharacters(comment);
    }

    @Override
    public void verifyCanDelete() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot delete " + getReportingTask().getIdentifier() + " because it is currently running");
        }
    }

    @Override
    public void verifyCanDisable() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot disable " + getReportingTask().getIdentifier() + " because it is currently running");
        }

        if (isDisabled()) {
            throw new IllegalStateException("Cannot disable " + getReportingTask().getIdentifier() + " because it is already disabled");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (!isDisabled()) {
            throw new IllegalStateException("Cannot enable " + getReportingTask().getIdentifier() + " because it is not disabled");
        }
    }

    @Override
    public void verifyCanStart() {
        if (isDisabled()) {
            throw new IllegalStateException("Cannot start " + getReportingTask().getIdentifier() + " because it is currently disabled");
        }

        if (isRunning()) {
            throw new IllegalStateException("Cannot start " + getReportingTask().getIdentifier() + " because it is already running");
        }
    }

    @Override
    public void verifyCanStop() {
        if (!isRunning()) {
            throw new IllegalStateException("Cannot stop " + getReportingTask().getIdentifier() + " because it is not running");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot update " + getReportingTask().getIdentifier() + " because it is currently running");
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
                throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is disabled");
            case RUNNING:
                throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is already running");
            case STOPPED:
                break;
        }
        final int activeThreadCount = getActiveThreadCount();
        if (activeThreadCount > 0) {
            throw new IllegalStateException(this.getIdentifier() + " cannot be started because it has " + activeThreadCount + " active threads already");
        }

        final Set<String> ids = new HashSet<>();
        for (final ControllerServiceNode node : ignoredReferences) {
            ids.add(node.getIdentifier());
        }

        final Collection<ValidationResult> validationResults = getValidationErrors(ids);
        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is not valid: " + result);
            }
        }
    }

    @Override
    public String toString() {
        return "ReportingTask[id=" + getIdentifier() + "]";
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors(Set<String> serviceIdentifiersNotToValidate) {
        Collection<ValidationResult> results = null;
        if (getScheduledState() == ScheduledState.STOPPED) {
            results = super.getValidationErrors(serviceIdentifiersNotToValidate);
        }
        return results != null ? results : Collections.emptySet();
    }

}
