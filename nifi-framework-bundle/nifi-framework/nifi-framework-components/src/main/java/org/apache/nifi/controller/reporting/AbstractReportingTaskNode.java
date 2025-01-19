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

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.ControllerServiceCreationDetails;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.VerifiableReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractReportingTaskNode extends AbstractComponentNode implements ReportingTaskNode {

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
                                     final ValidationContextFactory validationContextFactory, final ReloadComponent reloadComponent,
                                     final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(reportingTask, id, controllerServiceProvider, processScheduler, validationContextFactory,
                reportingTask.getComponent().getClass().getSimpleName(), reportingTask.getComponent().getClass().getCanonicalName(),
                reloadComponent, extensionManager, validationTrigger, false);
    }


    public AbstractReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id, final ControllerServiceProvider controllerServiceProvider,
                                     final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                     final String componentType, final String componentCanonicalClass, final ReloadComponent reloadComponent,
                                     final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                                     final boolean isExtensionMissing) {

        super(id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.reportingTaskRef = new AtomicReference<>(new ReportingTaskDetails(reportingTask));
        this.processScheduler = processScheduler;
        this.serviceLookup = controllerServiceProvider;

        final Class<?> reportingClass = reportingTask.getComponent().getClass();

        final DefaultSchedule dsc = reportingClass.getAnnotation(DefaultSchedule.class);
        if (dsc != null) {
            try {
                this.setSchedulingStrategy(dsc.strategy());
            } catch (Throwable ex) {
                LOG.error("Error while setting scheduling strategy from DefaultSchedule annotation", ex);
            }
            try {
                this.setSchedulingPeriod(dsc.period());
            } catch (Throwable ex) {
                this.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
                LOG.error("Error while setting scheduling period from DefaultSchedule annotation", ex);
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
    public TerminationAwareLogger getLogger() {
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while Reporting Task is running");
        }
        this.reportingTaskRef.set(new ReportingTaskDetails(reportingTask));
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws ReportingTaskInstantiationException {
        final String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
    }

    @Override
    public boolean isRunning() {
        return processScheduler.isScheduled(this) || processScheduler.getActiveThreadCount(this) > 0;
    }

    @Override
    public boolean isValidationNecessary() {
        return !processScheduler.isScheduled(this) || getValidationStatus() != ValidationStatus.VALID;
    }

    @Override
    public int getActiveThreadCount() {
        return processScheduler.getActiveThreadCount(this);
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup, getSchedulingPeriod());
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify " + this + " while the Reporting Task is running");
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
            throw new IllegalStateException("Cannot delete " + this + " because it is currently running");
        }
    }

    @Override
    public void verifyCanDisable() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot disable " + this + " because it is currently running");
        }

        if (isDisabled()) {
            throw new IllegalStateException("Cannot disable " + this + " because it is already disabled");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (!isDisabled()) {
            throw new IllegalStateException("Cannot enable " + this + " because it is not disabled");
        }
    }

    @Override
    public void verifyCanStart() {
        if (isDisabled()) {
            throw new IllegalStateException("Cannot start " + this + " because it is currently disabled");
        }

        final ValidationState validationState = getValidationState();
        if (validationState.getStatus() == ValidationStatus.INVALID) {
            throw new IllegalStateException("Cannot start " + this +
                " because it is invalid with the following validation errors: " + validationState.getValidationErrors());
        }
    }

    @Override
    public void verifyCanStop() {
        if (!isRunning()) {
            throw new IllegalStateException("Cannot stop " + this + " because it is not running");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot update " + this + " because it is currently running");
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

        final Collection<ValidationResult> validationResults = getValidationErrors(ignoredReferences);
        if (!validationResults.isEmpty()) {
            throw new IllegalStateException(this + " cannot be started because it is not currently valid");
        }
    }

    @Override
    public String toString() {
        return "ReportingTask[id=" + getIdentifier() + ", name=" + getName() + "]";
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return ParameterLookup.EMPTY;
    }

    @Override
    public void verifyCanPerformVerification() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot perform verification of " + this + " because Reporting Task is not fully stopped");
        }
    }

    @Override
    public List<ConfigVerificationResult> verifyConfiguration(final ConfigurationContext context, final ComponentLog logger, final ExtensionManager extensionManager) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            verifyCanPerformVerification();

            final long startNanos = System.nanoTime();
            // Call super's verifyConfig, which will perform component validation
            results.addAll(super.verifyConfig(context.getProperties(), context.getAnnotationData(), null));
            final long validationComplete = System.nanoTime();

            // If any invalid outcomes from validation, we do not want to perform additional verification, because we only run additional verification when the component is valid.
            // This is done in order to make it much simpler to develop these verifications, since the developer doesn't have to worry about whether or not the given values are valid.
            if (!results.isEmpty() && results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED)) {
                return results;
            }

            final ReportingTask reportingTask = getReportingTask();
            if (reportingTask instanceof VerifiableReportingTask) {
                logger.debug("{} is a VerifiableReportingTask. Will perform full verification of configuration.", this);
                final VerifiableReportingTask verifiable = (VerifiableReportingTask) reportingTask;

                // Check if the given configuration requires a different classloader than the current configuration
                final boolean classpathDifferent = isClasspathDifferent(context.getProperties());

                if (classpathDifferent) {
                    // Create a classloader for the given configuration and use that to verify the component's configuration
                    final Bundle bundle = extensionManager.getBundle(getBundleCoordinate());
                    final Set<URL> classpathUrls = getAdditionalClasspathResources(context.getProperties().keySet(), descriptor -> context.getProperty(descriptor).getValue());

                    final String classloaderIsolationKey = getClassLoaderIsolationKey(context);
                    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                    try (final InstanceClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(getComponentType(), getIdentifier(), bundle, classpathUrls, false,
                                    classloaderIsolationKey)) {
                        Thread.currentThread().setContextClassLoader(detectedClassLoader);
                        results.addAll(verifiable.verify(context, logger));
                    } finally {
                        Thread.currentThread().setContextClassLoader(currentClassLoader);
                    }
                } else {
                    // Verify the configuration, using the component's classloader
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, reportingTask.getClass(), getIdentifier())) {
                        results.addAll(verifiable.verify(context, logger));
                    }
                }

                final long validationNanos = validationComplete - startNanos;
                final long verificationNanos = System.nanoTime() - validationComplete;
                logger.debug("{} completed full configuration validation in {} plus {} for validation",
                    this, FormatUtils.formatNanos(verificationNanos, false), FormatUtils.formatNanos(validationNanos, false));
            } else {
                logger.debug("{} is not a VerifiableReportingTask, so will not perform full verification of configuration. Validation took {}", this,
                    FormatUtils.formatNanos(validationComplete - startNanos, false));
            }
        } catch (final Throwable t) {
            logger.error("Failed to perform verification of Reporting Task's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                .outcome(Outcome.FAILED)
                .verificationStepName("Perform Verification")
                .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                .build());
        }

        return results;
    }

    @Override
    public void notifyPrimaryNodeChanged(final PrimaryNodeState nodeState, final LifecycleState lifecycleState) {
        final Class<?> taskClass = getReportingTask().getClass();
        final List<Method> methods = ReflectionUtils.findMethodsWithAnnotations(taskClass, new Class[] {OnPrimaryNodeStateChange.class});
        if (methods.isEmpty()) {
            return;
        }

        lifecycleState.incrementActiveThreadCount(null);
        try {
            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), taskClass, getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, getReportingTask(), nodeState);
            }
        } finally {
            lifecycleState.decrementActiveThreadCount();
        }
    }

    @Override
    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.empty();
    }

    @Override
    public void migrateConfiguration(final Map<String, String> originalPropertyValues, final ControllerServiceFactory serviceFactory) {
        final ReportingTask task = getReportingTask();

        final Map<String, String> effectiveValues = new HashMap<>();
        originalPropertyValues.forEach((key, value) -> effectiveValues.put(key, mapRawValueToEffectiveValue(value)));

        final StandardPropertyConfiguration propertyConfig = new StandardPropertyConfiguration(effectiveValues,
                originalPropertyValues, this::mapRawValueToEffectiveValue, toString(), serviceFactory);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), task.getClass(), getIdentifier())) {
            task.migrateProperties(propertyConfig);
        } catch (final Exception e) {
            LOG.error("Failed to migrate Property Configuration for {}.", this, e);
        }

        if (propertyConfig.isModified()) {
            // Create any necessary Controller Services. It is important that we create the services
            // before updating the reporting tasks's properties, as it's necessary in order to properly account
            // for the Controller Service References.
            final List<ControllerServiceCreationDetails> servicesCreated = propertyConfig.getCreatedServices();
            servicesCreated.forEach(serviceFactory::create);

            overwriteProperties(propertyConfig.getRawProperties());
        }
    }

}
