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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.flowanalysis.VerifiableFlowAnalysisRule;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.validation.RuleViolationsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractFlowAnalysisRuleNode extends AbstractComponentNode implements FlowAnalysisRuleNode {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicReference<FlowAnalysisRuleDetails> flowAnalysisRuleRef;
    private final ControllerServiceLookup serviceLookup;
    private final RuleViolationsManager ruleViolationsManager;

    private volatile String comment;
    private EnforcementPolicy enforcementPolicy;

    private volatile FlowAnalysisRuleState state = FlowAnalysisRuleState.DISABLED;

    public AbstractFlowAnalysisRuleNode(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule, final String id,
                                        final ControllerServiceProvider controllerServiceProvider,
                                        final ValidationContextFactory validationContextFactory,
                                        final RuleViolationsManager ruleViolationsManager,
                                        final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(flowAnalysisRule, id, controllerServiceProvider, validationContextFactory, ruleViolationsManager,
                flowAnalysisRule.getComponent().getClass().getSimpleName(), flowAnalysisRule.getComponent().getClass().getCanonicalName(),
                reloadComponent, extensionManager, validationTrigger, false);
    }


    public AbstractFlowAnalysisRuleNode(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule, final String id, final ControllerServiceProvider controllerServiceProvider,
                                        final ValidationContextFactory validationContextFactory, final RuleViolationsManager ruleViolationsManager,
                                        final String componentType, final String componentCanonicalClass, final ReloadComponent reloadComponent,
                                        final ExtensionManager extensionManager, final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {

        super(id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.flowAnalysisRuleRef = new AtomicReference<>(new FlowAnalysisRuleDetails(flowAnalysisRule));
        this.serviceLookup = controllerServiceProvider;
        this.ruleViolationsManager = ruleViolationsManager;
        this.enforcementPolicy = EnforcementPolicy.WARN;
    }

    @Override
    public EnforcementPolicy getEnforcementPolicy() {
        return enforcementPolicy;
    }

    @Override
    public void setEnforcementPolicy(EnforcementPolicy enforcementPolicy) {
        this.enforcementPolicy = enforcementPolicy;
    }

    @Override
    public ConfigurableComponent getComponent() {
        return flowAnalysisRuleRef.get().getFlowAnalysisRule();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return flowAnalysisRuleRef.get().getBundleCoordinate();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return flowAnalysisRuleRef.get().getComponentLog();
    }

    @Override
    public FlowAnalysisRule getFlowAnalysisRule() {
        return flowAnalysisRuleRef.get().getFlowAnalysisRule();
    }

    @Override
    public void setFlowAnalysisRule(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule) {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot modify Flow Analysis Rule configuration while it is enabled");
        }
        this.flowAnalysisRuleRef.set(new FlowAnalysisRuleDetails(flowAnalysisRule));
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws FlowAnalysisRuleInstantiationException {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot reload Flow Analysis Rule while it is enabled");
        }
        String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
    }

    @Override
    public boolean isEnabled() {
        return FlowAnalysisRuleState.ENABLED.equals(state);
    }

    @Override
    public boolean isValidationNecessary() {
        return !isEnabled() || getValidationStatus() != ValidationStatus.VALID;
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup, null);
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot modify Flow Analysis Rule while it is enabled");
        }
    }

    @Override
    public FlowAnalysisRuleState getState() {
        return state;
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
        if (isEnabled()) {
            throw new IllegalStateException("Cannot delete " + getFlowAnalysisRule().getIdentifier() + " because it is enabled");
        }
    }

    @Override
    public void verifyCanDisable() {
        if (!isEnabled()) {
            throw new IllegalStateException("Cannot disable " + getFlowAnalysisRule().getIdentifier() + " because it is already disabled");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (getValidationStatus() == ValidationStatus.INVALID) {
            throw new IllegalStateException("Cannot enable " + getFlowAnalysisRule().getIdentifier() + " because it is in INVALID status");
        }

        if (isEnabled()) {
            throw new IllegalStateException("Cannot enable " + getFlowAnalysisRule().getIdentifier() + " because it is not disabled");
        }
    }

    @Override
    public void verifyCanEnable(final Set<ControllerServiceNode> ignoredServices) {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot enable " + getFlowAnalysisRule().getIdentifier() + " because it is not disabled");
        }

        final Collection<ValidationResult> validationResults = getValidationErrors(ignoredServices);
        if (!validationResults.isEmpty()) {
            throw new IllegalStateException(this + " cannot be enabled because it is not currently valid");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot update " + getFlowAnalysisRule().getIdentifier() + " because it is currently enabled");
        }
    }

    @Override
    public void verifyCanClearState() {
        verifyCanUpdate();
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
    public String toString() {
        FlowAnalysisRule flowAnalysisRule = flowAnalysisRuleRef.get().getFlowAnalysisRule();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), flowAnalysisRule.getClass(), flowAnalysisRule.getIdentifier())) {
            return getFlowAnalysisRule().toString();
        }
    }

    @Override
    public void enable() {
        verifyCanEnable();
        setState(FlowAnalysisRuleState.ENABLED, OnEnabled.class);
    }

    @Override
    public void disable() {
        verifyCanDisable();
        setState(FlowAnalysisRuleState.DISABLED, OnDisabled.class);

        ruleViolationsManager.removeRuleViolationsForRule(getIdentifier());
        ruleViolationsManager.cleanUp();
    }

    private void setState(FlowAnalysisRuleState newState, Class<? extends Annotation> annotation) {
        final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceLookup, null);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), getFlowAnalysisRule().getClass(), getIdentifier())) {
            ReflectionUtils.invokeMethodsWithAnnotation(annotation, getFlowAnalysisRule(), configContext);

            this.state = newState;

            log.debug("Successfully {} {}", newState.toString().toLowerCase(), this);
        } catch (Exception e) {
            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;

            final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), getFlowAnalysisRule(), new StandardLoggingContext(null));

            componentLog.error("Failed to invoke {} method", cause);

            log.error("Failed to invoke {} method of {}", annotation.getSimpleName(), getFlowAnalysisRule(), cause);
        }
    }

    @Override
    public void verifyCanPerformVerification() {
        if (isEnabled()) {
            throw new IllegalStateException("Cannot perform verification of " + this + " because Flow Analysis Rule is not fully stopped");
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
            if (!results.isEmpty() && results.stream().anyMatch(result -> result.getOutcome() == ConfigVerificationResult.Outcome.FAILED)) {
                return results;
            }

            final FlowAnalysisRule flowAnalysisRule = getFlowAnalysisRule();
            if (flowAnalysisRule instanceof VerifiableFlowAnalysisRule) {
                logger.debug("{} is a VerifiableFlowAnalysisRule. Will perform full verification of configuration.", this);
                final VerifiableFlowAnalysisRule verifiable = (VerifiableFlowAnalysisRule) flowAnalysisRule;

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
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, flowAnalysisRule.getClass(), getIdentifier())) {
                        results.addAll(verifiable.verify(context, logger));
                    }
                }

                final long validationNanos = validationComplete - startNanos;
                final long verificationNanos = System.nanoTime() - validationComplete;
                logger.debug("{} completed full configuration validation in {} plus {} for validation",
                    this, FormatUtils.formatNanos(verificationNanos, false), FormatUtils.formatNanos(validationNanos, false));
            } else {
                logger.debug("{} is not a VerifiableFlowAnalysisRule, so will not perform full verification of configuration. Validation took {}", this,
                    FormatUtils.formatNanos(validationComplete - startNanos, false));
            }
        } catch (final Throwable t) {
            logger.error("Failed to perform verification of Flow Analysis Rule's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                .outcome(ConfigVerificationResult.Outcome.FAILED)
                .verificationStepName("Perform Verification")
                .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                .build());
        }

        return results;
    }

    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.empty();
    }

}
