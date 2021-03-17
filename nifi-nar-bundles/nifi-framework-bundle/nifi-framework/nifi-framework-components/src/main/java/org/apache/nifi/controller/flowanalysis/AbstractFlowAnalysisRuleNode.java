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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
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
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleType;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractFlowAnalysisRuleNode extends AbstractComponentNode implements FlowAnalysisRuleNode {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlowAnalysisRuleNode.class);

    private final AtomicReference<FlowAnalysisRuleDetails> flowAnalysisRuleRef;
    private final ControllerServiceLookup serviceLookup;

    private volatile String comment;
    private volatile FlowAnalysisRuleState state = FlowAnalysisRuleState.DISABLED;
    private FlowAnalysisRuleType ruleType;

    public AbstractFlowAnalysisRuleNode(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule, final String id,
                                        final ControllerServiceProvider controllerServiceProvider,
                                        final ValidationContextFactory validationContextFactory, final ComponentVariableRegistry variableRegistry,
                                        final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(flowAnalysisRule, id, controllerServiceProvider, validationContextFactory,
                flowAnalysisRule.getComponent().getClass().getSimpleName(), flowAnalysisRule.getComponent().getClass().getCanonicalName(),
                variableRegistry, reloadComponent, extensionManager, validationTrigger, false);
    }


    public AbstractFlowAnalysisRuleNode(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule, final String id, final ControllerServiceProvider controllerServiceProvider,
                                        final ValidationContextFactory validationContextFactory,
                                        final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                                        final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                                        final boolean isExtensionMissing) {

        super(id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.flowAnalysisRuleRef = new AtomicReference<>(new FlowAnalysisRuleDetails(flowAnalysisRule));
        this.serviceLookup = controllerServiceProvider;
        this.ruleType = FlowAnalysisRuleType.RECOMMENDATION;
    }

    @Override
    public FlowAnalysisRuleType getRuleType() {
        return ruleType;
    }

    @Override
    public void setRuleType(FlowAnalysisRuleType ruleType) {
        this.ruleType = ruleType;
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
        String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls);
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
        return new StandardConfigurationContext(this, serviceLookup, null, getVariableRegistry());
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
    public void setState(final FlowAnalysisRuleState state) {
        this.state = state;
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
    public String toString() {
        return "FlowAnalysisRule[id=" + getIdentifier() + "]";
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return ParameterLookup.EMPTY;
    }
}
