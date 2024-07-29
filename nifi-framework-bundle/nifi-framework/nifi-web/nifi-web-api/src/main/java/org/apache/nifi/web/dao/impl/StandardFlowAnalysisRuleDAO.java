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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleInstantiationException;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.logging.repository.NopLogRepository;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class StandardFlowAnalysisRuleDAO extends ComponentDAO implements FlowAnalysisRuleDAO {

    private FlowAnalysisRuleProvider flowAnalysisRuleProvider;
    private ComponentStateDAO componentStateDAO;
    private ReloadComponent reloadComponent;
    private FlowController flowController;

    private FlowAnalysisRuleNode locateFlowAnalysisRule(final String flowAnalysisRuleId) {
        // get the flow analysis rule
        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleProvider.getFlowAnalysisRuleNode(flowAnalysisRuleId);

        // ensure the flow analysis rule exists
        if (flowAnalysisRule == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate flow analysis rule with id '%s'.", flowAnalysisRuleId));
        }

        return flowAnalysisRule;
    }

    @Override
    public void verifyCreate(final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        verifyCreate(flowAnalysisRuleProvider.getExtensionManager(), flowAnalysisRuleDTO.getType(), flowAnalysisRuleDTO.getBundle());
    }

    @Override
    public FlowAnalysisRuleNode createFlowAnalysisRule(final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        // ensure the type is specified
        if (flowAnalysisRuleDTO.getType() == null) {
            throw new IllegalArgumentException("The flow analysis rule type must be specified.");
        }

        try {
            // create the flow analysis rule
            final ExtensionManager extensionManager = flowAnalysisRuleProvider.getExtensionManager();
            final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(extensionManager, flowAnalysisRuleDTO.getType(), flowAnalysisRuleDTO.getBundle());
            final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleProvider.createFlowAnalysisRule(
                    flowAnalysisRuleDTO.getType(), flowAnalysisRuleDTO.getId(), bundleCoordinate, true);

            // ensure we can perform the update
            verifyUpdate(flowAnalysisRule, flowAnalysisRuleDTO);

            // perform the update
            configureFlowAnalysisRule(flowAnalysisRule, flowAnalysisRuleDTO);

            flowController.getFlowManager().getFlowAnalyzer().ifPresent(
                    flowAnalyzer -> flowAnalyzer.setFlowAnalysisRequired(true)
            );

            return flowAnalysisRule;
        } catch (FlowAnalysisRuleInstantiationException rtie) {
            throw new NiFiCoreException(rtie.getMessage(), rtie);
        }
    }

    @Override
    public FlowAnalysisRuleNode getFlowAnalysisRule(final String flowAnalysisRuleId) {
        return locateFlowAnalysisRule(flowAnalysisRuleId);
    }

    @Override
    public boolean hasFlowAnalysisRule(final String flowAnalysisRuleId) {
        return flowAnalysisRuleProvider.getFlowAnalysisRuleNode(flowAnalysisRuleId) != null;
    }

    @Override
    public Set<FlowAnalysisRuleNode> getFlowAnalysisRules() {
        return flowAnalysisRuleProvider.getAllFlowAnalysisRules();
    }

    @Override
    public FlowAnalysisRuleNode updateFlowAnalysisRule(final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        // get the flow analysis rule
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleDTO.getId());

        // ensure we can perform the update
        verifyUpdate(flowAnalysisRule, flowAnalysisRuleDTO);

        // perform the update
        configureFlowAnalysisRule(flowAnalysisRule, flowAnalysisRuleDTO);

        // attempt to change the underlying processor if an updated bundle is specified
        // updating the bundle must happen after configuring so that any additional classpath resources are set first
        updateBundle(flowAnalysisRule, flowAnalysisRuleDTO);

        // configure state
        // see if an update is necessary
        if (isNotNull(flowAnalysisRuleDTO.getState())) {
            final FlowAnalysisRuleState purposedState = FlowAnalysisRuleState.valueOf(flowAnalysisRuleDTO.getState());

            // only attempt an action if it is changing
            if (!purposedState.equals(flowAnalysisRule.getState())) {
                try {
                    // perform the appropriate action
                    switch (purposedState) {
                        case ENABLED:
                            flowAnalysisRuleProvider.enableFlowAnalysisRule(flowAnalysisRule);
                            break;
                        case DISABLED:
                            flowAnalysisRuleProvider.disableFlowAnalysisRule(flowAnalysisRule);
                            break;
                    }
                } catch (IllegalStateException | ComponentLifeCycleException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                } catch (NullPointerException npe) {
                    throw new NiFiCoreException("Unable to update flow analysis rule state.", npe);
                } catch (Exception e) {
                    throw new NiFiCoreException("Unable to update flow analysis rule state: " + e, e);
                }
            }
        }

        flowController.getFlowManager().getFlowAnalyzer().ifPresent(
                flowAnalyzer -> flowAnalyzer.setFlowAnalysisRequired(true)
        );

        return flowAnalysisRule;
    }

    private void updateBundle(FlowAnalysisRuleNode flowAnalysisRule, FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        final BundleDTO bundleDTO = flowAnalysisRuleDTO.getBundle();
        if (bundleDTO != null) {
            final ExtensionManager extensionManager = flowAnalysisRuleProvider.getExtensionManager();
            final BundleCoordinate incomingCoordinate = BundleUtils.getBundle(extensionManager, flowAnalysisRule.getCanonicalClassName(), bundleDTO);
            final BundleCoordinate existingCoordinate = flowAnalysisRule.getBundleCoordinate();
            if (!existingCoordinate.getCoordinate().equals(incomingCoordinate.getCoordinate())) {
                try {
                    // we need to use the property descriptors from the temp component here in case we are changing from a ghost component to a real component
                    final ConfigurableComponent tempComponent = extensionManager.getTempComponent(flowAnalysisRule.getCanonicalClassName(), incomingCoordinate);
                    final Set<URL> additionalUrls = flowAnalysisRule.getAdditionalClasspathResources(tempComponent.getPropertyDescriptors());
                    reloadComponent.reload(flowAnalysisRule, flowAnalysisRule.getCanonicalClassName(), incomingCoordinate, additionalUrls);
                } catch (FlowAnalysisRuleInstantiationException e) {
                    throw new NiFiCoreException(String.format("Unable to update flow analysis rule %s from %s to %s due to: %s",
                            flowAnalysisRuleDTO.getId(), flowAnalysisRule.getBundleCoordinate().getCoordinate(), incomingCoordinate.getCoordinate(), e.getMessage()), e);
                }
            }
        }
    }

    private List<String> validateProposedConfiguration(final FlowAnalysisRuleNode flowAnalysisRule, final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        final List<String> validationErrors = new ArrayList<>();

        return validationErrors;
    }

    @Override
    public void verifyDelete(final String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleId);
        flowAnalysisRule.verifyCanDelete();
    }

    @Override
    public void verifyUpdate(final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleDTO.getId());
        verifyUpdate(flowAnalysisRule, flowAnalysisRuleDTO);
    }

    private void verifyUpdate(final FlowAnalysisRuleNode flowAnalysisRule, final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        // ensure the state, if specified, is valid
        if (isNotNull(flowAnalysisRuleDTO.getState())) {
            try {
                final FlowAnalysisRuleState purposedState = FlowAnalysisRuleState.valueOf(flowAnalysisRuleDTO.getState());

                // only attempt an action if it is changing
                if (!purposedState.equals(flowAnalysisRule.getState())) {
                    // perform the appropriate action
                    switch (purposedState) {
                        case ENABLED:
                            flowAnalysisRule.verifyCanEnable();
                            break;
                        case DISABLED:
                            flowAnalysisRule.verifyCanDisable();
                            break;
                    }
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(String.format(
                        "The specified flow analysis rule state (%s) is not valid. Valid options are 'ENABLED' or 'DISABLED'.",
                        flowAnalysisRuleDTO.getState()));
            }
        }

        boolean modificationRequest = false;
        if (isAnyNotNull(flowAnalysisRuleDTO.getName(),
                flowAnalysisRuleDTO.getProperties(),
                flowAnalysisRuleDTO.getBundle())) {
            modificationRequest = true;

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(flowAnalysisRule, flowAnalysisRuleDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }

        final BundleDTO bundleDTO = flowAnalysisRuleDTO.getBundle();
        if (bundleDTO != null) {
            // ensures all nodes in a cluster have the bundle, throws exception if bundle not found for the given type
            final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(
                    flowAnalysisRuleProvider.getExtensionManager(), flowAnalysisRule.getCanonicalClassName(), bundleDTO);
            // ensure we are only changing to a bundle with the same group and id, but different version
            flowAnalysisRule.verifyCanUpdateBundle(bundleCoordinate);
        }

        if (modificationRequest) {
            flowAnalysisRule.verifyCanUpdate();
        }
    }

    @Override
    public void verifyConfigVerification(final String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode ruleNode = locateFlowAnalysisRule(flowAnalysisRuleId);
        ruleNode.verifyCanPerformVerification();
    }

    @Override
    public List<ConfigVerificationResultDTO> verifyConfiguration(final String flowAnalysisRuleId, final Map<String, String> properties) {
        final FlowAnalysisRuleNode ruleNode = locateFlowAnalysisRule(flowAnalysisRuleId);

        final LogRepository logRepository = new NopLogRepository();
        final ComponentLog configVerificationLog = new SimpleProcessLogger(ruleNode.getFlowAnalysisRule(), logRepository, new StandardLoggingContext(null));
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ParameterLookup parameterLookup = ParameterLookup.EMPTY;
        final ConfigurationContext configurationContext = new StandardConfigurationContext(ruleNode, properties, ruleNode.getAnnotationData(),
            parameterLookup, flowController.getControllerServiceProvider(), null);

        final List<ConfigVerificationResult> verificationResults = ruleNode.verifyConfiguration(configurationContext, configVerificationLog, extensionManager);
        final List<ConfigVerificationResultDTO> resultsDtos = verificationResults.stream()
            .map(this::createConfigVerificationResultDto)
            .collect(Collectors.toList());

        return resultsDtos;
    }

    private ConfigVerificationResultDTO createConfigVerificationResultDto(final ConfigVerificationResult result) {
        final ConfigVerificationResultDTO dto = new ConfigVerificationResultDTO();
        dto.setExplanation(result.getExplanation());
        dto.setOutcome(result.getOutcome().name());
        dto.setVerificationStepName(result.getVerificationStepName());
        return dto;
    }

    private void configureFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRule, final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        final String name = flowAnalysisRuleDTO.getName();
        final String comments = flowAnalysisRuleDTO.getComments();
        final String enforcementPolicy = flowAnalysisRuleDTO.getEnforcementPolicy();
        final Map<String, String> properties = flowAnalysisRuleDTO.getProperties();

        flowAnalysisRule.pauseValidationTrigger(); // avoid triggering validation multiple times
        try {
            if (isNotNull(enforcementPolicy)) {
                flowAnalysisRule.setEnforcementPolicy(EnforcementPolicy.valueOf(enforcementPolicy));
            }
            if (isNotNull(name)) {
                flowAnalysisRule.setName(name);
            }
            if (isNotNull(comments)) {
                flowAnalysisRule.setComments(comments);
            }
            if (isNotNull(properties)) {
                final Set<String> sensitiveDynamicPropertyNames = flowAnalysisRuleDTO.getSensitiveDynamicPropertyNames();
                flowAnalysisRule.setProperties(
                        properties,
                        false,
                        sensitiveDynamicPropertyNames == null ? Collections.emptySet() : sensitiveDynamicPropertyNames
                );
            }
        } finally {
            flowAnalysisRule.resumeValidationTrigger();
        }
    }

    @Override
    public void deleteFlowAnalysisRule(String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleId);
        flowAnalysisRuleProvider.removeFlowAnalysisRule(flowAnalysisRule);

        flowController.getFlowManager().getFlowAnalyzer().ifPresent(
                flowAnalyzer -> flowAnalyzer.setFlowAnalysisRequired(true)
        );
    }

    @Override
    public StateMap getState(String flowAnalysisRuleId, Scope scope) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleId);
        return componentStateDAO.getState(flowAnalysisRule, scope);
    }

    @Override
    public void verifyClearState(String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleId);
        flowAnalysisRule.verifyCanClearState();
    }

    @Override
    public void clearState(String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = locateFlowAnalysisRule(flowAnalysisRuleId);
        componentStateDAO.clearState(flowAnalysisRule);
    }

    @Autowired
    public void setFlowAnalysisRuleProvider(FlowAnalysisRuleProvider flowAnalysisRuleProvider) {
        this.flowAnalysisRuleProvider = flowAnalysisRuleProvider;
    }

    @Autowired
    public void setComponentStateDAO(ComponentStateDAO componentStateDAO) {
        this.componentStateDAO = componentStateDAO;
    }

    @Autowired
    public void setReloadComponent(ReloadComponent reloadComponent) {
        this.reloadComponent = reloadComponent;
    }

    @Autowired
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
