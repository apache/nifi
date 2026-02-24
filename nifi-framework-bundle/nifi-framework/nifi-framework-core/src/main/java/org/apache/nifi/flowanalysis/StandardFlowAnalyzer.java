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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleProvider;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisUtil;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link FlowAnalyzer} that uses {@link org.apache.nifi.flowanalysis.FlowAnalysisRule FlowAnalysisRules}.
 */
public class StandardFlowAnalyzer implements FlowAnalyzer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final RuleViolationsManager ruleViolationsManager;

    private final FlowAnalysisRuleProvider flowAnalysisRuleProvider;
    private final ExtensionManager extensionManager;

    private ControllerServiceProvider controllerServiceProvider;

    private volatile boolean flowAnalysisRequired;

    public StandardFlowAnalyzer(
            final RuleViolationsManager ruleViolationsManager,
            final FlowAnalysisRuleProvider flowAnalysisRuleProvider,
            final ExtensionManager extensionManager
    ) {
        this.ruleViolationsManager = ruleViolationsManager;
        this.flowAnalysisRuleProvider = flowAnalysisRuleProvider;
        this.extensionManager = extensionManager;
    }

    public void initialize(final ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    @Override
    public boolean isFlowAnalysisRequired() {
        return flowAnalysisRequired;
    }

    @Override
    public void setFlowAnalysisRequired(final boolean flowAnalysisRequired) {
        this.flowAnalysisRequired = flowAnalysisRequired;
    }

    @Override
    public void analyzeProcessor(final ProcessorNode processorNode) {
        logger.debug("Running analysis on {}", processorNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        final VersionedProcessor versionedProcessor = mapper.mapProcessor(
                processorNode,
                controllerServiceProvider,
                Collections.emptySet(),
                new HashMap<>()
        );

        analyzeComponent(versionedProcessor);
    }

    @Override
    public void analyzeControllerService(final ControllerServiceNode controllerServiceNode) {
        logger.debug("Running analysis on {}", controllerServiceNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        final VersionedControllerService versionedControllerService = mapper.mapControllerService(
                controllerServiceNode,
                controllerServiceProvider,
                Collections.emptySet(),
                new HashMap<>()
        );

        analyzeComponent(versionedControllerService);
    }

    private void analyzeComponent(final VersionedComponent component) {
        final long start = System.currentTimeMillis();

        final String componentId = component.getIdentifier();
        final ComponentType componentType = component.getComponentType();

        final Set<FlowAnalysisRuleNode> flowAnalysisRules = flowAnalysisRuleProvider.getAllFlowAnalysisRules();

        final Set<RuleViolation> violations = flowAnalysisRules.stream()
                .filter(FlowAnalysisRuleNode::isEnabled)
                .flatMap(flowAnalysisRuleNode -> {
                    final String ruleId = flowAnalysisRuleNode.getIdentifier();

                    try {
                        final Collection<ComponentAnalysisResult> analysisResults = flowAnalysisRuleNode
                                .getFlowAnalysisRule()
                                .analyzeComponent(component, flowAnalysisRuleNode.getFlowAnalysisRuleContext());

                        return analysisResults.stream()
                                .map(analysisResult -> new RuleViolation(
                                        flowAnalysisRuleNode.getEnforcementPolicy(),
                                        componentId,
                                        componentId,
                                        getDisplayName(component),
                                        componentType,
                                        component.getGroupIdentifier(),
                                        ruleId,
                                        analysisResult.getIssueId(),
                                        analysisResult.getMessage(),
                                        analysisResult.getExplanation()
                                ));
                    } catch (final Exception e) {
                        logger.error("FlowAnalysis error while running '{}' against '{}'", flowAnalysisRuleNode.getName(), component, e);
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toSet());

        ruleViolationsManager.upsertComponentViolations(componentId, violations);

        final long end = System.currentTimeMillis();
        final long durationMs = end - start;

        logger.trace("Flow Analysis of component '{}' took {} ms", componentId, durationMs);
    }

    @Override
    public void analyzeProcessGroup(final VersionedProcessGroup processGroup) {
        logger.debug("Running analysis on process group {}.", processGroup.getIdentifier());

        final long start = System.currentTimeMillis();

        final Set<FlowAnalysisRuleNode> flowAnalysisRules = flowAnalysisRuleProvider.getAllFlowAnalysisRules();

        final Collection<RuleViolation> groupViolations = new HashSet<>();
        final Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations = new HashMap<>();

        analyzeProcessGroup(processGroup, flowAnalysisRules, groupViolations, componentToRuleViolations);

        ruleViolationsManager.upsertGroupViolations(processGroup, groupViolations, componentToRuleViolations);

        final long end = System.currentTimeMillis();
        final long durationMs = end - start;

        logger.debug("Flow Analysis of process group '{}' took {} ms", processGroup.getIdentifier(), durationMs);
    }

    private void analyzeProcessGroup(
            final VersionedProcessGroup processGroup,
            final Set<FlowAnalysisRuleNode> flowAnalysisRules,
            final Collection<RuleViolation> groupViolations,
            final Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations
    ) {
        if (flowAnalysisRules.isEmpty()) {
            return;
        }
        final String groupId = processGroup.getIdentifier();
        final ComponentType processGroupComponentType = processGroup.getComponentType();

        flowAnalysisRules.stream()
                .filter(FlowAnalysisRuleNode::isEnabled)
                .forEach(flowAnalysisRuleNode -> {
                    final String ruleId = flowAnalysisRuleNode.getIdentifier();

                    try {
                        final Collection<GroupAnalysisResult> analysisResults = flowAnalysisRuleNode.getFlowAnalysisRule().analyzeProcessGroup(
                                processGroup,
                                flowAnalysisRuleNode.getFlowAnalysisRuleContext()
                        );

                        analysisResults.forEach(analysisResult -> {
                            final Optional<VersionedComponent> componentOptional = analysisResult.getComponent();

                            if (componentOptional.isPresent()) {
                                final VersionedComponent component = componentOptional.get();

                                componentToRuleViolations.computeIfAbsent(component, __ -> new HashSet<>())
                                        .add(new RuleViolation(
                                                flowAnalysisRuleNode.getEnforcementPolicy(),
                                                component.getGroupIdentifier(),
                                                component.getIdentifier(),
                                                getDisplayName(component),
                                                component.getComponentType(),
                                                component.getGroupIdentifier(),
                                                ruleId,
                                                analysisResult.getIssueId(),
                                                analysisResult.getMessage(),
                                                analysisResult.getExplanation()
                                        ));

                            } else {
                                groupViolations.add(new RuleViolation(
                                        flowAnalysisRuleNode.getEnforcementPolicy(),
                                        groupId,
                                        groupId,
                                        getDisplayName(processGroup),
                                        processGroupComponentType,
                                        groupId,
                                        ruleId,
                                        analysisResult.getIssueId(),
                                        analysisResult.getMessage(),
                                        analysisResult.getExplanation()
                                ));
                            }
                        });
                    } catch (final Exception e) {
                        logger.error("FlowAnalysis error while running '{}' against group '{}'", flowAnalysisRuleNode.getName(), groupId, e);
                    }
                });

        processGroup.getProcessors().forEach(processor -> analyzeComponent(processor));
        processGroup.getControllerServices().forEach(controllerService -> analyzeComponent(controllerService));

        processGroup.getProcessGroups().forEach(childProcessGroup -> analyzeProcessGroup(childProcessGroup, flowAnalysisRules, groupViolations, componentToRuleViolations));
    }

    private String getDisplayName(final VersionedComponent component) {
        final String displayName;

        if (component instanceof VersionedConnection) {
            final VersionedConnection connection = (VersionedConnection) component;
            displayName = connection.getSource().getName() + " > " + connection.getSelectedRelationships().stream().collect(Collectors.joining(","));
        } else {
            displayName = component.getName();
        }

        return displayName;
    }

    private NiFiRegistryFlowMapper createMapper() {
        final NiFiRegistryFlowMapper mapper = FlowAnalysisUtil.createMapper(extensionManager);

        return mapper;
    }
}
