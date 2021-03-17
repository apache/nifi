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
package org.apache.nifi.analyzeflow;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleProvider;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flowanalysis.FlowDetails;
import org.apache.nifi.flowanalysis.FlowAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisResults;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleType;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.validation.FlowAnalysisContext;
import org.apache.nifi.validation.RuleViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MainFlowAnalyzer implements FlowAnalyzer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private FlowAnalysisContext flowAnalysisContext;

    private FlowAnalysisRuleProvider flowAnalysisRuleProvider;
    private ExtensionManager extensionManager;
    private ControllerServiceProvider controllerServiceProvider;

    private Function<String, VersionedControllerService> controllerServiceDetailsProvider = id -> {
        final NiFiRegistryFlowMapper mapper = createMapper();

        ControllerServiceNode controllerServiceNode = controllerServiceProvider.getControllerServiceNode(id);

        VersionedControllerService versionedControllerService = mapper.mapControllerService(
            controllerServiceNode,
            controllerServiceProvider,
            Collections.emptySet(),
            new HashMap<>()
        );

        return versionedControllerService;
    };

    @Override
    public FlowAnalysisResults analyzeProcessor(ProcessorNode processorNode) {
        logger.debug("Running analysis on {}", processorNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        VersionedProcessor versionedProcessor = mapper.mapProcessor(
            processorNode,
            controllerServiceProvider,
            Collections.emptySet(),
            new HashMap<>()
        );

        FlowDetails flowDetails = new FlowDetails();
        flowDetails.getProcessors().add(versionedProcessor);

        return analyze(flowDetails);
    }

    @Override
    public FlowAnalysisResults analyzeControllerService(ControllerServiceNode controllerServiceNode) {
        logger.debug("Running analysis on {}", controllerServiceNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        VersionedControllerService versionedControllerService = mapper.mapControllerService(
            controllerServiceNode,
            controllerServiceProvider,
            Collections.emptySet(),
            new HashMap<>()
        );

        FlowDetails flowDetails = new FlowDetails();
        flowDetails.getControllerServices().add(versionedControllerService);

        return analyze(flowDetails);
    }

    @Override
    public FlowAnalysisResults analyzeFlow(VersionedProcessGroup processGroup) {
        FlowDetails flowDetails = new FlowDetails();

        accumulateComponents(processGroup, flowDetails);

        logger.debug("Running analysis on entire flow ({} components).", flowDetails.size());

        return analyze(flowDetails);
    }

    private NiFiRegistryFlowMapper createMapper() {
        NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(extensionManager, Function.identity());

        return mapper;
    }

    private void accumulateComponents(VersionedProcessGroup group, FlowDetails flowDetails) {
        flowDetails.getProcessors().addAll(group.getProcessors());
        flowDetails.getControllerServices().addAll(group.getControllerServices());

        for (VersionedProcessGroup childGroup : group.getProcessGroups()) {
            accumulateComponents(childGroup, flowDetails);
        }
    }

    private FlowAnalysisResults analyze(FlowDetails flowDetails) {
        Instant start = Instant.now();

        FlowAnalysisResults result = new FlowAnalysisResults();

        Set<FlowAnalysisRuleNode> flowAnalysisRules = flowAnalysisRuleProvider.getAllFlowAnalysisRules();

        flowAnalysisRules.stream()
            .filter(flowAnalysisRuleNode -> FlowAnalysisRuleState.ENABLED.equals(flowAnalysisRuleNode.getState()))
            .forEach(flowAnalysisRuleNode -> {
                try {
                    Collection<FlowAnalysisResult> flowAnalysisResults = flowAnalysisRuleNode.getFlowAnalysisRule().analyzeFlow(
                        flowAnalysisRuleNode.getName(),
                        flowAnalysisRuleNode.getFlowAnalysisContext(),
                        flowDetails,
                        controllerServiceDetailsProvider
                    );

                    result.addResults(flowAnalysisRuleNode.getRuleType(), flowAnalysisResults);
                } catch (Exception e) {
                    logger.error("FlowAnalysis error while running '{}'", flowAnalysisRuleNode.getName(), e);
                }
            });

        updateFlowAnalysisContext(flowDetails.targetIds(), result.getResults());

        Instant end = Instant.now();

        long durationMs = Duration.between(start, end).toMillis();

        logger.debug("Flow Analysis took {} ms", durationMs);

        return result;
    }

    private void updateFlowAnalysisContext(Set<String> affectedComponentIds, Map<FlowAnalysisRuleType, Set<FlowAnalysisResult>> ruleTypeToAnalysisResults) {
        flowAnalysisContext.getIdToRuleNameToRuleViolations()
            .values()
            .stream()
            .map(Map::values)
            .flatMap(Collection::stream)
            .filter(ruleViolation -> affectedComponentIds.contains(ruleViolation.getSubjectId()))
            .forEach(ruleViolation -> ruleViolation.setAvailable(false));

        ruleTypeToAnalysisResults
            .forEach((ruleType, analysisResults) -> analysisResults
                .forEach(analysisResult -> flowAnalysisContext
                    .addRuleViolation(analysisResult.getSubjectId(), new RuleViolation(ruleType, analysisResult.getSubjectId(), analysisResult.getRuleName(), analysisResult.getMessage()))
                )
            );

        flowAnalysisContext.getIdToRuleNameToRuleViolations().forEach(
            (__, ruleNameToRuleViolations) -> ruleNameToRuleViolations.entrySet()
                .removeIf(ruleNameAndRuleViolation -> !ruleNameAndRuleViolation.getValue().isAvailable())
        );
        flowAnalysisContext.getIdToRuleNameToRuleViolations().entrySet()
            .removeIf((idAndRuleNameToRuleViolations -> idAndRuleNameToRuleViolations.getValue().isEmpty()));
    }

    @Override
    public void setFlowAnalysisRuleProvider(FlowAnalysisRuleProvider flowAnalysisRuleProvider) {
        this.flowAnalysisRuleProvider = flowAnalysisRuleProvider;
    }

    @Override
    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void setControllerServiceProvider(ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    public void setFlowAnalysisContext(FlowAnalysisContext flowAnalysisContext) {
        this.flowAnalysisContext = flowAnalysisContext;
    }
}
