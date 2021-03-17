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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flowanalysis.AnalyzeFlowRequest;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleProvider;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AnalyzeFlowState;
import org.apache.nifi.flowanalysis.AnalyzeFlowStatus;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.validation.FlowAnalysisContext;
import org.apache.nifi.validation.RuleViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link FlowAnalyzer} that uses {@link org.apache.nifi.flowanalysis.FlowAnalysisRule FlowAnalysisRules}.
 */
public class MainFlowAnalyzer implements FlowAnalyzer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ConcurrentMap<String, AnalyzeFlowRequest> groupIdToRequest = new ConcurrentHashMap<>();

    private int nrOfThreads;
    private FlowAnalysisContext flowAnalysisContext;

    private ExecutorService executorService;

    private FlowAnalysisRuleProvider flowAnalysisRuleProvider;
    private ExtensionManager extensionManager;
    private ControllerServiceProvider controllerServiceProvider;

    @PostConstruct
    public void init() {
        ThreadFactory flowAnalyzerThreadFactory = new ThreadFactoryBuilder().setNameFormat("flow-analyzer-%d").build();
        executorService = Executors.newFixedThreadPool(nrOfThreads, flowAnalyzerThreadFactory);
    }

    @PreDestroy
    public void cleanUp() {
        executorService.shutdown();
    }

    @Override
    public void analyzeProcessor(ProcessorNode processorNode) {
        logger.debug("Running analysis on {}", processorNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        VersionedProcessor versionedProcessor = mapper.mapProcessor(
            processorNode,
            controllerServiceProvider,
            Collections.emptySet(),
            new HashMap<>()
        );

        analyzeComponent(versionedProcessor);
    }

    @Override
    public void analyzeControllerService(ControllerServiceNode controllerServiceNode) {
        logger.debug("Running analysis on {}", controllerServiceNode);

        final NiFiRegistryFlowMapper mapper = createMapper();

        VersionedControllerService versionedControllerService = mapper.mapControllerService(
            controllerServiceNode,
            controllerServiceProvider,
            Collections.emptySet(),
            new HashMap<>()
        );

        analyzeComponent(versionedControllerService);
    }

    private void analyzeComponent(VersionedComponent component) {
        Instant start = Instant.now();

        String componentId = component.getIdentifier();
        Set<FlowAnalysisRuleNode> flowAnalysisRules = flowAnalysisRuleProvider.getAllFlowAnalysisRules();

        Set<RuleViolation> violations = flowAnalysisRules.stream()
            .filter(FlowAnalysisRuleNode::isEnabled)
            .flatMap(flowAnalysisRuleNode -> {
                String ruleId = flowAnalysisRuleNode.getIdentifier();

                try {
                    Collection<ComponentAnalysisResult> analysisResults = flowAnalysisRuleNode
                        .getFlowAnalysisRule()
                        .analyzeComponent(component, flowAnalysisRuleNode.getFlowAnalysisRuleContext());

                    return analysisResults.stream()
                        .map(analysisResult -> new RuleViolation(
                            flowAnalysisRuleNode.getRuleType(),
                            componentId,
                            componentId,
                            component.getGroupIdentifier(),
                            ruleId,
                            analysisResult.getIssueId(),
                            analysisResult.getMessage()
                        ));
                } catch (Exception e) {
                    logger.error("FlowAnalysis error while running '{}' against '{}'", flowAnalysisRuleNode.getName(), component, e);
                    return Stream.empty();
                }
            })
            .collect(Collectors.toSet());

        flowAnalysisContext.upsertComponentViolations(componentId, violations);

        Instant end = Instant.now();

        long durationMs = Duration.between(start, end).toMillis();

        logger.debug("Flow Analysis took {} ms", durationMs);
    }

    @Override
    public AnalyzeFlowStatus createAnalyzeFlowRequest(VersionedProcessGroup processGroup) {
        String groupId = processGroup.getIdentifier();

        AtomicBoolean isNewRequest = new AtomicBoolean(false);
        AnalyzeFlowRequest request = groupIdToRequest.compute(groupId, (__, currentRequest) -> {
            if (currentRequest == null || currentRequest.getState().isFinished()) {
                isNewRequest.set(true);

                AnalyzeFlowRequest newRequest = new AnalyzeFlowRequest(groupId);

                return newRequest;
            } else {
                return currentRequest;
            }
        });

        if (isNewRequest.get()) {
            executorService.submit(() -> runProcessGroupAnalysis(processGroup, request));
        }

        return request;
    }

    protected void runProcessGroupAnalysis(VersionedProcessGroup processGroup, AnalyzeFlowRequest request) {
        try {
            if (request.getState() == AnalyzeFlowState.CANCELED) {
                return;
            }

            request.setState(AnalyzeFlowState.ANALYZING);

            analyzeProcessGroup(processGroup);

            request.setState(AnalyzeFlowState.COMPLETE);
        } catch (Exception e) {
            request.setState(AnalyzeFlowState.FAILURE, "Flow Analysis of process group '" + processGroup.getIdentifier() + "' failed due to " + e);
        }
    }

    @Override
    public AnalyzeFlowStatus getAnalyzeFlowRequest(String processGroupId) {
        AnalyzeFlowRequest request = groupIdToRequest.get(processGroupId);

        return request;
    }

    @Override
    public AnalyzeFlowStatus cancelAnalyzeFlowRequest(String processGroupId) {
        AnalyzeFlowRequest request = groupIdToRequest.get(processGroupId);

        if (request != null) {
            request.cancel();
        }

        return request;
    }

    @Override
    public void analyzeProcessGroup(VersionedProcessGroup processGroup) {
        logger.debug("Running analysis on process group {}.", processGroup.getIdentifier());

        Instant start = Instant.now();

        Set<FlowAnalysisRuleNode> flowAnalysisRules = flowAnalysisRuleProvider.getAllFlowAnalysisRules();

        Collection<RuleViolation> groupViolations = new HashSet<>();
        Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations = new HashMap<>();

        analyzeProcessGroup(processGroup, flowAnalysisRules, groupViolations, componentToRuleViolations);

        flowAnalysisContext.upsertGroupViolations(processGroup, groupViolations, componentToRuleViolations);

        Instant end = Instant.now();

        long durationMs = Duration.between(start, end).toMillis();

        logger.debug("Flow Analysis took {} ms", durationMs);
    }

    private void analyzeProcessGroup(
        VersionedProcessGroup processGroup,
        Set<FlowAnalysisRuleNode> flowAnalysisRules,
        Collection<RuleViolation> groupViolations,
        Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations
    ) {
        String groupId = processGroup.getIdentifier();

        flowAnalysisRules.stream()
            .filter(FlowAnalysisRuleNode::isEnabled)
            .forEach(flowAnalysisRuleNode -> {
                String ruleId = flowAnalysisRuleNode.getIdentifier();

                try {
                    Collection<GroupAnalysisResult> analysisResults = flowAnalysisRuleNode.getFlowAnalysisRule().analyzeProcessGroup(
                        processGroup,
                        flowAnalysisRuleNode.getFlowAnalysisRuleContext()
                    );

                    analysisResults.forEach(analysisResult -> {
                        Optional<VersionedComponent> componentOptional = analysisResult.getComponent();

                        if (componentOptional.isPresent()) {
                            VersionedComponent component = componentOptional.get();

                            componentToRuleViolations.computeIfAbsent(component, __ -> new HashSet<>())
                                .add(new RuleViolation(
                                        flowAnalysisRuleNode.getRuleType(),
                                        component.getGroupIdentifier(),
                                        component.getIdentifier(),
                                        component.getGroupIdentifier(),
                                        ruleId,
                                        analysisResult.getIssueId(),
                                        analysisResult.getMessage()
                                    )

                                );

                        } else {
                            groupViolations.add(new RuleViolation(
                                flowAnalysisRuleNode.getRuleType(),
                                groupId,
                                groupId,
                                groupId,
                                ruleId,
                                analysisResult.getIssueId(),
                                analysisResult.getMessage()
                            ));

                        }
                    });
                } catch (Exception e) {
                    logger.error("FlowAnalysis error while running '{}' against group '{}'", flowAnalysisRuleNode.getName(), groupId, e);
                }
            });

        processGroup.getProcessors().forEach(processor -> analyzeComponent(processor));
        processGroup.getControllerServices().forEach(controllerService -> analyzeComponent(controllerService));

        processGroup.getProcessGroups().forEach(childProcessGroup -> analyzeProcessGroup(childProcessGroup, flowAnalysisRules, groupViolations, componentToRuleViolations));
    }

    private NiFiRegistryFlowMapper createMapper() {
        NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(extensionManager, Function.identity());

        return mapper;
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

    public void setNrOfThreads(int nrOfThreads) {
        this.nrOfThreads = nrOfThreads;
    }

    public void setFlowAnalysisContext(FlowAnalysisContext flowAnalysisContext) {
        this.flowAnalysisContext = flowAnalysisContext;
    }
}
