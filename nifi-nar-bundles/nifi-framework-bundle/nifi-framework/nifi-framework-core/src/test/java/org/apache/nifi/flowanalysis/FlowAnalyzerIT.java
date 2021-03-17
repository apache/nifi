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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.cs.CounterControllerService;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlowAnalyzerIT extends AbstractFlowAnalysisIT {
    public static final String EXPLANATION_PREFIX = "explanation_";

    private ExecutorService executorService;
    private StandardFlowAnalyzer standardFlowAnalyzer;

    private AtomicReference<CountDownLatch> startRunAnalysis = new AtomicReference<>();
    private AtomicReference<CountDownLatch> finishAnalysis = new AtomicReference<>();

    private AtomicReference<RuntimeException> groupAnalysisError = new AtomicReference<>();

    @BeforeEach
    public void setUpFlowAnalyzerIT() throws Exception {
        // By default, we don't wait
        startRunAnalysis.set(new CountDownLatch(0));
        finishAnalysis.set(new CountDownLatch(0));

        executorService = Executors.newSingleThreadExecutor();
        standardFlowAnalyzer = new StandardFlowAnalyzer(
                getFlowController().getFlowManager().getRuleViolationsManager(),
                getFlowController(),
                getExtensionManager()
        ) {
            @Override
            public void analyzeProcessGroup(VersionedProcessGroup processGroup) {
                if (groupAnalysisError.get() != null) {
                    throw groupAnalysisError.get();
                }

                super.analyzeProcessGroup(processGroup);

                try {
                    finishAnalysis.get().await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        };
        standardFlowAnalyzer.initialize(getFlowController().getControllerServiceProvider());
    }

    @AfterEach
    public void tearDown() throws Exception {
        executorService.shutdown();
    }

    @Test
    public void testAnalyzeProcessorNoRule() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        Collection<RuleViolation> expected = new HashSet<>();

        // WHEN
        // THEN
        testAnalyzeProcessor(processorNode, expected);

    }

    @Test
    public void testAnalyzeProcessorNoViolation() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
        });

        Collection<RuleViolation> expected = new HashSet<>();

        // WHEN
        // THEN
        testAnalyzeProcessor(processorNode, expected);
    }

    @Test
    public void testAnalyzeProcessorDisableRuleBeforeAnalysis() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(
                analyzingComponent("disappearing_issueId", "Violation removed when rule is disabled")
        );
        flowAnalysisRuleNode.disable();

        Collection<RuleViolation> expected = new HashSet<>();

        // WHEN
        // THEN
        testAnalyzeProcessor(processorNode, expected);
    }

    @Test
    public void testAnalyzeProcessorProduceViolation() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(
                analyzingComponent(issueId, violationMessage)
        );

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessor(processorNode, expected);
    }

    @Test
    public void testAnalyzeProcessorProduceMultipleViolations() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId1 = "issueId1";
        String violationMessage1 = "Violation 1";

        String issueId2 = "issueId2";
        String violationMessage2 = "Violation 2";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(
                analyzingComponent(new HashMap<String, String>() {{
                    put(issueId1, violationMessage1);
                    put(issueId2, violationMessage2);
                }})
        );

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId1,
                        violationMessage1,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                ),
                createRuleViolation(
                        issueId2,
                        violationMessage2,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessor(processorNode, expected);
    }

    @Test
    public void testAnalyzeProcessorThenAnalyzeAgainWithDifferentResult() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage1 = "Previous violation gets overwritten";
        String violationMessage2 = "New violation";

        AtomicReference<String> violationMessageHolder = new AtomicReference<>(violationMessage1);

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
                ComponentAnalysisResult result = new ComponentAnalysisResult(
                        issueId,
                        violationMessageHolder.get(),
                        EXPLANATION_PREFIX + violationMessageHolder.get()
                );

                return Collections.singleton(result);
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage2,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        violationMessageHolder.set(violationMessage2);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);
    }


    @Test
    public void testAnalyzeProcessorProduceViolationFirstNoViolationSecond() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "disappearing_issueId";
        String violationMessage = "Violation removed when second analysis doesn't reproduce it";

        AtomicReference<String> violationMessageHolder = new AtomicReference<>(violationMessage);

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
                String violationMessage = violationMessageHolder.get();

                if (violationMessage != null) {
                    ComponentAnalysisResult result = new ComponentAnalysisResult(
                            issueId,
                            violationMessageHolder.get()
                    );

                    return Collections.singleton(result);
                } else {
                    return Collections.emptySet();
                }
            }
        });

        Collection<RuleViolation> expected = new HashSet<>();

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        violationMessageHolder.set(null);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessorDisableRuleAfterAnalysis() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "disappearing_issueId";
        String violationMessage = "Violation removed when rule is disabled";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));

        Collection<RuleViolation> expectedBeforeDisable = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        Collection<RuleViolation> expectedAfterDisable = new HashSet<>();

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expectedBeforeDisable);

        // WHEN
        flowAnalysisRuleNode.disable();
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expectedAfterDisable);
    }

    @Test
    public void testAnalyzeProcessorAndControllerServiceWithSameRuleProduceIndependentViolations() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        ControllerServiceNode controllerServiceNode = createControllerServiceNode(CounterControllerService.class.getName());

        String issueId = "issueId";
        String violationMessage = "Same violation message for both processor node and controller service";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage,
                        flowAnalysisRuleNode,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                ),
                createRuleViolation(
                        issueId,
                        violationMessage,
                        flowAnalysisRuleNode,
                        controllerServiceNode.getIdentifier(),
                        controllerServiceNode.getIdentifier(),
                        controllerServiceNode.getName(),
                        controllerServiceNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        standardFlowAnalyzer.analyzeControllerService(controllerServiceNode);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessGroupDisableRuleBeforeAnalysis() throws Exception {
        // GIVEN
        String issueId = "disappearing_issueId";
        String violationMessage = "Violation removed when rule is disabled";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(analyzingProcessGroup(issueId, violationMessage));

        Collection<RuleViolation> expected = new HashSet<>();

        // WHEN
        // THEN
        rule.disable();
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupProduceViolation() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String violationMessage = "Violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(analyzingProcessGroup(issueId, violationMessage));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage,
                        rule,
                        processGroup.getIdentifier(),
                        processGroup.getIdentifier(),
                        processGroup.getName(),
                        processGroup.getIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupWithProcessor() throws Exception {
        // GIVEN
        String groupViolationIssueId = "group_violation_issueId";
        String groupViolationMessage = "Group violation";

        String processorViolationIssueId = "processor_violation_IssueId";
        String processorViolationMessage = "Processor violation";

        ProcessGroup group = createProcessGroup(getRootGroup());

        ProcessorNode processorNode = createProcessorNode(group);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(group);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                        .map(processor -> newResultForComponent(processor, processorViolationIssueId, processorViolationMessage))
                        .forEach(results::add);

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        groupViolationIssueId,
                        groupViolationMessage,
                        rule,
                        group.getIdentifier(),
                        group.getIdentifier(),
                        group.getName(),
                        group.getIdentifier()
                ),
                createRuleViolation(
                        processorViolationIssueId,
                        processorViolationMessage,
                        rule,
                        group.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        group.getIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupWithConnectionThatHasNoName() throws Exception {
        // GIVEN

        String connectionViolationIssueId = "connection_violation_IssueId";
        String connectionViolationMessage = "Connection violation";

        ProcessGroup group = createProcessGroup(getRootGroup());

        ProcessorNode sourceProcessor = createProcessorNode(group);
        ProcessorNode destinationProcessor = createProcessorNode(group);
        Connection connection = createConnection(group, null, sourceProcessor, destinationProcessor, Arrays.asList("success", "other_relationship"));

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(group);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                processGroup.getConnections().stream()
                        .map(connection -> newResultForComponent(connection, connectionViolationIssueId, connectionViolationMessage))
                        .forEach(results::add);

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        connectionViolationIssueId,
                        connectionViolationMessage,
                        rule,
                        group.getIdentifier(),
                        connection.getIdentifier(),
                        sourceProcessor.getName() + " > " + "success,other_relationship",
                        group.getIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupWithConnectionThatHasName() throws Exception {
        // GIVEN

        String connectionViolationIssueId = "connection_violation_IssueId";
        String connectionViolationMessage = "Connection violation";

        ProcessGroup group = createProcessGroup(getRootGroup());

        ProcessorNode sourceProcessor = createProcessorNode(group);
        ProcessorNode destinationProcessor = createProcessorNode(group);
        Connection connection = createConnection(group, "connectionName", sourceProcessor, destinationProcessor, Arrays.asList("success", "other_relationship"));

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(group);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                processGroup.getConnections().stream()
                        .map(connection -> newResultForComponent(connection, connectionViolationIssueId, connectionViolationMessage))
                        .forEach(results::add);

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        connectionViolationIssueId,
                        connectionViolationMessage,
                        rule,
                        group.getIdentifier(),
                        connection.getIdentifier(),
                        connection.getName(),
                        group.getIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupAndProcessorWithDifferentRules() throws Exception {
        // GIVEN
        String groupViolationIssueId = "group_violation_issueId";
        String groupViolationMessage = "Group violation";

        String processorViolationIssueId = "processor_violation_issueId";
        String processorViolationMessage = "Processor violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessorNode processorNode = createProcessorNode(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode processGroupAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingProcessGroup(groupViolationIssueId, groupViolationMessage));
        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(processorViolationIssueId, processorViolationMessage));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        groupViolationIssueId,
                        groupViolationMessage,
                        processGroupAnalyzerRule,
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getName(),
                        versionedProcessGroup.getIdentifier()
                ),
                createRuleViolation(
                        processorViolationIssueId,
                        processorViolationMessage,
                        processorAnalyzerRule,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);

    }

    @Test
    public void testAnalyzeProcessorIndividuallyAndAsPartOfGroup() throws Exception {
        // GIVEN
        String groupViolationIssueId = "group_violation_issueId";
        String groupViolationMessage = "Group violation";

        String processorViolationIssueIdInGroupAnalysis = "processor_inGroupAnalysis_violationIssueId";
        String processorViolationMessageInGroupAnalysis = "Processor violation when analyzed as part of the group";

        String processorViolationIssueIdInComponentAnalysis = "processor_inComponentAnalysis_ViolationMessage";
        String processorViolationMessageInComponentAnalysis = "Processor violation when analyzed as individual component";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        ProcessorNode processorNode = createProcessorNode(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode processGroupAnalyzerRule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                        .map(processor -> newResultForComponent(processor, processorViolationIssueIdInGroupAnalysis, processorViolationMessageInGroupAnalysis))
                        .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
                processorViolationIssueIdInComponentAnalysis,
                processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        groupViolationIssueId,
                        groupViolationMessage,
                        processGroupAnalyzerRule,
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getName(),
                        versionedProcessGroup.getIdentifier()
                ),
                createRuleViolation(
                        processorViolationIssueIdInGroupAnalysis,
                        processorViolationMessageInGroupAnalysis,
                        processGroupAnalyzerRule,
                        versionedProcessGroup.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        versionedProcessGroup.getIdentifier()
                ),
                createRuleViolation(
                        processorViolationIssueIdInComponentAnalysis,
                        processorViolationMessageInComponentAnalysis,
                        processorAnalyzerRule,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN;
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessorIndividuallyAndAsPartOfGroupButThenDisableGroupRule() throws Exception {
        // GIVEN
        String groupViolationIssueId = "disappearing_group_violation_issueId";
        String groupViolationMessage = "Group violation removed after rule is disabled";

        String processorViolationIssueIdInGroupAnalysis = "disappearing_processor_inGroupAnalysis_violationIssueId";
        String processorViolationMessageInGroupAnalysis = "Processor violation when analyzed as part of the group gets remove after rule is disabled";

        String processorViolationIssueIdInComponentAnalysis = "processor_inComponentAnalysis_violationIssueId";
        String processorViolationMessageInComponentAnalysis = "Processor violation when analyzed as individual component";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        ProcessorNode processorNode = createProcessorNode(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode processGroupAnalyzerRule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                        .map(processor -> newResultForComponent(processor, processorViolationIssueIdInGroupAnalysis, processorViolationMessageInGroupAnalysis))
                        .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
                processorViolationIssueIdInComponentAnalysis,
                processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        processorViolationIssueIdInComponentAnalysis,
                        processorViolationMessageInComponentAnalysis,
                        processorAnalyzerRule,
                        processorNode.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processorNode.getProcessGroupIdentifier()
                )
        ));

        // WHEN;
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        processGroupAnalyzerRule.disable();

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessorIndividuallyAndAsPartOfGroupButThenDisableProcessorRule() throws Exception {
        // GIVEN
        String groupViolationIssueId = "group_violation_issueId";
        String groupViolationMessage = "Group violation";

        String processorViolationIssueIdInGroupAnalysis = "processor_inGroupAnalysis_violationIssueId";
        String processorViolationMessageInGroupAnalysis = "Processor violation when analyzed as part of the group";

        String processorViolationIssueIdInComponentAnalysis = "disappearing_processor_inComponentAnalysis_ViolationMessage";
        String processorViolationMessageInComponentAnalysis = "Processor violation when analyzed as individual component removed when rule is disabled";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        ProcessorNode processorNode = createProcessorNode(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode processGroupAnalyzerRule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                        .map(processor -> newResultForComponent(processor, processorViolationIssueIdInGroupAnalysis, processorViolationMessageInGroupAnalysis))
                        .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
                processorViolationIssueIdInComponentAnalysis,
                processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        groupViolationIssueId,
                        groupViolationMessage,
                        processGroupAnalyzerRule,
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getIdentifier(),
                        versionedProcessGroup.getName(),
                        versionedProcessGroup.getIdentifier()
                ),
                createRuleViolation(
                        processorViolationIssueIdInGroupAnalysis,
                        processorViolationMessageInGroupAnalysis,
                        processGroupAnalyzerRule,
                        versionedProcessGroup.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        versionedProcessGroup.getIdentifier()
                )
        ));

        // WHEN;
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        processorAnalyzerRule.disable();

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessGroupWithGrandChildProcessGroupAllContainingProcessors() throws Exception {
        // GIVEN
        String processorViolationIssueId = "processor_violation";
        String processorViolationMessage = "Processor violation";

        String groupViolationIssueId = "group_violation";
        String groupViolationMessage = "Group violation";

        String groupScopedProcessorViolationIssueId = "group_scoped_processor_violation";
        String groupScopedProcessorViolationMessage = "Group scoped processor violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);
        ProcessGroup grandChildProcessGroup = createProcessGroup(childProcessGroup);

        ProcessorNode processorNode = createProcessorNode(processGroup);
        ProcessorNode childProcessorNode = createProcessorNode(childProcessGroup);
        ProcessorNode grandChildProcessorNode = createProcessorNode(grandChildProcessGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
                Collection<ComponentAnalysisResult> results = new HashSet<>();

                results.add(new ComponentAnalysisResult(
                        processorViolationIssueId,
                        processorViolationMessage,
                        EXPLANATION_PREFIX + processorViolationMessage
                ));

                return results;
            }

            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                        .map(processor -> newResultForComponent(processor, groupScopedProcessorViolationIssueId, groupScopedProcessorViolationMessage))
                        .forEach(results::add);

                return results;
            }
        });

        RuleViolation expectedProcessorViolation = createRuleViolation(
                processorViolationIssueId,
                processorViolationMessage,
                rule,
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getName(),
                processGroup.getIdentifier()
        );
        RuleViolation expectedChildProcessorViolation = createRuleViolation(
                processorViolationIssueId,
                processorViolationMessage,
                rule,
                childProcessorNode.getIdentifier(),
                childProcessorNode.getIdentifier(),
                childProcessorNode.getName(),
                childProcessGroup.getIdentifier()
        );
        RuleViolation expectedGrandChildProcessorViolation = createRuleViolation(
                processorViolationIssueId,
                processorViolationMessage,
                rule,
                grandChildProcessorNode.getIdentifier(),
                grandChildProcessorNode.getIdentifier(),
                grandChildProcessorNode.getName(),
                grandChildProcessGroup.getIdentifier()
        );

        RuleViolation expectedGroupScopedProcessorViolation = createRuleViolation(
                groupScopedProcessorViolationIssueId,
                groupScopedProcessorViolationMessage,
                rule,
                processGroup.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getName(),
                processGroup.getIdentifier()
        );
        RuleViolation expectedGroupScopedChildProcessorViolation = createRuleViolation(
                groupScopedProcessorViolationIssueId,
                groupScopedProcessorViolationMessage,
                rule,
                childProcessGroup.getIdentifier(),
                childProcessorNode.getIdentifier(),
                childProcessorNode.getName(),
                childProcessGroup.getIdentifier()
        );
        RuleViolation expectedGroupScopedGrandChildProcessorViolation = createRuleViolation(
                groupScopedProcessorViolationIssueId,
                groupScopedProcessorViolationMessage,
                rule,
                grandChildProcessGroup.getIdentifier(),
                grandChildProcessorNode.getIdentifier(),
                grandChildProcessorNode.getName(),
                grandChildProcessGroup.getIdentifier()
        );
        RuleViolation expectedGroupViolation = createRuleViolation(
                groupViolationIssueId,
                groupViolationMessage,
                rule,
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                processGroup.getName(),
                processGroup.getIdentifier()
        );
        RuleViolation expectedChildGroupViolation = createRuleViolation(
                groupViolationIssueId,
                groupViolationMessage,
                rule,
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getName(),
                childProcessGroup.getIdentifier()
        );
        RuleViolation expectedGrandChildGroupViolation = createRuleViolation(
                groupViolationIssueId,
                groupViolationMessage,
                rule,
                grandChildProcessGroup.getIdentifier(),
                grandChildProcessGroup.getIdentifier(),
                grandChildProcessGroup.getName(),
                grandChildProcessGroup.getIdentifier()
        );

        Collection<RuleViolation> expectedAllViolations = new HashSet<>(Arrays.asList(
                expectedProcessorViolation,
                expectedChildProcessorViolation,
                expectedGrandChildProcessorViolation,

                expectedGroupScopedProcessorViolation,
                expectedGroupScopedChildProcessorViolation,
                expectedGroupScopedGrandChildProcessorViolation,

                expectedGroupViolation,
                expectedChildGroupViolation,
                expectedGrandChildGroupViolation
        ));

        Collection<RuleViolation> expectedAllProcessorViolations = new HashSet<>(Arrays.asList(
                expectedProcessorViolation,
                expectedGroupScopedProcessorViolation
        ));
        Collection<RuleViolation> expectedAllChildProcessorViolations = new HashSet<>(Arrays.asList(
                expectedChildProcessorViolation,
                expectedGroupScopedChildProcessorViolation
        ));
        Collection<RuleViolation> expectedAllGrandChildProcessorViolations = new HashSet<>(Arrays.asList(
                expectedGrandChildProcessorViolation,
                expectedGroupScopedGrandChildProcessorViolation
        ));

        Collection<RuleViolation> expectedAllGroupViolations = new HashSet<>(Arrays.asList(
                expectedProcessorViolation,
                expectedGroupScopedProcessorViolation,
                expectedGroupViolation
        ));
        Collection<RuleViolation> expectedAllChildGroupViolations = new HashSet<>(Arrays.asList(
                expectedChildProcessorViolation,
                expectedGroupScopedChildProcessorViolation,
                expectedChildGroupViolation
        ));
        Collection<RuleViolation> expectedAllGrandChildGroupViolations = new HashSet<>(Arrays.asList(
                expectedGrandChildProcessorViolation,
                expectedGroupScopedGrandChildProcessorViolation,
                expectedGrandChildGroupViolation
        ));

        // WHEN;
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expectedAllViolations);


        Collection<RuleViolation> actualAllProcessorViolations = getRuleViolationsManager().getRuleViolationsForSubject(processorNode.getIdentifier());
        assertEquals(expectedAllProcessorViolations, actualAllProcessorViolations);

        Collection<RuleViolation> actualAllChildProcessorViolations = getRuleViolationsManager().getRuleViolationsForSubject(childProcessorNode.getIdentifier());
        assertEquals(expectedAllChildProcessorViolations, actualAllChildProcessorViolations);

        Collection<RuleViolation> actualAllGrandChildProcessorViolations = getRuleViolationsManager().getRuleViolationsForSubject(grandChildProcessorNode.getIdentifier());
        assertEquals(expectedAllGrandChildProcessorViolations, actualAllGrandChildProcessorViolations);


        Collection<RuleViolation> actualAllGroupViolations = getRuleViolationsManager().getRuleViolationsForGroup(processGroup.getIdentifier());
        assertEquals(expectedAllGroupViolations, actualAllGroupViolations);

        Collection<RuleViolation> actualAllChildGroupViolations = getRuleViolationsManager().getRuleViolationsForGroup(childProcessGroup.getIdentifier());
        assertEquals(expectedAllChildGroupViolations, actualAllChildGroupViolations);

        Collection<RuleViolation> actualAllGrandChildGroupViolations = getRuleViolationsManager().getRuleViolationsForGroup(grandChildProcessGroup.getIdentifier());
        assertEquals(expectedAllGrandChildGroupViolations, actualAllGrandChildGroupViolations);
    }

    @Test
    public void testAnalyzeProcessGroupProduceViolationThenChildProcessGroupProduceNoViolation() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String processorViolationMessage = "Violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);

        ProcessorNode processorNode = createProcessorNode(processGroup);
        ProcessorNode childProcessorNode = createProcessorNode(childProcessGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);
        VersionedProcessGroup versionedChildProcessGroup = mapProcessGroup(childProcessGroup);

        AtomicBoolean produceViolation = new AtomicBoolean(true);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                if (produceViolation.get()) {
                    processGroup.getProcessors().stream()
                            .map(processor -> newResultForComponent(processor, issueId, processorViolationMessage))
                            .forEach(results::add);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        processorViolationMessage,
                        rule,
                        processGroup.getIdentifier(),
                        processorNode.getIdentifier(),
                        processorNode.getName(),
                        processGroup.getIdentifier()
                )
        ));

        // WHEN;
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        produceViolation.set(false);
        standardFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessGroupWhereChildGroupProducesViolation() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String violationMessage = "Violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                if (processGroup.getIdentifier().equals(childProcessGroup.getIdentifier())) {
                    GroupAnalysisResult result = newResultForGroup(issueId, violationMessage);

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage,
                        rule,
                        childProcessGroup.getIdentifier(),
                        childProcessGroup.getIdentifier(),
                        childProcessGroup.getName(),
                        childProcessGroup.getIdentifier()
                )
        ));

        // WHEN
        // THEN
        testAnalyzeProcessGroup(versionedProcessGroup, expected);
    }

    @Test
    public void testAnalyzeProcessGroupNewParentAnalysisCanClearPreviousChildAnalysis() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String violationMessage = "Violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);
        VersionedProcessGroup versionedChildProcessGroup = mapProcessGroup(childProcessGroup);

        AtomicBoolean produceChildViolation = new AtomicBoolean(true);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                if (produceChildViolation.get() && processGroup.getIdentifier().equals(childProcessGroup.getIdentifier())) {
                    GroupAnalysisResult result = newResultForGroup(issueId, violationMessage);

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = Collections.emptySet();

        // WHEN
        standardFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);
        produceChildViolation.set(false);
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testAnalyzeProcessGroupNewParentAnalysisOverridesPreviousChildAnalysis() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String violationMessage1 = "Previous violation gets overwritten";
        String violationMessage2 = "New violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);
        VersionedProcessGroup versionedChildProcessGroup = mapProcessGroup(childProcessGroup);

        AtomicReference<String> violationMessageWrapper = new AtomicReference<>();

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                if (processGroup.getIdentifier().equals(childProcessGroup.getIdentifier())) {
                    GroupAnalysisResult result = newResultForGroup(issueId, violationMessageWrapper.get());

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                createRuleViolation(
                        issueId,
                        violationMessage2,
                        rule,
                        childProcessGroup.getIdentifier(),
                        childProcessGroup.getIdentifier(),
                        childProcessGroup.getName(),
                        childProcessGroup.getIdentifier()
                )
        ));

        // WHEN
        violationMessageWrapper.set(violationMessage1);
        standardFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);
        violationMessageWrapper.set(violationMessage2);
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expected);
    }

    @Test
    public void testRecommendationDoesNotInvalidateComponent() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setEnforcementPolicy(EnforcementPolicy.WARN);

        Collection<ValidationResult> expected = Collections.emptyList();

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        // THEN
        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testPolicyInvalidatesProcessor() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setEnforcementPolicy(EnforcementPolicy.ENFORCE);

        Collection<ValidationResult> expected = Arrays.asList(
                new ValidationResult.Builder()
                        .subject(processorNode.getComponent().getClass().getSimpleName())
                        .valid(false)
                        .explanation(violationMessage)
                        .build()
        );

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        // THEN
        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testPolicyInvalidatesControllerService() throws Exception {
        // GIVEN
        ControllerServiceNode controllerServiceNode = createControllerServiceNode(CounterControllerService.class.getName());

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setEnforcementPolicy(EnforcementPolicy.ENFORCE);

        Collection<ValidationResult> expected = Arrays.asList(
                new ValidationResult.Builder()
                        .subject(controllerServiceNode.getComponent().getClass().getSimpleName())
                        .valid(false)
                        .explanation(violationMessage)
                        .build()
        );

        // WHEN
        standardFlowAnalyzer.analyzeControllerService(controllerServiceNode);
        controllerServiceNode.performValidation();

        // THEN
        Collection<ValidationResult> actual = controllerServiceNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testChangingPolicyToRecommendationRemovesValidationError() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setEnforcementPolicy(EnforcementPolicy.ENFORCE);

        Collection<ValidationResult> expected = Arrays.asList();

        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        assertEquals(ValidationStatus.INVALID, processorNode.getValidationStatus());

        flowAnalysisRuleNode.setEnforcementPolicy(EnforcementPolicy.WARN);

        standardFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());
        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    private AbstractFlowAnalysisRule analyzingComponent(String issueId, String violationMessage) {
        AbstractFlowAnalysisRule rule = analyzingComponent(new HashMap<String, String>() {{
            put(issueId, violationMessage);
        }});

        return rule;
    }

    private AbstractFlowAnalysisRule analyzingComponent(HashMap<String, String> issueIdToViolationMessage) {
        AbstractFlowAnalysisRule rule = new AbstractFlowAnalysisRule() {
            @Override
            public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
                Set<ComponentAnalysisResult> results = issueIdToViolationMessage.entrySet().stream()
                        .map(issueIdAndViolationMessage -> new ComponentAnalysisResult(
                                issueIdAndViolationMessage.getKey(),
                                issueIdAndViolationMessage.getValue(),
                                EXPLANATION_PREFIX + issueIdAndViolationMessage.getValue())
                        )
                        .collect(Collectors.toSet());

                return results;
            }
        };

        return rule;
    }

    private AbstractFlowAnalysisRule analyzingProcessGroup(String issueId, String violationMessage) {
        return new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                GroupAnalysisResult result = newResultForGroup(issueId, violationMessage);

                return Collections.singleton(result);
            }
        };
    }

    private GroupAnalysisResult newResultForGroup(String issueId, String violationMessage) {
        return GroupAnalysisResult.forGroup(issueId, violationMessage)
                .explanation(EXPLANATION_PREFIX + violationMessage)
                .build();
    }

    private GroupAnalysisResult newResultForComponent(VersionedComponent component, String issueId, String violationMessage) {
        return GroupAnalysisResult.forComponent(component, issueId, violationMessage)
                .explanation(EXPLANATION_PREFIX + violationMessage)
                .build();
    }

    private void testAnalyzeProcessor(ProcessorNode processorNode, Collection<RuleViolation> expected) {
        // WHEN
        standardFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);
    }

    private void testAnalyzeProcessGroup(VersionedProcessGroup versionedProcessGroup, Collection<RuleViolation> expected) {
        // WHEN
        standardFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expected);
    }

    private void checkActualViolations(Collection<RuleViolation> expected) {
        Collection<RuleViolation> actual = getRuleViolationsManager().getAllRuleViolations();

        assertEquals(expected, actual);
    }

    private RuleViolation createRuleViolation(
            String issueId,
            String processorViolationMessage,
            FlowAnalysisRuleNode rule,
            String scope,
            String subjectId,
            String subjectDisplayName,
            String groupId
    ) {
        return new RuleViolation(
                rule.getEnforcementPolicy(),
                scope,
                subjectId,
                subjectDisplayName,
                groupId,
                rule.getIdentifier(),
                issueId,
                processorViolationMessage,
                "explanation_" + processorViolationMessage
        );
    }

    private RuleViolationsManager getRuleViolationsManager() {
        return getFlowController().getFlowManager().getRuleViolationsManager();
    }
}
