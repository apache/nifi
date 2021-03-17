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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flowanalysis.AnalyzeFlowRequest;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.AnalyzeFlowState;
import org.apache.nifi.flowanalysis.AnalyzeFlowStatus;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleType;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.cs.CounterControllerService;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.validation.RuleViolation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class FlowAnalyzerIT extends AbstractFlowAnalysisIT {
    private MainFlowAnalyzer mainFlowAnalyzer;

    private AtomicReference<CountDownLatch> startRunAnalysis = new AtomicReference<>();
    private AtomicReference<CountDownLatch> finishAnalysis = new AtomicReference<>();

    private CountDownLatch analysisStarted = new CountDownLatch(1);
    private CountDownLatch analysisRunFinished = new CountDownLatch(1);

    private AtomicReference<RuntimeException> groupAnalysisError = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        // By default we don't wait
        startRunAnalysis.set(new CountDownLatch(0));
        finishAnalysis.set(new CountDownLatch(0));

        mainFlowAnalyzer = new MainFlowAnalyzer() {
            @Override
            protected void runProcessGroupAnalysis(VersionedProcessGroup processGroup, AnalyzeFlowRequest request) {
                try {
                    startRunAnalysis.get().await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                super.runProcessGroupAnalysis(processGroup, request);

                analysisRunFinished.countDown();
            }

            @Override
            public void analyzeProcessGroup(VersionedProcessGroup processGroup) {
                analysisStarted.countDown();

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

        mainFlowAnalyzer.setFlowAnalysisRuleProvider(getFlowController());
        mainFlowAnalyzer.setExtensionManager(getExtensionManager());
        mainFlowAnalyzer.setControllerServiceProvider(getFlowController().getControllerServiceProvider());
        mainFlowAnalyzer.setFlowAnalysisContext(flowAnalysisContext);

        mainFlowAnalyzer.setNrOfThreads(1);
        mainFlowAnalyzer.init();
    }

    @After
    public void tearDown() throws Exception {
        mainFlowAnalyzer.cleanUp();
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
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId,
                violationMessage
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
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId1,
                violationMessage1
            ),
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId2,
                violationMessage2
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
                ComponentAnalysisResult result = ComponentAnalysisResult.newResult(
                    issueId,
                    violationMessageHolder.get()
                );

                return Collections.singleton(result);
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId,
                violationMessage2
            )
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        violationMessageHolder.set(violationMessage2);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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
                    ComponentAnalysisResult result = ComponentAnalysisResult.newResult(
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
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        violationMessageHolder.set(null);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId,
                violationMessage
            )
        ));

        Collection<RuleViolation> expectedAfterDisable = new HashSet<>();

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expectedBeforeDisable);

        // WHEN
        flowAnalysisRuleNode.disable();
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId,
                violationMessage
            ),
            new RuleViolation(
                flowAnalysisRuleNode.getRuleType(),
                controllerServiceNode.getIdentifier(),
                controllerServiceNode.getIdentifier(),
                controllerServiceNode.getProcessGroupIdentifier(),
                flowAnalysisRuleNode.getIdentifier(),
                issueId,
                violationMessage
            )
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        mainFlowAnalyzer.analyzeControllerService(controllerServiceNode);

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
            new RuleViolation(
                rule.getRuleType(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
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

                results.add(GroupAnalysisResult.newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(
                        processor,
                        processorViolationIssueId,
                        processorViolationMessage
                    ))
                    .forEach(results::add);

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                rule.getRuleType(),
                group.getIdentifier(),
                group.getIdentifier(),
                group.getIdentifier(),
                rule.getIdentifier(),
                groupViolationIssueId,
                groupViolationMessage
            ),
            new RuleViolation(
                rule.getRuleType(),
                group.getIdentifier(),
                processorNode.getIdentifier(),
                group.getIdentifier(),
                rule.getIdentifier(),
                processorViolationIssueId,
                processorViolationMessage
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
            new RuleViolation(
                processGroupAnalyzerRule.getRuleType(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                processGroupAnalyzerRule.getIdentifier(),
                groupViolationIssueId,
                groupViolationMessage
            ),
            new RuleViolation(
                processorAnalyzerRule.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                processorAnalyzerRule.getIdentifier(),
                processorViolationIssueId,
                processorViolationMessage
            )
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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

                results.add(GroupAnalysisResult.newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(
                        processor,
                        processorViolationIssueIdInGroupAnalysis,
                        processorViolationMessageInGroupAnalysis
                    ))
                    .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
            processorViolationIssueIdInComponentAnalysis,
            processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                processGroupAnalyzerRule.getRuleType(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                processGroupAnalyzerRule.getIdentifier(),
                groupViolationIssueId,
                groupViolationMessage
            ),
            new RuleViolation(
                processGroupAnalyzerRule.getRuleType(),
                versionedProcessGroup.getIdentifier(),
                processorNode.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                processGroupAnalyzerRule.getIdentifier(),
                processorViolationIssueIdInGroupAnalysis,
                processorViolationMessageInGroupAnalysis
            ),
            new RuleViolation(
                processorAnalyzerRule.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                processorAnalyzerRule.getIdentifier(),
                processorViolationIssueIdInComponentAnalysis,
                processorViolationMessageInComponentAnalysis
            )
        ));

        // WHEN;
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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

                results.add(GroupAnalysisResult.newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(
                        processor,
                        processorViolationIssueIdInGroupAnalysis,
                        processorViolationMessageInGroupAnalysis)
                    )
                    .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
            processorViolationIssueIdInComponentAnalysis,
            processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                processorAnalyzerRule.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                processorAnalyzerRule.getIdentifier(),
                processorViolationIssueIdInComponentAnalysis,
                processorViolationMessageInComponentAnalysis
            )
        ));

        // WHEN;
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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

                results.add(GroupAnalysisResult.newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(
                        processor,
                        processorViolationIssueIdInGroupAnalysis,
                        processorViolationMessageInGroupAnalysis
                    ))
                    .forEach(results::add);

                return results;
            }
        });

        FlowAnalysisRuleNode processorAnalyzerRule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(
            processorViolationIssueIdInComponentAnalysis,
            processorViolationMessageInComponentAnalysis
        ));

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                processGroupAnalyzerRule.getRuleType(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                processGroupAnalyzerRule.getIdentifier(),
                groupViolationIssueId,
                groupViolationMessage
            ),
            new RuleViolation(
                processGroupAnalyzerRule.getRuleType(),
                versionedProcessGroup.getIdentifier(),
                processorNode.getIdentifier(),
                versionedProcessGroup.getIdentifier(),
                processGroupAnalyzerRule.getIdentifier(),
                processorViolationIssueIdInGroupAnalysis,
                processorViolationMessageInGroupAnalysis
            )
        ));

        // WHEN;
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        mainFlowAnalyzer.analyzeProcessor(processorNode);

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

                results.add(ComponentAnalysisResult.newResult(processorViolationIssueId, processorViolationMessage));

                return results;
            }

            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(GroupAnalysisResult.newResultForGroup(groupViolationIssueId, groupViolationMessage));

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(processor, groupScopedProcessorViolationIssueId, groupScopedProcessorViolationMessage))
                    .forEach(results::add);

                return results;
            }
        });

        RuleViolation expectedProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            processorNode.getIdentifier(),
            processorNode.getIdentifier(),
            processGroup.getIdentifier(),
            rule.getIdentifier(),
            processorViolationIssueId,
            processorViolationMessage
        );
        RuleViolation expectedChildProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            childProcessorNode.getIdentifier(),
            childProcessorNode.getIdentifier(),
            childProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            processorViolationIssueId,
            processorViolationMessage
        );
        RuleViolation expectedGrandChildProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            grandChildProcessorNode.getIdentifier(),
            grandChildProcessorNode.getIdentifier(),
            grandChildProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            processorViolationIssueId,
            processorViolationMessage
        );

        RuleViolation expectedGroupScopedProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            processGroup.getIdentifier(),
            processorNode.getIdentifier(),
            processGroup.getIdentifier(),
            rule.getIdentifier(),
            groupScopedProcessorViolationIssueId,
            groupScopedProcessorViolationMessage
        );
        RuleViolation expectedGroupScopedChildProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            childProcessGroup.getIdentifier(),
            childProcessorNode.getIdentifier(),
            childProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            groupScopedProcessorViolationIssueId,
            groupScopedProcessorViolationMessage
        );
        RuleViolation expectedGroupScopedGrandChildProcessorViolation = new RuleViolation(
            rule.getRuleType(),
            grandChildProcessGroup.getIdentifier(),
            grandChildProcessorNode.getIdentifier(),
            grandChildProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            groupScopedProcessorViolationIssueId,
            groupScopedProcessorViolationMessage
        );
        RuleViolation expectedGroupViolation = new RuleViolation(
            rule.getRuleType(),
            processGroup.getIdentifier(),
            processGroup.getIdentifier(),
            processGroup.getIdentifier(),
            rule.getIdentifier(),
            groupViolationIssueId,
            groupViolationMessage
        );
        RuleViolation expectedChildGroupViolation = new RuleViolation(
            rule.getRuleType(),
            childProcessGroup.getIdentifier(),
            childProcessGroup.getIdentifier(),
            childProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            groupViolationIssueId,
            groupViolationMessage
        );
        RuleViolation expectedGrandChildGroupViolation = new RuleViolation(
            rule.getRuleType(),
            grandChildProcessGroup.getIdentifier(),
            grandChildProcessGroup.getIdentifier(),
            grandChildProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            groupViolationIssueId,
            groupViolationMessage
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
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expectedAllViolations);


        Collection<RuleViolation> actualAllProcessorViolations = flowAnalysisContext.getRuleViolationsForSubject(processorNode.getIdentifier());
        assertEquals(expectedAllProcessorViolations, actualAllProcessorViolations);

        Collection<RuleViolation> actualAllChildProcessorViolations = flowAnalysisContext.getRuleViolationsForSubject(childProcessorNode.getIdentifier());
        assertEquals(expectedAllChildProcessorViolations, actualAllChildProcessorViolations);

        Collection<RuleViolation> actualAllGrandChildProcessorViolations = flowAnalysisContext.getRuleViolationsForSubject(grandChildProcessorNode.getIdentifier());
        assertEquals(expectedAllGrandChildProcessorViolations, actualAllGrandChildProcessorViolations);


        Collection<RuleViolation> actualAllGroupViolations = flowAnalysisContext.getRuleViolationsForGroup(processGroup.getIdentifier());
        assertEquals(expectedAllGroupViolations, actualAllGroupViolations);

        Collection<RuleViolation> actualAllChildGroupViolations = flowAnalysisContext.getRuleViolationsForGroup(childProcessGroup.getIdentifier());
        assertEquals(expectedAllChildGroupViolations, actualAllChildGroupViolations);

        Collection<RuleViolation> actualAllGrandChildGroupViolations = flowAnalysisContext.getRuleViolationsForGroup(grandChildProcessGroup.getIdentifier());
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
                        .map(processor -> GroupAnalysisResult.newResultForComponent(processor, issueId, processorViolationMessage))
                        .forEach(results::add);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                rule.getRuleType(),
                processGroup.getIdentifier(),
                processorNode.getIdentifier(),
                processGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                processorViolationMessage
            )
        ));

        // WHEN;
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);
        produceViolation.set(false);
        mainFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);

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
                    GroupAnalysisResult result = GroupAnalysisResult.newResultForGroup(issueId, violationMessage);

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                rule.getRuleType(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
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
                    GroupAnalysisResult result = GroupAnalysisResult.newResultForGroup(issueId, violationMessage);

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = Collections.emptySet();

        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);
        produceChildViolation.set(false);
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

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
                    GroupAnalysisResult result = GroupAnalysisResult.newResultForGroup(issueId, violationMessageWrapper.get());

                    results.add(result);
                }

                return results;
            }
        });

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
            new RuleViolation(
                rule.getRuleType(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage2
            )
        ));

        // WHEN
        violationMessageWrapper.set(violationMessage1);
        mainFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);
        violationMessageWrapper.set(violationMessage2);
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

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
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.RECOMMENDATION);

        Collection<ValidationResult> expected = Collections.emptyList();

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
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
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<ValidationResult> expected = Arrays.asList(
            new ValidationResult.Builder()
                .subject(processorNode.getComponent().getClass().getSimpleName())
                .valid(false)
                .explanation(violationMessage)
                .build()
        );

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
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
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<ValidationResult> expected = Arrays.asList(
            new ValidationResult.Builder()
                .subject(controllerServiceNode.getComponent().getClass().getSimpleName())
                .valid(false)
                .explanation(violationMessage)
                .build()
        );

        // WHEN
        mainFlowAnalyzer.analyzeControllerService(controllerServiceNode);
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
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<ValidationResult> expected = Arrays.asList();

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        assertEquals(ValidationStatus.INVALID, processorNode.getValidationStatus());

        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.RECOMMENDATION);

        mainFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());
        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testDisablingRuleViolationRemovesValidationError() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<ValidationResult> expected = Arrays.asList();

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        assertEquals(ValidationStatus.INVALID, processorNode.getValidationStatus());

        flowAnalysisContext.updateRuleViolation(
            processorNode.getIdentifier(),
            processorNode.getIdentifier(),
            flowAnalysisRuleNode.getIdentifier(),
            issueId,
            false
        );

        mainFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());
        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testReEnablingRuleViolationProducesValidationError() throws Exception {
        // GIVEN
        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        String issueId = "issueId";
        String violationMessage = "Violation";

        FlowAnalysisRuleNode flowAnalysisRuleNode = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        flowAnalysisRuleNode.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<ValidationResult> expected = Arrays.asList(
            new ValidationResult.Builder()
                .subject(processorNode.getComponent().getClass().getSimpleName())
                .valid(false)
                .explanation(violationMessage)
                .build()
        );

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);
        processorNode.performValidation();

        assertEquals(ValidationStatus.INVALID, processorNode.getValidationStatus());

        flowAnalysisContext.updateRuleViolation(
            processorNode.getIdentifier(),
            processorNode.getIdentifier(),
            flowAnalysisRuleNode.getIdentifier(),
            issueId,
            false
        );

        processorNode.performValidation();

        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());

        flowAnalysisContext.updateRuleViolation(
            processorNode.getIdentifier(),
            processorNode.getIdentifier(),
            flowAnalysisRuleNode.getIdentifier(),
            issueId,
            true
        );

        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.INVALID, processorNode.getValidationStatus());

        Collection<ValidationResult> actual = processorNode.getValidationErrors();

        assertEquals(expected, actual);
    }

    @Test
    public void testDisabledProcessorRuleViolationRemainsDisabledAfterNewAnalysis() throws Exception {
        // GIVEN
        String issueId = "issueId";
        String violationMessage = "Violation";

        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(analyzingComponent(issueId, violationMessage));
        rule.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<RuleViolation> expectedRuleViolations = new HashSet<>(Arrays.asList(
            disable(new RuleViolation(
                rule.getRuleType(),
                processorNode.getIdentifier(),
                processorNode.getIdentifier(),
                processorNode.getProcessGroupIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
            ))
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);

        flowAnalysisContext.updateRuleViolation(
            processorNode.getIdentifier(),
            processorNode.getIdentifier(),
            rule.getIdentifier(),
            issueId,
            false
        );

        mainFlowAnalyzer.analyzeProcessor(processorNode);

        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());

        checkActualViolations(expectedRuleViolations);
    }

    @Test
    public void testDisabledProcessorSpecificGroupScopedRuleViolationRemainsDisabledAfterNewAnalysis() throws Exception {
        // GIVEN
        String issueId = "processor_violation";
        String violationMessage = "Processor violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        ProcessorNode processorNode = createProcessorNode((context, session) -> {
        });
        processGroup.addProcessor(processorNode);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                processGroup.getProcessors().stream()
                    .map(processor -> GroupAnalysisResult.newResultForComponent(processor, issueId, violationMessage))
                    .forEach(results::add);

                return results;
            }
        });
        rule.setRuleType(FlowAnalysisRuleType.POLICY);

        Collection<RuleViolation> expectedRuleViolations = new HashSet<>(Arrays.asList(
            disable(new RuleViolation(
                rule.getRuleType(),
                processGroup.getIdentifier(),
                processorNode.getIdentifier(),
                processGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
            ))
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        flowAnalysisContext.updateRuleViolation(
            processGroup.getIdentifier(),
            processorNode.getIdentifier(),
            rule.getIdentifier(),
            issueId,
            false
        );

        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        processorNode.performValidation();

        // THEN
        assertEquals(ValidationStatus.VALID, processorNode.getValidationStatus());

        checkActualViolations(expectedRuleViolations);
    }

    @Test
    public void testDisabledGroupScopedRuleViolationRemainsDisabledAfterNewAnalysis() throws Exception {
        // GIVEN
        String issueId = "group_violation";
        String violationMessage = "Group violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(GroupAnalysisResult.newResultForGroup(issueId, violationMessage));

                return results;
            }
        });

        Collection<RuleViolation> expectedRuleViolations = new HashSet<>(Arrays.asList(
            disable(new RuleViolation(
                rule.getRuleType(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
            ))
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        flowAnalysisContext.updateRuleViolation(
            processGroup.getIdentifier(),
            processGroup.getIdentifier(),
            rule.getIdentifier(),
            issueId,
            false
        );

        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expectedRuleViolations);
    }

    @Test
    public void testDisabledChildGroupScopedRuleViolationRemainsDisabledAfterNewChildGroupAnalysis() throws Exception {
        // GIVEN
        String issueId = "group_violation";
        String violationMessage = "Group violation";

        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        ProcessGroup childProcessGroup = createProcessGroup(processGroup);

        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);
        VersionedProcessGroup versionedChildProcessGroup = mapProcessGroup(childProcessGroup);

        FlowAnalysisRuleNode rule = createAndEnableFlowAnalysisRuleNode(new AbstractFlowAnalysisRule() {
            @Override
            public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
                Collection<GroupAnalysisResult> results = new HashSet<>();

                results.add(GroupAnalysisResult.newResultForGroup(issueId, violationMessage));

                return results;
            }
        });

        Collection<RuleViolation> expectedRuleViolations = new HashSet<>(Arrays.asList(
            new RuleViolation(
                rule.getRuleType(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                processGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
            ),
            disable(new RuleViolation(
                rule.getRuleType(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                childProcessGroup.getIdentifier(),
                rule.getIdentifier(),
                issueId,
                violationMessage
            ))
        ));

        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        flowAnalysisContext.updateRuleViolation(
            childProcessGroup.getIdentifier(),
            childProcessGroup.getIdentifier(),
            rule.getIdentifier(),
            issueId,
            false
        );

        mainFlowAnalyzer.analyzeProcessGroup(versionedChildProcessGroup);

        // THEN
        checkActualViolations(expectedRuleViolations);
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
                    .map(issueIdAndViolationMessage -> ComponentAnalysisResult.newResult(issueIdAndViolationMessage.getKey(), issueIdAndViolationMessage.getValue()))
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
                GroupAnalysisResult result = GroupAnalysisResult.newResultForGroup(issueId, violationMessage);

                return Collections.singleton(result);
            }
        };
    }

    private RuleViolation disable(RuleViolation expectedRuleViolation) {
        expectedRuleViolation.setEnabled(false);

        return expectedRuleViolation;
    }

    private void testAnalyzeProcessor(ProcessorNode processorNode, Collection<RuleViolation> expected) {
        // WHEN
        mainFlowAnalyzer.analyzeProcessor(processorNode);

        // THEN
        checkActualViolations(expected);
    }

    private void testAnalyzeProcessGroup(VersionedProcessGroup versionedProcessGroup, Collection<RuleViolation> expected) {
        // WHEN
        mainFlowAnalyzer.analyzeProcessGroup(versionedProcessGroup);

        // THEN
        checkActualViolations(expected);
    }

    private void checkActualViolations(Collection<RuleViolation> expected) {
        Collection<RuleViolation> actual = flowAnalysisContext.getAllRuleViolations();

        assertEquals(expected, actual);
    }

    @Test
    public void testCreateAnalyzeFlowRequestAndComplete() throws Exception {
        // GIVEN
        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        AnalyzeFlowStatus expectedStatusAtCreateAndBeforeAnalysis = createExpectedStatus(processGroup, AnalyzeFlowState.WAITING);
        AnalyzeFlowStatus expectedStatusDuringAnalysis = createExpectedStatus(processGroup, AnalyzeFlowState.ANALYZING);
        AnalyzeFlowStatus expectedStatusAfterAnalysis = createExpectedStatus(processGroup, AnalyzeFlowState.COMPLETE);

        startRunAnalysis.set(new CountDownLatch(1));
        finishAnalysis.set(new CountDownLatch(1));

        // WHEN
        // THEN
        AnalyzeFlowStatus actualStatusAtCreate = mainFlowAnalyzer.createAnalyzeFlowRequest(versionedProcessGroup);
        checkActualStatus(expectedStatusAtCreateAndBeforeAnalysis, actualStatusAtCreate);

        AnalyzeFlowStatus actualStateBeforeAnalysis = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAtCreateAndBeforeAnalysis, actualStateBeforeAnalysis);

        startRunAnalysis.get().countDown();
        analysisStarted.await();
        AnalyzeFlowStatus actualStateDuringAnalysis = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusDuringAnalysis, actualStateDuringAnalysis);
        finishAnalysis.get().countDown();

        analysisRunFinished.await();
        AnalyzeFlowStatus actualStateAfterAnalysis = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAfterAnalysis, actualStateAfterAnalysis);
    }

    @Test
    public void testCreateAnalyzeFlowRequestAndCancelBeforeStart() throws Exception {
        // GIVEN
        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        AnalyzeFlowStatus expectedStatusAtCreate = createExpectedStatus(processGroup, AnalyzeFlowState.WAITING);
        AnalyzeFlowStatus expectedStatusAtAndAfterCancelAndAfterAnalysis = createExpectedStatus(processGroup, AnalyzeFlowState.CANCELED);

        startRunAnalysis.set(new CountDownLatch(1));

        // WHEN
        // THEN
        AnalyzeFlowStatus actualStatusAtCreate = mainFlowAnalyzer.createAnalyzeFlowRequest(versionedProcessGroup);
        checkActualStatus(expectedStatusAtCreate, actualStatusAtCreate);

        AnalyzeFlowStatus actualStateAtCancel = mainFlowAnalyzer.cancelAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAtAndAfterCancelAndAfterAnalysis, actualStateAtCancel);

        AnalyzeFlowStatus actualStateAfterCancel = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAtAndAfterCancelAndAfterAnalysis, actualStateAfterCancel);

        startRunAnalysis.get().countDown();
        analysisRunFinished.await();

        AnalyzeFlowStatus actualStateAfterAnalysis = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAtAndAfterCancelAndAfterAnalysis, actualStateAfterAnalysis);
    }

    @Test
    public void testCreateAnalyzeFlowRequestAndFail() throws Exception {
        // GIVEN
        ProcessGroup processGroup = createProcessGroup(getRootGroup());
        VersionedProcessGroup versionedProcessGroup = mapProcessGroup(processGroup);

        String failureExplanation = "group_analysis_failure";
        AnalyzeFlowStatus expectedStatusAfterAnalysis = createExpectedStatus(
            processGroup,
            AnalyzeFlowState.FAILURE,
            "Flow Analysis of process group '" + processGroup.getIdentifier() + "' failed due to java.lang.RuntimeException: " + failureExplanation
        );

        groupAnalysisError.set(new RuntimeException(failureExplanation));

        // WHEN
        // THEN
        mainFlowAnalyzer.createAnalyzeFlowRequest(versionedProcessGroup);

        analysisRunFinished.await();
        AnalyzeFlowStatus actualStateAfterAnalysis = mainFlowAnalyzer.getAnalyzeFlowRequest(processGroup.getIdentifier());
        checkActualStatus(expectedStatusAfterAnalysis, actualStateAfterAnalysis);
    }

    private AnalyzeFlowStatus createExpectedStatus(ProcessGroup processGroup, AnalyzeFlowState expectedState, String explanation) {
        AnalyzeFlowRequest status = new AnalyzeFlowRequest(processGroup.getIdentifier());
        status.setState(expectedState, explanation);

        return status;
    }

    private AnalyzeFlowStatus createExpectedStatus(ProcessGroup processGroup, AnalyzeFlowState expectedState) {
        AnalyzeFlowRequest status = new AnalyzeFlowRequest(processGroup.getIdentifier());
        status.setState(expectedState);

        return status;
    }

    private void checkActualStatus(AnalyzeFlowStatus expected, AnalyzeFlowStatus actual) {
        List<Function<AnalyzeFlowStatus, Object>> equalityProperties = Arrays.asList(
            AnalyzeFlowStatus::getProcessGroupId,
            AnalyzeFlowStatus::getState,
            AnalyzeFlowStatus::getFailureReason
        );

        assertEquals(
            new EqualsWrapper<>(expected, equalityProperties),
            new EqualsWrapper<>(actual, equalityProperties)
        );
    }
}
