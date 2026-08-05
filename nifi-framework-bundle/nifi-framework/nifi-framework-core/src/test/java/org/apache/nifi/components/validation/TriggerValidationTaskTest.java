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

package org.apache.nifi.components.validation;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TriggerValidationTaskTest {

    private ExecutorService executorService;

    @AfterEach
    void shutdownExecutor() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void testSerialConstructorTriggersEveryComponentOnCallingThread() {
        final Fixture fixture = new Fixture();
        final List<ComponentNode> triggered = new CopyOnWriteArrayList<>();
        final ValidationTrigger trigger = recordingTrigger(triggered);

        final TriggerValidationTask task = new TriggerValidationTask(fixture.flowManager, trigger);
        task.run();

        assertTrue(task.isValidationComplete());
        assertEquals(new HashSet<>(fixture.expectedComponentNodes()), new HashSet<>(triggered));
        verify(fixture.connector, times(1)).validateComponents(trigger);
    }

    @Test
    void testParallelConstructorTriggersEveryComponentAndCompletes() throws InterruptedException {
        final Fixture fixture = new Fixture();
        final List<ComponentNode> triggered = new CopyOnWriteArrayList<>();
        final ValidationTrigger trigger = recordingTrigger(triggered);
        executorService = Executors.newFixedThreadPool(4);

        final TriggerValidationTask task = new TriggerValidationTask(fixture.flowManager, trigger, executorService);
        task.run();

        assertTrue(task.isValidationComplete());
        assertEquals(new HashSet<>(fixture.expectedComponentNodes()), new HashSet<>(triggered));
        verify(fixture.connector, times(1)).validateComponents(trigger);
    }

    @Test
    void testParallelConstructorReportsIncompleteWhenExecutorRejectsWork() {
        final Fixture fixture = new Fixture();
        final List<ComponentNode> triggered = new CopyOnWriteArrayList<>();
        final ValidationTrigger trigger = recordingTrigger(triggered);

        executorService = Executors.newFixedThreadPool(2);
        executorService.shutdown(); // no longer accepts new work; submit() will throw RejectedExecutionException

        final TriggerValidationTask task = new TriggerValidationTask(fixture.flowManager, trigger, executorService);
        task.run();

        assertFalse(task.isValidationComplete());
    }

    @Test
    void testParallelConstructorReportsIncompleteWhenInterruptedWhileAwaitingCompletion() throws InterruptedException {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessorNode slowProcessor = mock(ProcessorNode.class);

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findAllProcessors()).thenReturn(List.of(slowProcessor));
        when(flowManager.getAllControllerServices()).thenReturn(Collections.emptySet());
        when(flowManager.getAllReportingTasks()).thenReturn(Collections.emptySet());
        when(flowManager.getAllFlowAnalysisRules()).thenReturn(Collections.emptySet());
        when(flowManager.getAllParameterProviders()).thenReturn(Collections.emptySet());
        when(flowManager.getAllFlowRegistryClients()).thenReturn(Collections.emptySet());
        when(flowManager.getAllConnectors()).thenReturn(Collections.emptyList());

        final CountDownLatch validationStarted = new CountDownLatch(1);
        final CountDownLatch releaseValidation = new CountDownLatch(1);
        final ValidationTrigger trigger = new ValidationTrigger() {
            @Override
            public void triggerAsync(final ComponentNode component) {
            }

            @Override
            public void trigger(final ComponentNode component) {
                validationStarted.countDown();
                try {
                    // Simulate a component validation that is still in progress when the calling
                    // thread is interrupted; the pool thread itself is not interrupted.
                    releaseValidation.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        executorService = Executors.newSingleThreadExecutor();
        final TriggerValidationTask task = new TriggerValidationTask(flowManager, trigger, executorService);

        final AtomicBoolean interruptStatusRestored = new AtomicBoolean(false);
        final Thread runner = new Thread(() -> {
            task.run();
            interruptStatusRestored.set(Thread.currentThread().isInterrupted());
        });

        try {
            runner.start();
            assertTrue(validationStarted.await(5, TimeUnit.SECONDS), "Validation did not start within the timeout");

            runner.interrupt();
            runner.join(5_000);

            assertFalse(runner.isAlive(), "TriggerValidationTask did not return promptly after being interrupted");
            assertFalse(task.isValidationComplete());
            assertTrue(interruptStatusRestored.get(), "Interrupt status should be restored on the thread that called run()");
        } finally {
            releaseValidation.countDown();
        }
    }

    private static ValidationTrigger recordingTrigger(final List<ComponentNode> triggered) {
        return new ValidationTrigger() {
            @Override
            public void triggerAsync(final ComponentNode component) {
                triggered.add(component);
            }

            @Override
            public void trigger(final ComponentNode component) {
                triggered.add(component);
            }
        };
    }

    /**
     * Provides a FlowManager mock with exactly one component of each type that TriggerValidationTask is expected
     * to validate, so tests can assert that every category is covered.
     */
    private static final class Fixture {
        private final FlowManager flowManager = mock(FlowManager.class);
        private final ControllerServiceNode controllerService = mock(ControllerServiceNode.class);
        private final ReportingTaskNode reportingTask = mock(ReportingTaskNode.class);
        private final FlowAnalysisRuleNode flowAnalysisRule = mock(FlowAnalysisRuleNode.class);
        private final ParameterProviderNode parameterProvider = mock(ParameterProviderNode.class);
        private final FlowRegistryClientNode flowRegistryClient = mock(FlowRegistryClientNode.class);
        private final ProcessorNode processor = mock(ProcessorNode.class);
        private final ConnectorNode connector = mock(ConnectorNode.class);
        private final ProcessGroup rootGroup = mock(ProcessGroup.class);

        private Fixture() {
            when(flowManager.getAllControllerServices()).thenReturn(Set.of(controllerService));
            when(flowManager.getAllReportingTasks()).thenReturn(Set.of(reportingTask));
            when(flowManager.getAllFlowAnalysisRules()).thenReturn(Set.of(flowAnalysisRule));
            when(flowManager.getAllParameterProviders()).thenReturn(Set.of(parameterProvider));
            when(flowManager.getAllFlowRegistryClients()).thenReturn(Set.of(flowRegistryClient));
            when(flowManager.getAllConnectors()).thenReturn(List.of(connector));
            when(flowManager.getRootGroup()).thenReturn(rootGroup);
            when(rootGroup.findAllProcessors()).thenReturn(List.of(processor));
        }

        private List<ComponentNode> expectedComponentNodes() {
            return List.of(controllerService, reportingTask, flowAnalysisRule, parameterProvider, processor, flowRegistryClient);
        }
    }
}
