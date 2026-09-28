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
package org.apache.nifi.controller.scheduling.auto;

import org.apache.nifi.controller.scheduling.SchedulingSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardAutoSchedulingControllerTest {
    private static final SystemSchedulingSnapshot AVAILABLE_SYSTEM_CAPACITY = SystemSchedulingSnapshot.createBuilder()
            .setCpuLoadAvailable(true)
            .setCpuLoad(0.25D)
            .setAverageCpuLoad(0.25D)
            .setMaxGlobalConcurrentTasks(32)
            .build();

    @Test
    void testQueuedWorkCanAcquireMoreCapacityBeforeFirstCompletion() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.completedInvocations = 0;
        simulation.observe(0);
        assertEquals(2, simulation.settings.concurrentTasks());

        for (int second = 0; second < 15; second++) {
            simulation.observe(0);
            assertTrue(simulation.settings.concurrentTasks() <= 2);
        }

        simulation.completedInvocations = 1;
        simulation.observe(1);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @CsvSource({"1000, 1", "2000, 2"})
    void testCatchingUpWithWaitingWorkRequiresThroughputGain(final long comparisonFlowFiles, final int expectedConcurrentTasks) {
        final Simulation simulation = new Simulation(12, true, false);
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(25), simulation.settings.runDurationNanos());

        simulation.processorReady = false;
        simulation.inputQueueHasFlowFiles = false;
        simulation.observe(6, comparisonFlowFiles);
        assertEquals(expectedConcurrentTasks, simulation.settings.concurrentTasks());

        simulation.concurrentTaskUtilization = 0.5D;
        simulation.observe(1000);
        assertEquals(expectedConcurrentTasks, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 100, 1000})
    void testHigherConcurrencyMustImproveUsefulThroughput(final long comparisonFlowFiles) {
        final Simulation simulation = new Simulation(12, false, false);
        final int originalConcurrentTasks = simulation.settings.concurrentTasks();
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
        simulation.observe(6, comparisonFlowFiles);

        assertEquals(originalConcurrentTasks, simulation.settings.concurrentTasks());
        assertEquals(ConcurrencyUpdateReason.PREVIOUS_INCREASE_DID_NOT_IMPROVE_THROUGHPUT,
                simulation.controller.getConcurrencyUpdateStatus().reason());

        for (int second = 0; second < 5; second++) {
            simulation.observe(1000);
            assertEquals(1, simulation.settings.concurrentTasks());
        }
    }

    @Test
    void testSourceRequiresRecentActivity() {
        final Simulation source = new Simulation(12, false, false);
        source.inputQueueHasFlowFiles = false;
        source.sourceProcessorRecentlyReportedActivity = true;
        source.observe(100);
        assertEquals(2, source.settings.concurrentTasks());

        final Simulation idleSource = new Simulation(12, false, false);
        idleSource.inputQueueHasFlowFiles = false;
        idleSource.concurrentTaskUtilization = 0D;
        idleSource.observe(0);
        assertEquals(1, idleSource.settings.concurrentTasks());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 16, 64})
    void testConcurrencyAdaptsToConfiguredMaximum(final int maxConcurrentTasks) {
        final Simulation simulation = new Simulation(maxConcurrentTasks, false, false);
        int acceptedConcurrentTasks = 1;
        int maxConcurrentTasksObserved = 1;
        for (int second = 0; second < maxConcurrentTasks * 8; second++) {
            simulation.observe(simulation.settings.concurrentTasks() * 1000L);
            maxConcurrentTasksObserved = Math.max(maxConcurrentTasksObserved, simulation.settings.concurrentTasks());
            if (simulation.lastDecision.reason() == ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_INCREASED_THROUGHPUT) {
                assertEquals(acceptedConcurrentTasks + 1, simulation.settings.concurrentTasks());
                acceptedConcurrentTasks = simulation.settings.concurrentTasks();
            }

            assertTrue(simulation.settings.concurrentTasks() <= maxConcurrentTasks);
        }

        assertEquals(maxConcurrentTasks, maxConcurrentTasksObserved);
    }

    @Test
    void testSerialProcessorRemainsSerial() {
        final Simulation simulation = new Simulation(12, true, true);
        for (int second = 0; second < 10; second++) {
            simulation.observe(1000);
            assertEquals(1, simulation.settings.concurrentTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCpuSaturationPreservesProductiveCapacity(final boolean duringHigherConcurrencyTest) {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(duringHigherConcurrencyTest ? 1 : 4, 0L);
        if (duringHigherConcurrencyTest) {
            simulation.observe(1000);
        }

        final int expectedConcurrentTasks = duringHigherConcurrencyTest ? 2 : 4;
        simulation.systemMeasurements = SystemSchedulingSnapshot.createBuilder()
                .setCpuLoadAvailable(true)
                .setCpuLoad(0.95D)
                .setAverageCpuLoad(0.95D)
                .setMaxGlobalConcurrentTasks(32)
                .setCpuLoadAboveLimit(true)
                .build();
        simulation.observe(15, expectedConcurrentTasks * 1000L);
        assertEquals(expectedConcurrentTasks, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "false, 1000"})
    void testIdleBackpressuredAndIntermittentWorkReleaseUnusedCapacity(final boolean backpressured, final long work) {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(4, 0L);
        simulation.processorReady = !backpressured;
        simulation.inputQueueHasFlowFiles = backpressured || work > 0L;
        simulation.concurrentTaskUtilization = backpressured ? 1D : 0.4D;
        simulation.backpressureNanos = backpressured ? TimeUnit.SECONDS.toNanos(1) : 0L;
        for (int second = 0; second < 9; second++) {
            final int originalConcurrentTasks = simulation.settings.concurrentTasks();
            simulation.observe(work);
            if (simulation.lastDecision.reason() == ProcessorSchedulingDecisionReason.REDUCED_UNUSED_CONCURRENT_TASK) {
                assertEquals(originalConcurrentTasks - 1, simulation.settings.concurrentTasks());
            }
        }

        assertEquals(1, simulation.settings.concurrentTasks());

        simulation.processorReady = true;
        simulation.inputQueueHasFlowFiles = true;
        simulation.concurrentTaskUtilization = 1D;
        simulation.backpressureNanos = 0L;
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @Test
    void testReductionPreservesThroughputAndRestoresCapacityWhenNeeded() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(4, 0L);
        simulation.concurrentTaskUtilization = 0.5D;
        simulation.observe(30, 1000);
        assertEquals(4, simulation.settings.concurrentTasks());

        simulation.observe(1000);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY, simulation.lastDecision.reason());
        assertEquals(3, simulation.settings.concurrentTasks());
        simulation.observe(6, 1000);
        assertEquals(ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT, simulation.lastDecision.reason());
        assertEquals(3, simulation.settings.concurrentTasks());

        // Each lower setting is compared against the rate measured with 4 tasks, not against the previous lower setting.
        simulation.observe(3, 960);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY, simulation.lastDecision.reason());
        simulation.observe(6, 960);
        assertEquals(ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT, simulation.lastDecision.reason());
        assertEquals(2, simulation.settings.concurrentTasks());

        simulation.observe(3, 915);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY, simulation.lastDecision.reason());
        simulation.observe(6, 915);
        assertEquals(ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_REDUCED_THROUGHPUT_OR_INCREASED_INPUT_QUEUE, simulation.lastDecision.reason());
        assertEquals(2, simulation.settings.concurrentTasks());

        // A rejected lower setting refreshes the reference rate, so the next test is compared against the current rate.
        simulation.observe(30, 915);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY, simulation.lastDecision.reason());
        simulation.observe(6, 915);
        assertEquals(ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT, simulation.lastDecision.reason());
        assertEquals(1, simulation.settings.concurrentTasks());

        final Simulation busy = new Simulation(4, false, false);
        busy.settings = new SchedulingSettings(4, 0L);
        busy.observe(30, 4000);
        busy.observe(4000);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY, busy.lastDecision.reason());
        assertEquals(3, busy.settings.concurrentTasks());

        busy.observe(6, 3000);
        assertEquals(ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_REDUCED_THROUGHPUT_OR_INCREASED_INPUT_QUEUE, busy.lastDecision.reason());
        assertEquals(4, busy.settings.concurrentTasks());
    }

    @Test
    void testUnavailableCpuMeasurementStillAllowsBoundedProbing() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.systemMeasurements = SystemSchedulingSnapshot.createBuilder()
                .setCpuLoad(-1D)
                .setAverageCpuLoad(-1D)
                .setMaxGlobalConcurrentTasks(32)
                .build();
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @Test
    void testSharedBudgetAndFailuresBoundGrowth() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.systemMeasurements = SystemSchedulingSnapshot.createBuilder()
                .setCpuLoadAvailable(true)
                .setCpuLoad(0.25D)
                .setAverageCpuLoad(0.25D)
                .setMaxGlobalConcurrentTasks(1)
                .setFractionOfSamplesWithAllGlobalTaskSlotsUsed(1D)
                .setFractionOfGlobalTaskTimeSpentWaiting(0.5D)
                .build();
        simulation.observe(1000);
        assertEquals(1, simulation.settings.concurrentTasks());
        assertEquals(ConcurrencyUpdateReason.CONCURRENT_TASK_LIMIT_REACHED, simulation.controller.getConcurrencyUpdateStatus().reason());

        simulation.systemMeasurements = AVAILABLE_SYSTEM_CAPACITY;
        simulation.completedInvocations = 100;
        simulation.failedInvocations = 1;
        simulation.observe(1000);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_HIGHER_CONCURRENCY, simulation.lastDecision.reason());
        assertEquals(2, simulation.settings.concurrentTasks());

        simulation.completedInvocations = 10;
        simulation.observe(1000);
        assertEquals(1, simulation.settings.concurrentTasks());
        simulation.observe(1000);
        assertEquals(ProcessorSchedulingDecisionReason.PROCESSOR_INVOCATION_FAILED, simulation.lastDecision.reason());
    }

    @Test
    void testConfiguredAndProcessorCountLimitReasons() {
        final Simulation configuredLimit = new Simulation(12, false, false);
        configuredLimit.settings = new SchedulingSettings(12, 0L);
        configuredLimit.observe(12_000);
        assertEquals(ConcurrencyUpdateReason.CONFIGURED_MAX_CONCURRENT_TASKS_REACHED,
                configuredLimit.controller.getConcurrencyUpdateStatus().reason());

        final Simulation processorCountLimit = new Simulation(8, false, false, true, 2);
        processorCountLimit.settings = new SchedulingSettings(8, 0L);
        processorCountLimit.observe(8_000);
        assertEquals(ConcurrencyUpdateReason.CPU_LIMIT_REACHED, processorCountLimit.controller.getConcurrencyUpdateStatus().reason());
    }

    @Test
    void testWaitingWorkAndResourceLimitReasons() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.inputQueueHasFlowFiles = false;
        simulation.observe(0);
        assertEquals(ConcurrencyUpdateReason.NO_WAITING_WORK, simulation.controller.getConcurrencyUpdateStatus().reason());

        simulation.inputQueueHasFlowFiles = true;
        simulation.systemMeasurements = SystemSchedulingSnapshot.createBuilder()
                .setCpuLoadAvailable(true)
                .setCpuLoad(0.95D)
                .setAverageCpuLoad(0.95D)
                .setMaxGlobalConcurrentTasks(32)
                .setCpuLoadAboveLimit(true)
                .build();
        simulation.observe(1000);
        assertEquals(ConcurrencyUpdateReason.CPU_LOAD_TOO_HIGH, simulation.controller.getConcurrencyUpdateStatus().reason());
    }

    @Test
    void testConcurrencyComparisonHasBoundedLifetimeWithoutCompletions() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.completedInvocations = 0;
        simulation.observe(0);
        simulation.observe(30, 0);

        assertNotEquals(ConcurrencyEvaluationState.MEASURING_CONCURRENCY_CHANGE, simulation.lastDecision.state());
        assertEquals(1, simulation.settings.concurrentTasks());
    }

    @Test
    void testComparisonCoversSeveralInvocationsPerTask() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.completedInvocations = 1;
        simulation.processorInvocationNanos = TimeUnit.SECONDS.toNanos(1);
        simulation.observe(100);
        assertEquals(ProcessorSchedulingDecisionReason.TESTING_HIGHER_CONCURRENCY, simulation.lastDecision.reason());

        simulation.completedInvocations = 2;
        simulation.processorInvocationNanos = TimeUnit.SECONDS.toNanos(2);
        simulation.observe(9, 200);
        assertEquals(ProcessorSchedulingDecisionReason.COLLECTING_COMPARISON_MEASUREMENTS, simulation.lastDecision.reason());

        simulation.observe(200);
        assertEquals(ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_INCREASED_THROUGHPUT, simulation.lastDecision.reason());
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @Test
    void testSlowSerializedWorkDoesNotAccumulateUnproductiveConcurrency() {
        final Simulation simulation = new Simulation(12, false, false);
        int maxConcurrentTasksObserved = 1;
        boolean reducedToOne = false;
        for (int second = 1; second <= 120; second++) {
            final boolean completed = second % 5 == 0;
            simulation.completedInvocations = completed ? 1 : 0;
            simulation.processorInvocationNanos = completed ? TimeUnit.SECONDS.toNanos(5L * simulation.settings.concurrentTasks()) : 0L;
            simulation.observe(completed ? 100 : 0);
            maxConcurrentTasksObserved = Math.max(maxConcurrentTasksObserved, simulation.settings.concurrentTasks());
            if (second > 60 && simulation.settings.concurrentTasks() == 1) {
                reducedToOne = true;
            }
        }

        assertTrue(maxConcurrentTasksObserved <= 3);
        assertTrue(reducedToOne);
    }

    @Test
    void testScalableWorkloadRampsDespiteVariableCommitRates() {
        final Simulation simulation = new Simulation(16, false, false);
        final double[] relativeRates = {1.3D, 0.7D, 1.2D, 0.8D, 1.1D, 0.9D};
        int maxConcurrentTasksObserved = 1;
        for (int second = 0; second < 180; second++) {
            simulation.observe((long) (1000 * simulation.settings.concurrentTasks() * relativeRates[second % relativeRates.length]));
            maxConcurrentTasksObserved = Math.max(maxConcurrentTasksObserved, simulation.settings.concurrentTasks());
        }

        assertEquals(16, maxConcurrentTasksObserved);
    }

    private static class Simulation {
        private final StandardAutoSchedulingController controller;
        private SchedulingSettings settings;
        private SystemSchedulingSnapshot systemMeasurements = AVAILABLE_SYSTEM_CAPACITY;
        private ProcessorSchedulingDecision lastDecision;
        private long timestampNanos;
        private long completedInvocations = 10;
        private long failedInvocations;
        private long processorInvocationNanos;
        private long backpressureNanos;
        private double concurrentTaskUtilization = 1D;
        private boolean processorReady = true;
        private boolean inputQueueHasFlowFiles = true;
        private boolean sourceProcessorRecentlyReportedActivity;

        private Simulation(final int maxConcurrentTasks, final boolean batchingSupported, final boolean processorTriggeredSerially) {
            this(maxConcurrentTasks, batchingSupported, processorTriggeredSerially, false, Runtime.getRuntime().availableProcessors());
        }

        private Simulation(final int maxConcurrentTasks, final boolean batchingSupported, final boolean processorTriggeredSerially,
                           final boolean maxConcurrentTasksBasedOnAvailableProcessors, final int availableProcessorCount) {
            controller = new StandardAutoSchedulingController(maxConcurrentTasks, batchingSupported, processorTriggeredSerially,
                    maxConcurrentTasksBasedOnAvailableProcessors, availableProcessorCount);
            systemMeasurements = SystemSchedulingSnapshot.createBuilder()
                    .setCpuLoadAvailable(true)
                    .setCpuLoad(0.25D)
                    .setAverageCpuLoad(0.25D)
                    .setMaxGlobalConcurrentTasks(Math.max(32, maxConcurrentTasks))
                    .build();
            settings = new SchedulingSettings(1, batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25) : 0L);
        }

        private void observe(final int seconds, final long work) {
            for (int second = 0; second < seconds; second++) {
                observe(work);
            }
        }

        private void observe(final long committedFlowFiles) {
            timestampNanos += TimeUnit.SECONDS.toNanos(1);
            final ProcessorSchedulingSnapshot processorMeasurements = ProcessorSchedulingSnapshot.createBuilder()
                    .setTimestampNanos(timestampNanos)
                    .setMeasurementWindowNanos(TimeUnit.SECONDS.toNanos(1))
                    .setAppliedSettings(settings)
                    .setCommittedFlowFiles(committedFlowFiles)
                    .setCompletedInvocations(completedInvocations)
                    .setFailedInvocations(failedInvocations)
                    .setProcessorInvocationNanos(processorInvocationNanos)
                    .setBackpressureNanos(backpressureNanos)
                    .setConcurrentTaskUtilization(concurrentTaskUtilization)
                    .setProcessorReady(processorReady)
                    .setInputQueueHasFlowFiles(inputQueueHasFlowFiles)
                    .setSourceProcessorRecentlyReportedActivity(sourceProcessorRecentlyReportedActivity)
                    .build();
            lastDecision = controller.evaluate(processorMeasurements, systemMeasurements);
            if (lastDecision.applySettings()) {
                settings = lastDecision.settings();
            }
        }
    }
}
