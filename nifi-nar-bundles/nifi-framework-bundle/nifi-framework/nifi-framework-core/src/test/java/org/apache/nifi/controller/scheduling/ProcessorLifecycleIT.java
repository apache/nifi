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
package org.apache.nifi.controller.scheduling;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Validate Processor's life-cycle operation within the context of
 * {@link FlowController} and {@link StandardProcessScheduler}
 */
public class ProcessorLifecycleIT {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorLifecycleIT.class);
    private static final long SHORT_DELAY_TOLERANCE = 10000L;
    private static final long MEDIUM_DELAY_TOLERANCE = 15000L;
    private static final long LONG_DELAY_TOLERANCE = 20000L;

    private FlowManager flowManager;
    private Map<String, String> properties = new HashMap<>();
    private volatile String propsFile = "src/test/resources/lifecycletest.nifi.properties";
    private ProcessScheduler processScheduler;

    @Before
    public void before() throws Exception {
        properties.put("P", "hello");
    }

    @After
    public void after() throws Exception {
        if (processScheduler != null) {
            processScheduler.shutdown();
            processScheduler = null;
        }

        FileUtils.deleteDirectory(new File("./target/lifecycletest"));
    }

    private void assertCondition(final Supplier<Boolean> supplier) {
        assertCondition(supplier, SHORT_DELAY_TOLERANCE);
    }

    private void assertCondition(final Supplier<Boolean> supplier, final long delayToleranceMillis) {
        final long startTime = System.currentTimeMillis();
        while (((System.currentTimeMillis() - startTime) < delayToleranceMillis) && !supplier.get()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.interrupted();
                break;
            }
        }
        assertTrue(supplier.get());
    }

    @Test
    public void validateEnableOperation() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();
        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        final ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(),
                UUID.randomUUID().toString(), fcsb.getSystemBundle().getBundleDetails().getCoordinate());

        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getPhysicalScheduledState());
        // validates idempotency
        for (int i = 0; i < 2; i++) {
            testProcNode.enable();
        }
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getPhysicalScheduledState());
        testProcNode.disable();
        assertCondition(() -> ScheduledState.DISABLED == testProcNode.getScheduledState());
        assertCondition(() -> ScheduledState.DISABLED == testProcNode.getPhysicalScheduledState());
    }

    @Test
    public void validateDisableOperation() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        final ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(),
                UUID.randomUUID().toString(), fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getPhysicalScheduledState());
        // validates idempotency
        for (int i = 0; i < 2; i++) {
            testProcNode.disable();
        }
        assertCondition(() -> ScheduledState.DISABLED == testProcNode.getScheduledState());
        assertCondition(() -> ScheduledState.DISABLED == testProcNode.getPhysicalScheduledState());

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.DISABLED == testProcNode.getPhysicalScheduledState());
    }

    /**
     * Will validate the idempotent nature of processor start operation which
     * can be called multiple times without any side-effects.
     */
    @Test
    public void validateIdempotencyOfProcessorStartOperation() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        final ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        processScheduler.startProcessor(testProcNode, true);
        processScheduler.startProcessor(testProcNode, true);

        Thread.sleep(500);
        assertCondition(() -> testProcessor.operationNames.size() == 1);
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
    }

    /**
     * Validates that stop calls are harmless and idempotent if processor is not
     * in STARTING or RUNNING state.
     */
    @Test
    public void validateStopCallsAreMeaninglessIfProcessorNotStarted() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();
        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        final ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        // sets the scenario for the processor to run
        int randomDelayLimit = 3000;
        this.randomOnTriggerDelay(testProcessor, randomDelayLimit);

        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        assertTrue(testProcessor.operationNames.isEmpty());
    }

    /**
     * Validates the processors start/stop sequence where the order of
     * operations can only be @OnScheduled, @OnUnscheduled, @OnStopped.
     */
    @Test
    @Ignore
    public void validateSuccessfulAndOrderlyShutdown() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();
        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        int randomDelayLimit = 3000;
        this.randomOnTriggerDelay(testProcessor, randomDelayLimit);

        testProcNode.setMaxConcurrentTasks(4);
        testProcNode.setScheduldingPeriod("500 millis");
        testProcNode.setAutoTerminatedRelationships(Collections.singleton(new Relationship.Builder().name("success").build()));

        testGroup.addProcessor(testProcNode);

        flowManager.getGroup(testGroup.getIdentifier()).startProcessing();
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);

        flowManager.getRootGroup().stopProcessing();

        Thread.sleep(randomDelayLimit); // up to randomDelayLimit, otherwise next assertion may fail as the processor still executing

        // validates that regardless of how many running tasks, lifecycle
        // operation are invoked atomically (once each).
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);
        // . . . hence only 3 operations must be in the list
        assertCondition(() -> testProcessor.operationNames.size() == 3, SHORT_DELAY_TOLERANCE);
        // . . . and ordered as @OnScheduled, @OnUnscheduled, @OnStopped
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
        assertEquals("@OnUnscheduled", testProcessor.operationNames.get(1));
        assertEquals("@OnStopped", testProcessor.operationNames.get(2));
    }

    /**
     * Concurrency test that is basically hammers on both stop and start
     * operation validating their idempotency.
     */
    @Test
    @Ignore
    public void validateLifecycleOperationOrderWithConcurrentCallsToStartStop() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        final ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        int startCallsCount = 10000;
        final CountDownLatch countDownCounter = new CountDownLatch(startCallsCount);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState());
        final Random random = new Random();
        for (int i = 0; i < startCallsCount / 2; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    LockSupport.parkNanos(random.nextInt(9000000));
                    processScheduler.stopProcessor(testProcNode);
                    countDownCounter.countDown();
                }
            });
        }
        for (int i = 0; i < startCallsCount / 2; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    LockSupport.parkNanos(random.nextInt(9000000));
                    processScheduler.startProcessor(testProcNode, true);
                    countDownCounter.countDown();
                }
            });
        }
        assertTrue(countDownCounter.await(1000000, TimeUnit.MILLISECONDS));
        String previousOperation = null;
        for (String operationName : testProcessor.operationNames) {
            if (previousOperation == null || previousOperation.equals("@OnStopped")) {
                assertEquals("@OnScheduled", operationName);
            } else if (previousOperation.equals("@OnScheduled")) {
                assertEquals("@OnUnscheduled", operationName);
            } else if (previousOperation.equals("@OnUnscheduled")) {
                assertTrue(operationName.equals("@OnStopped") || operationName.equals("@OnScheduled"));
            }
            previousOperation = operationName;
        }
        executor.shutdownNow();
    }

    /**
     * Validates that processor can be stopped before start sequence finished.
     */
    @Test
    public void validateProcessorUnscheduledAndStoppedWhenStopIsCalledBeforeProcessorFullyStarted() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        int delay = 200;
        this.longRunningOnSchedule(testProcessor, delay);

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), MEDIUM_DELAY_TOLERANCE);

        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), MEDIUM_DELAY_TOLERANCE);
        assertCondition(() -> testProcessor.operationNames.size() == 3, LONG_DELAY_TOLERANCE);
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
        assertEquals("@OnUnscheduled", testProcessor.operationNames.get(1));
        assertEquals("@OnStopped", testProcessor.operationNames.get(2));
    }

    /**
     * Validates that Processor is eventually started once invocation of
     *
     * @OnSchedule stopped throwing exceptions.
     */
    @Test
    public void validateProcessScheduledAfterAdministrativeDelayDueToTheOnScheduledException() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);
        testProcessor.generateExceptionOnScheduled = true;
        testProcessor.keepFailingOnScheduledTimes = 2;

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), LONG_DELAY_TOLERANCE);
        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);
    }

    /**
     * Validates that Processor can be stopped when @OnScheduled constantly
     * fails. Basically validates that the re-try loop breaks if user initiated
     * stopProcessor.
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledConstantlyFails() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.longRunningOnUnschedule(testProcessor, 100);
        testProcessor.generateExceptionOnScheduled = true;
        testProcessor.keepFailingOnScheduledTimes = Integer.MAX_VALUE;

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);
        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);
    }

    /**
     * Validates that the Processor can be stopped when @OnScheduled blocks
     * indefinitely but written to react to thread interrupts
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledBlocksIndefinitelyInterruptable() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT, "5 sec");
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        // sets the scenario for the processor to run
        this.blockingInterruptableOnUnschedule(testProcessor);

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), SHORT_DELAY_TOLERANCE);
        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), MEDIUM_DELAY_TOLERANCE);
    }

    /**
     * Validates that the Processor can be stopped when @OnScheduled blocks
     * indefinitely and written to ignore thread interrupts
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledBlocksIndefinitelyUninterruptable() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT, "1 sec");
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        // sets the scenario for the processor to run
        this.blockingUninterruptableOnUnschedule(testProcessor);

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), MEDIUM_DELAY_TOLERANCE);
        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), MEDIUM_DELAY_TOLERANCE);
    }

    /**
     * Validates that processor can be stopped if onTrigger() keeps throwing
     * exceptions.
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnTriggerThrowsException() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testProcNode.setProperties(properties);
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);
        testProcessor.generateExceptionOnTrigger = true;

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), LONG_DELAY_TOLERANCE);
        processScheduler.disableProcessor(testProcNode);
        assertCondition(() -> ScheduledState.RUNNING == testProcNode.getScheduledState(), LONG_DELAY_TOLERANCE);
        processScheduler.stopProcessor(testProcNode);
        assertCondition(() -> ScheduledState.STOPPED == testProcNode.getScheduledState(), LONG_DELAY_TOLERANCE);
    }

    /**
     * Validate that processor will not be validated on failing
     * PropertyDescriptor validation.
     */
    @Test(expected = IllegalStateException.class)
    public void validateStartFailsOnInvalidProcessorWithMissingProperty() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        processScheduler.startProcessor(testProcNode, true);
        fail();
    }

    /**
     * Validate that processor will not be validated on failing
     * ControllerService validation (not enabled).
     */
    @Test(expected = IllegalStateException.class)
    public void validateStartFailsOnInvalidProcessorWithDisabledService() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());

        ControllerServiceNode testServiceNode = flowManager.createControllerService(TestService.class.getName(), "serv",
                fcsb.getSystemBundle().getBundleDetails().getCoordinate(), null, true, true);
        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());

        properties.put("S", testServiceNode.getIdentifier());
        testProcNode.setProperties(properties);

        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        testProcessor.withService = true;

        processScheduler.startProcessor(testProcNode, true);
        fail();
    }

    /**
     * The successful processor start with ControllerService dependency.
     */
    @Test
    public void validateStartSucceedsOnProcessorWithEnabledService() throws Exception {
        final FlowManagerAndSystemBundle fcsb = this.buildFlowControllerForTest();
        flowManager = fcsb.getFlowManager();

        ProcessGroup testGroup = flowManager.createProcessGroup(UUID.randomUUID().toString());

        ControllerServiceNode testServiceNode = flowManager.createControllerService(TestService.class.getName(), "foo",
                fcsb.getSystemBundle().getBundleDetails().getCoordinate(), null, true, true);
        testGroup.addControllerService(testServiceNode);

        ProcessorNode testProcNode = flowManager.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString(),
                fcsb.getSystemBundle().getBundleDetails().getCoordinate());
        testGroup.addProcessor(testProcNode);

        properties.put("S", testServiceNode.getIdentifier());
        testProcNode.setProperties(properties);

        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        testProcessor.withService = true;
        this.noop(testProcessor);

        testServiceNode.performValidation();
        processScheduler.enableControllerService(testServiceNode);

        testProcNode.performValidation();
        processScheduler.startProcessor(testProcNode, true);

        Thread.sleep(500);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
    }


    /**
     * Scenario where onTrigger() is executed with random delay limited to
     * 'delayLimit', yet with guaranteed exit from onTrigger().
     */
    private void randomOnTriggerDelay(TestProcessor testProcessor, int delayLimit) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        RandomOrFixedDelayedRunnable delayedRunnable = new RandomOrFixedDelayedRunnable(delayLimit, true);
        testProcessor.setScenario(emptyRunnable, emptyRunnable, emptyRunnable, delayedRunnable);
    }

    /**
     * Scenario where @OnSchedule is executed with delay limited to
     * 'delayLimit'.
     */
    private void longRunningOnSchedule(TestProcessor testProcessor, int delayLimit) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        RandomOrFixedDelayedRunnable delayedRunnable = new RandomOrFixedDelayedRunnable(delayLimit, false);
        testProcessor.setScenario(delayedRunnable, emptyRunnable, emptyRunnable, emptyRunnable);
    }

    /**
     * Scenario where @OnUnschedule is executed with delay limited to
     * 'delayLimit'.
     */
    private void longRunningOnUnschedule(TestProcessor testProcessor, int delayLimit) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        RandomOrFixedDelayedRunnable delayedRunnable = new RandomOrFixedDelayedRunnable(delayLimit, false);
        testProcessor.setScenario(emptyRunnable, delayedRunnable, emptyRunnable, emptyRunnable);
    }

    /**
     * Scenario where @OnSchedule blocks indefinitely yet interruptible.
     */
    private void blockingInterruptableOnUnschedule(TestProcessor testProcessor) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        BlockingInterruptableRunnable blockingRunnable = new BlockingInterruptableRunnable();
        testProcessor.setScenario(blockingRunnable, emptyRunnable, emptyRunnable, emptyRunnable);
    }

    /**
     * Scenario where @OnSchedule blocks indefinitely and un-interruptible.
     */
    private void blockingUninterruptableOnUnschedule(TestProcessor testProcessor) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        BlockingUninterruptableRunnable blockingRunnable = new BlockingUninterruptableRunnable();
        testProcessor.setScenario(blockingRunnable, emptyRunnable, emptyRunnable, emptyRunnable);
    }

    /**
     * Scenario where all tasks are no op.
     */
    private void noop(TestProcessor testProcessor) {
        EmptyRunnable emptyRunnable = new EmptyRunnable();
        testProcessor.setScenario(emptyRunnable, emptyRunnable, emptyRunnable, emptyRunnable);
    }

    private FlowManagerAndSystemBundle buildFlowControllerForTest(final String propKey, final String propValue) throws Exception {
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "1 sec");
        addProps.put(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "target/test-classes/state-management.xml");
        addProps.put(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider");
        addProps.put(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        addProps.put("nifi.remote.input.socket.port", "");
        addProps.put("nifi.remote.input.secure", "");
        if (propKey != null && propValue != null) {
            addProps.put(propKey, propValue);
        }
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, addProps);

        final Bundle systemBundle = SystemBundle.create(nifiProperties);
        final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        final FlowController flowController = FlowController.createStandaloneInstance(mock(FlowFileEventRepository.class), nifiProperties,
            mock(Authorizer.class), mock(AuditService.class), null, new VolatileBulletinRepository(),
            new FileBasedVariableRegistry(nifiProperties.getVariableRegistryPropertiesPaths()),
            mock(FlowRegistryClient.class), extensionManager);

        final FlowManager flowManager = flowController.getFlowManager();
        this.processScheduler = flowController.getProcessScheduler();

        return new FlowManagerAndSystemBundle(flowManager, systemBundle);
    }

    private FlowManagerAndSystemBundle buildFlowControllerForTest() throws Exception {
        return buildFlowControllerForTest(null, null);
    }


    private static class FlowManagerAndSystemBundle {

        private final FlowManager flowManager;
        private final Bundle systemBundle;

        public FlowManagerAndSystemBundle(FlowManager flowManager, Bundle systemBundle) {
            this.flowManager = flowManager;
            this.systemBundle = systemBundle;
        }

        public FlowManager getFlowManager() {
            return flowManager;
        }

        public Bundle getSystemBundle() {
            return systemBundle;
        }
    }

    /**
     */
    public static class TestProcessor extends AbstractProcessor {
        private static final Runnable NOP = () -> {};

        private Runnable onScheduleCallback = NOP;
        private Runnable onUnscheduleCallback = NOP;
        private Runnable onStopCallback = NOP;
        private Runnable onTriggerCallback = NOP;

        private boolean generateExceptionOnScheduled;
        private boolean generateExceptionOnTrigger;

        private boolean withService;

        private int keepFailingOnScheduledTimes;

        private int onScheduledExceptionCount;

        private final List<String> operationNames = new LinkedList<>();

        void setScenario(Runnable onScheduleCallback, Runnable onUnscheduleCallback, Runnable onStopCallback,
                Runnable onTriggerCallback) {
            this.onScheduleCallback = onScheduleCallback;
            this.onUnscheduleCallback = onUnscheduleCallback;
            this.onStopCallback = onStopCallback;
            this.onTriggerCallback = onTriggerCallback;
        }

        @OnScheduled
        public void schedule(ProcessContext ctx) {
            this.operationNames.add("@OnScheduled");
            if (this.generateExceptionOnScheduled
                    && this.onScheduledExceptionCount++ < this.keepFailingOnScheduledTimes) {
                throw new RuntimeException("Intentional");
            }
            this.onScheduleCallback.run();
        }

        @OnUnscheduled
        public void unschedule() {
            this.operationNames.add("@OnUnscheduled");
            this.onUnscheduleCallback.run();
        }

        @OnStopped
        public void stop() {
            this.operationNames.add("@OnStopped");
            this.onStopCallback.run();
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            PropertyDescriptor PROP = new PropertyDescriptor.Builder()
                    .name("P")
                    .description("Blah Blah")
                    .required(true)
                    .addValidator(new Validator() {
                        @Override
                        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                            return new ValidationResult.Builder().subject(subject).input(value).valid(value != null && !value.isEmpty()).explanation(subject + " cannot be empty").build();
                        }
                    })
                    .build();

            PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
                    .name("S")
                    .description("Blah Blah")
                    .required(true)
                    .identifiesControllerService(ITestservice.class)
                    .build();

            return this.withService ? Arrays.asList(PROP, SERVICE) : Arrays.asList(PROP);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            if (this.generateExceptionOnTrigger) {
                throw new RuntimeException("Intentional");
            }
            this.onTriggerCallback.run();
        }
    }


    public static class TestService extends AbstractControllerService implements ITestservice {

    }

    public static interface ITestservice extends ControllerService {
    }


    private static class EmptyRunnable implements Runnable {

        @Override
        public void run() {

        }
    }


    private static class BlockingInterruptableRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    private static class BlockingUninterruptableRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }


    private static class RandomOrFixedDelayedRunnable implements Runnable {

        private final int delayLimit;
        private final boolean randomDelay;

        public RandomOrFixedDelayedRunnable(int delayLimit, boolean randomDelay) {
            this.delayLimit = delayLimit;
            this.randomDelay = randomDelay;
        }
        Random random = new Random();

        @Override
        public void run() {
            try {
                if (this.randomDelay) {
                    Thread.sleep(random.nextInt(this.delayLimit));
                } else {
                    Thread.sleep(this.delayLimit);
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while sleeping");
                Thread.currentThread().interrupt();
            }
        }
    }
}
