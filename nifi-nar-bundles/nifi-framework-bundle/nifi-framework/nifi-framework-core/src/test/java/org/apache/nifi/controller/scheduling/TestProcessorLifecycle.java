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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
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
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.MockProvenanceEventRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate Processor's life-cycle operation within the context of
 * {@link FlowController} and {@link StandardProcessScheduler}
 */
public class TestProcessorLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TestProcessorLifecycle.class);

    @Before
    public void before() {
        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");
        NiFiProperties.getInstance().setProperty(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "1 sec");
        NiFiProperties.getInstance().setProperty(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "target/test-classes/state-management.xml");
        NiFiProperties.getInstance().setProperty(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider");
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteDirectory(new File("./target/test-repo"));
        FileUtils.deleteDirectory(new File("./target/content_repository"));
    }

    @Test
    public void validateEnableOperation() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        final ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(),
                UUID.randomUUID().toString());

        assertEquals(ScheduledState.STOPPED, testProcNode.getScheduledState());
        assertEquals(ScheduledState.STOPPED, testProcNode.getLogicalScheduledState());
        // validates idempotency
        for (int i = 0; i < 2; i++) {
            testProcNode.enable();
        }
        assertEquals(ScheduledState.STOPPED, testProcNode.getScheduledState());
        assertEquals(ScheduledState.STOPPED, testProcNode.getLogicalScheduledState());
        testProcNode.disable();
        assertEquals(ScheduledState.DISABLED, testProcNode.getScheduledState());
        assertEquals(ScheduledState.DISABLED, testProcNode.getLogicalScheduledState());
        fc.shutdown(true);
    }


    @Test
    public void validateDisableOperation() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        final ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(),
                UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        assertEquals(ScheduledState.STOPPED, testProcNode.getScheduledState());
        assertEquals(ScheduledState.STOPPED, testProcNode.getLogicalScheduledState());
        // validates idempotency
        for (int i = 0; i < 2; i++) {
            testProcNode.disable();
        }
        assertEquals(ScheduledState.DISABLED, testProcNode.getScheduledState());
        assertEquals(ScheduledState.DISABLED, testProcNode.getLogicalScheduledState());

        ProcessScheduler ps = fc.getProcessScheduler();
        ps.startProcessor(testProcNode);
        assertEquals(ScheduledState.DISABLED, testProcNode.getLogicalScheduledState());

        fc.shutdown(true);
    }

    /**
     * Will validate the idempotent nature of processor start operation which
     * can be called multiple times without any side-effects.
     */
    @Test
    public void validateIdempotencyOfProcessorStartOperation() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        final ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        int randomDelayLimit = 3000;
        this.randomOnTriggerDelay(testProcessor, randomDelayLimit);
        final ProcessScheduler ps = fc.getProcessScheduler();
        ExecutorService executor = Executors.newCachedThreadPool();

        int startCallsCount = 100;
        final CountDownLatch countDownCounter = new CountDownLatch(startCallsCount);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        for (int i = 0; i < startCallsCount; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    ps.startProcessor(testProcNode);
                    countDownCounter.countDown();
                }
            });
        }

        assertTrue(countDownCounter.await(2000, TimeUnit.MILLISECONDS));
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
        // regardless of how many threads attempted to start Processor, it must
        // only be started once, hence have only single entry for @OnScheduled
        assertEquals(1, testProcessor.operationNames.size());
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
        fc.shutdown(true);
        executor.shutdownNow();
    }

    /**
     * Validates that stop calls are harmless and idempotent if processor is not
     * in STARTING or RUNNING state.
     */
    @Test
    public void validateStopCallsAreMeaninglessIfProcessorNotStarted() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        final ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        // sets the scenario for the processor to run
        int randomDelayLimit = 3000;
        this.randomOnTriggerDelay(testProcessor, randomDelayLimit);
        final ProcessScheduler ps = fc.getProcessScheduler();
        ps.stopProcessor(testProcNode);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        assertTrue(testProcessor.operationNames.size() == 0);
        fc.shutdown(true);
    }

    /**
     * Validates the processors start/stop sequence where the order of
     * operations can only be @OnScheduled, @OnUnscheduled, @OnStopped.
     */
    @Test
    public void validateSuccessfullAndOrderlyShutdown() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        int randomDelayLimit = 3000;
        this.randomOnTriggerDelay(testProcessor, randomDelayLimit);

        testProcNode.setMaxConcurrentTasks(4);
        testProcNode.setScheduldingPeriod("500 millis");
        testProcNode.setAutoTerminatedRelationships(Collections.singleton(new Relationship.Builder().name("success").build()));

        testGroup.addProcessor(testProcNode);

        fc.startProcessGroup(testGroup.getIdentifier());
        Thread.sleep(2000); // let it run for a while
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);

        fc.stopAllProcessors();

        Thread.sleep(randomDelayLimit); // up to randomDelayLimit, otherwise next assertion may fail as the processor still executing

        // validates that regardless of how many running tasks, lifecycle
        // operation are invoked atomically (once each).
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        // . . . hence only 3 operations must be in the list
        assertEquals(3, testProcessor.operationNames.size());
        // . . . and ordered as @OnScheduled, @OnUnscheduled, @OnStopped
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
        assertEquals("@OnUnscheduled", testProcessor.operationNames.get(1));
        assertEquals("@OnStopped", testProcessor.operationNames.get(2));

        fc.shutdown(true);
    }

    /**
     * Concurrency test that is basically hammers on both stop and start
     * operation validating their idempotency.
     */
    @Test
    public void validateLifecycleOperationOrderWithConcurrentCallsToStartStop() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        final ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);

        final ProcessScheduler ps = fc.getProcessScheduler();
        ExecutorService executor = Executors.newFixedThreadPool(100);
        int startCallsCount = 10000;
        final CountDownLatch countDownCounter = new CountDownLatch(startCallsCount);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        final Random random = new Random();
        for (int i = 0; i < startCallsCount / 2; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    LockSupport.parkNanos(random.nextInt(9000000));
                    ps.stopProcessor(testProcNode);
                    countDownCounter.countDown();
                }
            });
        }
        for (int i = 0; i < startCallsCount / 2; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    LockSupport.parkNanos(random.nextInt(9000000));
                    ps.startProcessor(testProcNode);
                    countDownCounter.countDown();
                }
            });
        }
        assertTrue(countDownCounter.await(10000, TimeUnit.MILLISECONDS));
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
        fc.shutdown(true);
    }

    /**
     * Validates that processor can be stopped before start sequence finished.
     */
    @Test
    public void validateProcessorUnscheduledAndStoppedWhenStopIsCalledBeforeProcessorFullyStarted() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        int delay = 2000;
        this.longRunningOnSchedule(testProcessor, delay);
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        assertTrue(testProcNode.getLogicalScheduledState() == ScheduledState.RUNNING);

        ps.stopProcessor(testProcNode);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPING);
        assertTrue(testProcNode.getLogicalScheduledState() == ScheduledState.STOPPED);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);

        assertEquals(2, testProcessor.operationNames.size());
        assertEquals("@OnScheduled", testProcessor.operationNames.get(0));
        assertEquals("@OnUnscheduled", testProcessor.operationNames.get(1));
        fc.shutdown(true);
    }

    /**
     * Validates that Processor is eventually started once invocation
     * of @OnSchedule stopped throwing exceptions.
     */
    @Test
    public void validateProcessScheduledAfterAdministrativeDelayDueToTheOnScheduledException() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);
        testProcessor.generateExceptionOnScheduled = true;
        testProcessor.keepFailingOnScheduledTimes = 2;
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
        ps.stopProcessor(testProcNode);
        Thread.sleep(500);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        fc.shutdown(true);
    }

    /**
     * Validates that Processor can be stopped when @OnScheduled constantly
     * fails. Basically validates that the re-try loop breaks if user initiated
     * stopProcessor.
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledConstantlyFails() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.longRunningOnUnschedule(testProcessor, 100);
        testProcessor.generateExceptionOnScheduled = true;
        testProcessor.keepFailingOnScheduledTimes = Integer.MAX_VALUE;
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        ps.stopProcessor(testProcNode);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPING);
        Thread.sleep(500);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        fc.shutdown(true);
    }

    /**
     * Validates that the Processor can be stopped when @OnScheduled blocks
     * indefinitely but written to react to thread interrupts
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledBlocksIndefinitelyInterruptable() throws Exception {
        NiFiProperties.getInstance().setProperty(NiFiProperties.PROCESSOR_START_TIMEOUT, "5 sec");
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        // sets the scenario for the processor to run
        this.blockingInterruptableOnUnschedule(testProcessor);
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        ps.stopProcessor(testProcNode);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPING);
        Thread.sleep(4000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        fc.shutdown(true);
    }

    /**
     * Validates that the Processor can be stopped when @OnScheduled blocks
     * indefinitely and written to ignore thread interrupts
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnScheduledBlocksIndefinitelyUninterruptable() throws Exception {
        NiFiProperties.getInstance().setProperty(NiFiProperties.PROCESSOR_START_TIMEOUT, "5 sec");
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        // sets the scenario for the processor to run
        this.blockingUninterruptableOnUnschedule(testProcessor);
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        ps.disableProcessor(testProcNode); // no effect
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STARTING);
        assertTrue(testProcNode.getLogicalScheduledState() == ScheduledState.RUNNING);
        ps.stopProcessor(testProcNode);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPING);
        assertTrue(testProcNode.getLogicalScheduledState() == ScheduledState.STOPPED);
        Thread.sleep(4000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        fc.shutdown(true);
    }

    /**
     * Validates that processor can be stopped if onTrigger() keeps trowing
     * exceptions.
     */
    @Test
    public void validateProcessorCanBeStoppedWhenOnTriggerThrowsException() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        testProcNode.setProperty("P", "hello");
        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();

        // sets the scenario for the processor to run
        this.noop(testProcessor);
        testProcessor.generateExceptionOnTrigger = true;
        ProcessScheduler ps = fc.getProcessScheduler();

        ps.startProcessor(testProcNode);
        Thread.sleep(1000);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
        ps.disableProcessor(testProcNode);
        Thread.sleep(100);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
        ps.stopProcessor(testProcNode);
        Thread.sleep(500);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.STOPPED);
        fc.shutdown(true);
    }

    /**
     * Validate that processor will not be validated on failing
     * PropertyDescriptor validation.
     */
    @Test(expected = IllegalStateException.class)
    public void validateStartFailsOnInvalidProcessorWithMissingProperty() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());
        ProcessScheduler ps = fc.getProcessScheduler();
        try {
            ps.startProcessor(testProcNode);
            fail();
        } finally {
            fc.shutdown(true);
        }
    }

    /**
     * Validate that processor will not be validated on failing
     * ControllerService validation (not enabled).
     */
    @Test(expected = IllegalStateException.class)
    public void validateStartFailsOnInvalidProcessorWithDisabledService() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);

        ControllerServiceNode testServiceNode = fc.createControllerService(TestService.class.getName(), "serv", true);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());


        testProcNode.setProperty("P", "hello");
        testProcNode.setProperty("S", testServiceNode.getIdentifier());

        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        testProcessor.withService = true;

        ProcessScheduler ps = fc.getProcessScheduler();
        try {
            ps.startProcessor(testProcNode);
            fail();
        } finally {
            fc.shutdown(true);
        }
    }

    /**
     * The successful processor start with ControllerService dependency.
     */
    @Test
    public void validateStartSucceedsOnProcessorWithEnabledService() throws Exception {
        FlowController fc = this.buildFlowControllerForTest();
        ProcessGroup testGroup = fc.createProcessGroup(UUID.randomUUID().toString());
        this.setControllerRootGroup(fc, testGroup);

        ControllerServiceNode testServiceNode = fc.createControllerService(TestService.class.getName(), "foo", true);
        ProcessorNode testProcNode = fc.createProcessor(TestProcessor.class.getName(), UUID.randomUUID().toString());

        testProcNode.setProperty("P", "hello");
        testProcNode.setProperty("S", testServiceNode.getIdentifier());

        TestProcessor testProcessor = (TestProcessor) testProcNode.getProcessor();
        testProcessor.withService = true;
        this.noop(testProcessor);

        ProcessScheduler ps = fc.getProcessScheduler();
        ps.enableControllerService(testServiceNode);
        ps.startProcessor(testProcNode);

        Thread.sleep(500);
        assertTrue(testProcNode.getScheduledState() == ScheduledState.RUNNING);
        fc.shutdown(true);
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

    /**
     *
     */
    private FlowController buildFlowControllerForTest() throws Exception {
        NiFiProperties properties = NiFiProperties.getInstance();
        properties.setProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceEventRepository.class.getName());
        properties.setProperty("nifi.remote.input.socket.port", "");
        properties.setProperty("nifi.remote.input.secure", "");

        return FlowController.createStandaloneInstance(mock(FlowFileEventRepository.class), properties,
                mock(UserService.class), mock(AuditService.class), null);
    }

    /**
     *
     */
    private void setControllerRootGroup(FlowController controller, ProcessGroup processGroup) {
        try {
            Method m = FlowController.class.getDeclaredMethod("setRootGroup", ProcessGroup.class);
            m.setAccessible(true);
            m.invoke(controller, processGroup);
            controller.initializeFlow();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set root group", e);
        }
    }

    /**
     */
    public static class TestProcessor extends AbstractProcessor {
        private Runnable onScheduleCallback;
        private Runnable onUnscheduleCallback;
        private Runnable onStopCallback;
        private Runnable onTriggerCallback;

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

            return this.withService ? Arrays.asList(new PropertyDescriptor[] { PROP, SERVICE })
                    : Arrays.asList(new PropertyDescriptor[] { PROP });
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            if (this.generateExceptionOnTrigger) {
                throw new RuntimeException("Intentional");
            }
            this.onTriggerCallback.run();
        }
    }

    /**
     */
    public static class TestService extends AbstractControllerService implements ITestservice {

    }

    /**
     */
    public static interface ITestservice extends ControllerService {

    }

    /**
     */
    private static class EmptyRunnable implements Runnable {
        @Override
        public void run() {

        }
    }

    /**
     */
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

    /**
     */
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

    /**
     */
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
