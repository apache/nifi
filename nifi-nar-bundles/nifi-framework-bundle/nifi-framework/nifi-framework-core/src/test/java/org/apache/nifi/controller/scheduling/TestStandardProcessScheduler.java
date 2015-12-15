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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.Heartbeater;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StandardReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStandardProcessScheduler {
    private StandardProcessScheduler scheduler = null;
    private ReportingTaskNode taskNode = null;
    private TestReportingTask reportingTask = null;

    @Before
    public void setup() throws InitializationException {
        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");
        this.refreshNiFiProperties();
        scheduler = new StandardProcessScheduler(Mockito.mock(Heartbeater.class), Mockito.mock(ControllerServiceProvider.class), null);
        scheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, Mockito.mock(SchedulingAgent.class));

        reportingTask = new TestReportingTask();
        final ReportingInitializationContext config = new StandardReportingInitializationContext(UUID.randomUUID().toString(), "Test", SchedulingStrategy.TIMER_DRIVEN, "5 secs",
            Mockito.mock(ComponentLog.class), null);
        reportingTask.initialize(config);

        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(null);
        taskNode = new StandardReportingTaskNode(reportingTask, UUID.randomUUID().toString(), null, scheduler, validationContextFactory);
    }

    /**
     * We have run into an issue where a Reporting Task is scheduled to run but throws an Exception
     * from a method with the @OnScheduled annotation. User stops Reporting Task, updates configuration
     * to fix the issue. Reporting Task then finishes running @OnSchedule method and is then scheduled to run.
     * This unit test is intended to verify that we have this resolved.
     */
    @Test
    public void testReportingTaskDoesntKeepRunningAfterStop() throws InterruptedException, InitializationException {
        scheduler.schedule(taskNode);

        // Let it try to run a few times.
        Thread.sleep(1000L);

        scheduler.unschedule(taskNode);

        final int attempts = reportingTask.onScheduleAttempts.get();
        // give it a sec to make sure that it's finished running.
        Thread.sleep(1500L);
        final int attemptsAfterStop = reportingTask.onScheduleAttempts.get() - attempts;

        // allow 1 extra run, due to timing issues that could call it as it's being stopped.
        assertTrue("After unscheduling Reporting Task, task ran an additional " + attemptsAfterStop + " times", attemptsAfterStop <= 1);
    }

    @Test(timeout = 6000)
    public void testDisableControllerServiceWithProcessorTryingToStartUsingIt() throws InterruptedException {
        final Processor proc = new ServiceReferencingProcessor();

        final ControllerServiceProvider serviceProvider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode service = serviceProvider.createControllerService(NoStartServiceImpl.class.getName(), "service", true);
        final ProcessorNode procNode = new StandardProcessorNode(proc, UUID.randomUUID().toString(),
                new StandardValidationContextFactory(serviceProvider), scheduler, serviceProvider);

        procNode.setProperty(ServiceReferencingProcessor.SERVICE_DESC.getName(), service.getIdentifier());

        scheduler.enableControllerService(service);
        scheduler.startProcessor(procNode);

        Thread.sleep(1000L);

        scheduler.stopProcessor(procNode);
        assertTrue(service.isActive());
        assertTrue(service.getState() == ControllerServiceState.ENABLING);
        scheduler.disableControllerService(service);
        assertTrue(service.getState() == ControllerServiceState.DISABLING);
        assertFalse(service.isActive());
        Thread.sleep(1000);
        assertTrue(service.getState() == ControllerServiceState.DISABLED);
    }


    private class TestReportingTask extends AbstractReportingTask {
        private final AtomicBoolean failOnScheduled = new AtomicBoolean(true);
        private final AtomicInteger onScheduleAttempts = new AtomicInteger(0);
        private final AtomicInteger triggerCount = new AtomicInteger(0);

        @OnScheduled
        public void onScheduled() {
            onScheduleAttempts.incrementAndGet();

            if (failOnScheduled.get()) {
                throw new RuntimeException("Intentional Exception for testing purposes");
            }
        }

        @Override
        public void onTrigger(final ReportingContext context) {
            triggerCount.getAndIncrement();
        }
    }


    private static class ServiceReferencingProcessor extends AbstractProcessor {
        static final PropertyDescriptor SERVICE_DESC = new PropertyDescriptor.Builder()
                .name("service")
                .identifiesControllerService(NoStartService.class)
                .required(true)
                .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            final List<PropertyDescriptor> properties = new ArrayList<>();
            properties.add(SERVICE_DESC);
            return properties;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }

    private void refreshNiFiProperties() {
        try {
            Field instanceField = NiFiProperties.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    /**
     * Validates the atomic nature of ControllerServiceNode.enable() method
     * which must only trigger @OnEnabled once, regardless of how many threads
     * may have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateServiceEnablementLogicHappensOnlyOnce() throws Exception {
        final ProcessScheduler scheduler = createScheduler();
        StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode serviceNode = provider.createControllerService(SimpleTestService.class.getName(),
                "1", false);
        assertFalse(serviceNode.isActive());
        SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.enableControllerService(serviceNode);
                        assertTrue(serviceNode.isActive());
                    } catch (Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }
        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        Thread.sleep(500);
        executor.shutdown();
        assertFalse(asyncFailed.get());
        assertEquals(1, ts.enableInvocationCount());
    }

    /**
     * Validates the atomic nature of ControllerServiceNode.disable(..) method
     * which must never trigger @OnDisabled, regardless of how many threads may
     * have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateDisabledServiceCantBeDisabled() throws Exception {
        final ProcessScheduler scheduler = createScheduler();
        StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode serviceNode = provider.createControllerService(SimpleTestService.class.getName(),
                "1", false);
        SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.disableControllerService(serviceNode);
                        assertFalse(serviceNode.isActive());
                    } catch (Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }
        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        Thread.sleep(500);
        executor.shutdown();
        assertFalse(asyncFailed.get());
        assertEquals(0, ts.disableInvocationCount());
    }

    /**
     * Validates the atomic nature of ControllerServiceNode.disable() method
     * which must only trigger @OnDisabled once, regardless of how many threads
     * may have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateEnabledServiceCanOnlyBeDisabledOnce() throws Exception {
        final ProcessScheduler scheduler = createScheduler();
        StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode serviceNode = provider.createControllerService(SimpleTestService.class.getName(),
                "1", false);
        SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        scheduler.enableControllerService(serviceNode);
        assertTrue(serviceNode.isActive());
        ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.disableControllerService(serviceNode);
                        assertFalse(serviceNode.isActive());
                    } catch (Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }
        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        Thread.sleep(500);
        executor.shutdown();
        assertFalse(asyncFailed.get());
        assertEquals(1, ts.disableInvocationCount());
    }

    /**
     * Validates that service that is infinitely blocking in @OnEnabled can
     * still have DISABLE operation initiated. The service itself will be set to
     * DISABLING state at which point UI and all will know that such service can
     * not be transitioned any more into any other state until it finishes
     * enabling (which will never happen in our case thus should be addressed by
     * user). However, regardless of user's mistake NiFi will remain
     * functioning.
     */
    @Test
    public void validateNeverEnablingServiceCanStillBeDisabled() throws Exception {
        final ProcessScheduler scheduler = createScheduler();
        StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode serviceNode = provider.createControllerService(LongEnablingService.class.getName(),
                "1", false);
        LongEnablingService ts = (LongEnablingService) serviceNode.getControllerServiceImplementation();
        ts.setLimit(Long.MAX_VALUE);
        scheduler.enableControllerService(serviceNode);
        Thread.sleep(100);
        assertTrue(serviceNode.isActive());
        assertEquals(1, ts.enableInvocationCount());

        Thread.sleep(1000);
        scheduler.disableControllerService(serviceNode);
        assertFalse(serviceNode.isActive());
        assertEquals(ControllerServiceState.DISABLING, serviceNode.getState());
        assertEquals(0, ts.disableInvocationCount());
    }

    /**
     * Validates that the service that is currently in ENABLING state can be
     * disabled and that its @OnDisabled operation will be invoked as soon
     * as @OnEnable finishes.
     */
    @Test
    public void validateLongEnablingServiceCanStillBeDisabled() throws Exception {
        final ProcessScheduler scheduler = createScheduler();
        StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);
        final ControllerServiceNode serviceNode = provider.createControllerService(LongEnablingService.class.getName(),
                "1", false);
        LongEnablingService ts = (LongEnablingService) serviceNode.getControllerServiceImplementation();
        ts.setLimit(3000);
        scheduler.enableControllerService(serviceNode);
        Thread.sleep(100);
        assertTrue(serviceNode.isActive());
        assertEquals(1, ts.enableInvocationCount());

        Thread.sleep(100);
        scheduler.disableControllerService(serviceNode);
        assertFalse(serviceNode.isActive());
        assertEquals(ControllerServiceState.DISABLING, serviceNode.getState());
        assertEquals(0, ts.disableInvocationCount());
        // wait a bit. . . Enabling will finish and @OnDisabled will be invoked
        // automatically
        Thread.sleep(3000);
        assertEquals(ControllerServiceState.DISABLED, serviceNode.getState());
        assertEquals(1, ts.disableInvocationCount());
    }

    public static class SimpleTestService extends AbstractControllerService {

        private final AtomicInteger enableCounter = new AtomicInteger();
        private final AtomicInteger disableCounter = new AtomicInteger();

        @OnEnabled
        public void enable(ConfigurationContext context) {
            this.enableCounter.incrementAndGet();
        }

        @OnDisabled
        public void disable(ConfigurationContext context) {
            this.disableCounter.incrementAndGet();
        }

        public int enableInvocationCount() {
            return this.enableCounter.get();
        }

        public int disableInvocationCount() {
            return this.disableCounter.get();
        }
    }

    public static class LongEnablingService extends AbstractControllerService {
        private final AtomicInteger enableCounter = new AtomicInteger();
        private final AtomicInteger disableCounter = new AtomicInteger();

        private volatile long limit;

        @OnEnabled
        public void enable(ConfigurationContext context) throws Exception {
            this.enableCounter.incrementAndGet();
            Thread.sleep(limit);
        }

        @OnDisabled
        public void disable(ConfigurationContext context) {
            this.disableCounter.incrementAndGet();
        }

        public int enableInvocationCount() {
            return this.enableCounter.get();
        }

        public int disableInvocationCount() {
            return this.disableCounter.get();
        }

        public void setLimit(long limit) {
            this.limit = limit;
        }
    }

    private ProcessScheduler createScheduler() {
        return new StandardProcessScheduler(mock(Heartbeater.class), null, null);
    }
}
