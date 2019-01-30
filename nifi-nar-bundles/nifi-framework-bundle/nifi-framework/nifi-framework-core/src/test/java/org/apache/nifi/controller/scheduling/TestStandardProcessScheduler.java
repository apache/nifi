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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StandardReportingTaskNode;
import org.apache.nifi.controller.scheduling.processors.FailOnScheduledProcessor;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestStandardProcessScheduler {

    private StandardProcessScheduler scheduler = null;
    private ReportingTaskNode taskNode = null;
    private TestReportingTask reportingTask = null;
    private final StateManagerProvider stateMgrProvider = Mockito.mock(StateManagerProvider.class);
    private VariableRegistry variableRegistry = VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY;
    private FlowController controller;
    private FlowManager flowManager;
    private ProcessGroup rootGroup;
    private NiFiProperties nifiProperties;
    private Bundle systemBundle;
    private ExtensionDiscoveringManager extensionManager;
    private ControllerServiceProvider serviceProvider;

    private volatile String propsFile = TestStandardProcessScheduler.class.getResource("/standardprocessschedulertest.nifi.properties").getFile();

    @Before
    public void setup() throws InitializationException {
        final Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "2 millis");
        overrideProperties.put(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT, "10 millis");
        this.nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, overrideProperties);

        // load the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        scheduler = new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class), null, stateMgrProvider, nifiProperties);
        scheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, Mockito.mock(SchedulingAgent.class));

        reportingTask = new TestReportingTask();
        final ReportingInitializationContext config = new StandardReportingInitializationContext(UUID.randomUUID().toString(), "Test", SchedulingStrategy.TIMER_DRIVEN, "5 secs",
                Mockito.mock(ComponentLog.class), null, KerberosConfig.NOT_CONFIGURED, null);
        reportingTask.initialize(config);

        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(null, variableRegistry);
        final TerminationAwareLogger logger = Mockito.mock(TerminationAwareLogger.class);
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final LoggableComponent<ReportingTask> loggableComponent = new LoggableComponent<>(reportingTask, systemBundle.getBundleDetails().getCoordinate(), logger);
        taskNode = new StandardReportingTaskNode(loggableComponent, UUID.randomUUID().toString(), Mockito.mock(FlowController.class), scheduler, validationContextFactory,
            new StandardComponentVariableRegistry(variableRegistry), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        flowManager = Mockito.mock(FlowManager.class);
        controller = Mockito.mock(FlowController.class);
        when(controller.getFlowManager()).thenReturn(flowManager);
        Mockito.when(controller.getExtensionManager()).thenReturn(extensionManager);

        serviceProvider = new StandardControllerServiceProvider(controller, scheduler, null);

        final ConcurrentMap<String, ProcessorNode> processorMap = new ConcurrentHashMap<>();
        Mockito.doAnswer(new Answer<ProcessorNode>() {
            @Override
            public ProcessorNode answer(InvocationOnMock invocation) {
                final String id = invocation.getArgumentAt(0, String.class);
                return processorMap.get(id);
            }
        }).when(flowManager).getProcessorNode(Mockito.anyString());

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                final ProcessorNode procNode = invocation.getArgumentAt(0, ProcessorNode.class);
                processorMap.putIfAbsent(procNode.getIdentifier(), procNode);
                return null;
            }
        }).when(flowManager).onProcessorAdded(any(ProcessorNode.class));

        when(controller.getControllerServiceProvider()).thenReturn(serviceProvider);

        rootGroup = new MockProcessGroup(controller);
        when(flowManager.getGroup(Mockito.anyString())).thenReturn(rootGroup);

        when(controller.getReloadComponent()).thenReturn(Mockito.mock(ReloadComponent.class));

        doAnswer(new Answer<ControllerServiceNode>() {
            @Override
            public ControllerServiceNode answer(final InvocationOnMock invocation) throws Throwable {
                final String type = invocation.getArgumentAt(0, String.class);
                final String id = invocation.getArgumentAt(1, String.class);
                final BundleCoordinate bundleCoordinate = invocation.getArgumentAt(2, BundleCoordinate.class);

                final ControllerServiceNode serviceNode = new ExtensionBuilder()
                    .identifier(id)
                    .type(type)
                    .bundleCoordinate(bundleCoordinate)
                    .controllerServiceProvider(serviceProvider)
                    .processScheduler(Mockito.mock(ProcessScheduler.class))
                    .nodeTypeProvider(Mockito.mock(NodeTypeProvider.class))
                    .validationTrigger(Mockito.mock(ValidationTrigger.class))
                    .reloadComponent(Mockito.mock(ReloadComponent.class))
                    .variableRegistry(variableRegistry)
                    .stateManagerProvider(Mockito.mock(StateManagerProvider.class))
                    .extensionManager(extensionManager)
                    .buildControllerService();

                serviceProvider.onControllerServiceAdded(serviceNode);
                return serviceNode;
            }
        }).when(flowManager).createControllerService(anyString(), anyString(), any(BundleCoordinate.class), anySet(), anyBoolean(), anyBoolean());
    }

    @After
    public void after() throws Exception {
        controller.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/standardprocessschedulertest"));
    }

    /**
     * We have run into an issue where a Reporting Task is scheduled to run but
     * throws an Exception from a method with the @OnScheduled annotation. User
     * stops Reporting Task, updates configuration to fix the issue. Reporting
     * Task then finishes running @OnSchedule method and is then scheduled to
     * run. This unit test is intended to verify that we have this resolved.
     */
    @Test
    public void testReportingTaskDoesntKeepRunningAfterStop() throws InterruptedException {
        taskNode.performValidation();
        scheduler.schedule(taskNode);

        // Let it try to run a few times.
        Thread.sleep(25L);

        scheduler.unschedule(taskNode);

        final int attempts = reportingTask.onScheduleAttempts.get();
        // give it a sec to make sure that it's finished running.
        Thread.sleep(250L);
        final int attemptsAfterStop = reportingTask.onScheduleAttempts.get() - attempts;

        // allow 1 extra run, due to timing issues that could call it as it's being stopped.
        assertTrue("After unscheduling Reporting Task, task ran an additional " + attemptsAfterStop + " times", attemptsAfterStop <= 1);
    }

    @Test(timeout = 60000)
    public void testDisableControllerServiceWithProcessorTryingToStartUsingIt() throws InterruptedException {
        final String uuid = UUID.randomUUID().toString();
        final Processor proc = new ServiceReferencingProcessor();
        proc.initialize(new StandardProcessorInitializationContext(uuid, null, null, null, KerberosConfig.NOT_CONFIGURED));

        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);

        final ControllerServiceNode service = flowManager.createControllerService(NoStartServiceImpl.class.getName(), "service",
                systemBundle.getBundleDetails().getCoordinate(), null, true, true);

        rootGroup.addControllerService(service);

        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, variableRegistry);
        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, uuid, validationContextFactory, scheduler,
            serviceProvider, new StandardComponentVariableRegistry(VariableRegistry.EMPTY_REGISTRY), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        Map<String, String> procProps = new HashMap<>();
        procProps.put(ServiceReferencingProcessor.SERVICE_DESC.getName(), service.getIdentifier());
        procNode.setProperties(procProps);

        service.performValidation();
        scheduler.enableControllerService(service);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);

        Thread.sleep(25L);

        scheduler.stopProcessor(procNode);
        assertTrue(service.isActive());
        assertSame(service.getState(), ControllerServiceState.ENABLING);
        scheduler.disableControllerService(service);
        assertSame(service.getState(), ControllerServiceState.DISABLING);
        assertFalse(service.isActive());

        while (service.getState() != ControllerServiceState.DISABLED) {
            Thread.sleep(5L);
        }
        assertSame(service.getState(), ControllerServiceState.DISABLED);
    }

    public class TestReportingTask extends AbstractReportingTask {

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

    public static class ServiceReferencingProcessor extends AbstractProcessor {

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
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
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
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true);

        serviceNode.performValidation();

        assertFalse(serviceNode.isActive());
        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        final ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.enableControllerService(serviceNode);
                        assertTrue(serviceNode.isActive());
                    } catch (final Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

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
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true);
        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        final ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.disableControllerService(serviceNode);
                        assertFalse(serviceNode.isActive());
                    } catch (final Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
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
        final StandardProcessScheduler scheduler = createScheduler();
        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true);

        assertSame(ValidationStatus.VALID, serviceNode.performValidation());

        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        scheduler.enableControllerService(serviceNode).get();
        assertTrue(serviceNode.isActive());
        final ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scheduler.disableControllerService(serviceNode);
                        assertFalse(serviceNode.isActive());
                    } catch (final Exception e) {
                        e.printStackTrace();
                        asyncFailed.set(true);
                    }
                }
            });
        }
        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS); // change to seconds.
        assertFalse(asyncFailed.get());
        assertEquals(1, ts.disableInvocationCount());
    }

    @Test
    public void validateDisablingOfTheFailedService() throws Exception {
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(FailingService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true);
        serviceNode.performValidation();

        final Future<?> future = scheduler.enableControllerService(serviceNode);
        try {
            future.get();
        } catch (final Exception e) {
            // Expected behavior because the FailingService throws Exception when attempting to enable
        }

        scheduler.shutdown();

        /*
         * Because it was never disabled it will remain active since its
         * enabling is being retried. This may actually be a bug in the
         * scheduler since it probably has to shut down all components (disable
         * services, shut down processors etc) before shutting down itself
         */
        assertTrue(serviceNode.isActive());
        assertSame(serviceNode.getState(), ControllerServiceState.ENABLING);
    }

    /**
     * Validates that in multi threaded environment enabling service can still
     * be disabled. This test is set up in such way that disabling of the
     * service could be initiated by both disable and enable methods. In other
     * words it tests two conditions in
     * {@link StandardControllerServiceNode#disable(ScheduledExecutorService)}
     * where the disabling of the service can be initiated right there (if
     * ENABLED), or if service is still enabling its disabling will be deferred
     * to the logic in
     * {@link StandardControllerServiceNode#enable(ScheduledExecutorService, long)}
     * IN any even the resulting state of the service is DISABLED
     */
    @Test
    @Ignore
    public void validateEnabledDisableMultiThread() throws Exception {
        final StandardProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(controller, scheduler, null);
        final ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 200; i++) {
            final ControllerServiceNode serviceNode = flowManager.createControllerService(RandomShortDelayEnablingService.class.getName(), "1",
                    systemBundle.getBundleDetails().getCoordinate(), null, false, true);

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    scheduler.enableControllerService(serviceNode);
                }
            });
            Thread.sleep(10); // ensure that enable gets initiated before disable
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    scheduler.disableControllerService(serviceNode);
                }
            });
            Thread.sleep(100);
            assertFalse(serviceNode.isActive());
            assertEquals(ControllerServiceState.DISABLED, serviceNode.getState());
        }

        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        Thread.sleep(500);
        executor.shutdown();
        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
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
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(LongEnablingService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true);

        final LongEnablingService ts = (LongEnablingService) serviceNode.getControllerServiceImplementation();
        ts.setLimit(Long.MAX_VALUE);

        serviceNode.performValidation();
        scheduler.enableControllerService(serviceNode);

        assertTrue(serviceNode.isActive());
        final long maxTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ts.enableInvocationCount() != 1 && System.nanoTime() <= maxTime) {
            Thread.sleep(1L);
        }
        assertEquals(1, ts.enableInvocationCount());

        scheduler.disableControllerService(serviceNode);
        assertFalse(serviceNode.isActive());
        assertEquals(ControllerServiceState.DISABLING, serviceNode.getState());
        assertEquals(0, ts.disableInvocationCount());
    }

    // Test that if processor throws Exception in @OnScheduled, it keeps getting scheduled
    @Test(timeout = 10000)
    public void testProcessorThrowsExceptionOnScheduledRetry() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(3);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider, variableRegistry),
            scheduler, serviceProvider, new StandardComponentVariableRegistry(VariableRegistry.EMPTY_REGISTRY), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        procNode.performValidation();
        rootGroup.addProcessor(procNode);

        scheduler.startProcessor(procNode, true);
        while (!proc.isSucceess()) {
            Thread.sleep(5L);
        }

        assertEquals(3, proc.getOnScheduledInvocationCount());
    }

    // Test that if processor times out in the @OnScheduled but responds to interrupt, it keeps getting scheduled
    @Test(timeout = 10000)
    public void testProcessorTimeOutRespondsToInterrupt() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(0);
        proc.setOnScheduledSleepDuration(20, TimeUnit.MINUTES, true, 1);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider, variableRegistry),
            scheduler, serviceProvider, new StandardComponentVariableRegistry(VariableRegistry.EMPTY_REGISTRY), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);
        while (!proc.isSucceess()) {
            Thread.sleep(5L);
        }

        // The first time that the processor's @OnScheduled method is called, it will sleep for 20 minutes. The scheduler should interrupt
        // that thread and then try again. The second time, the Processor will not sleep because setOnScheduledSleepDuration was called
        // above with iterations = 1
        assertEquals(2, proc.getOnScheduledInvocationCount());
    }

    // Test that if processor times out in the @OnScheduled and does not respond to interrupt, it is not scheduled again
    @Test(timeout = 10000)
    public void testProcessorTimeOutNoResponseToInterrupt() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(0);
        proc.setOnScheduledSleepDuration(20, TimeUnit.MINUTES, false, 1);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider, variableRegistry),
            scheduler, serviceProvider, new StandardComponentVariableRegistry(VariableRegistry.EMPTY_REGISTRY), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);

        Thread.sleep(100L);
        assertEquals(1, proc.getOnScheduledInvocationCount());
        Thread.sleep(100L);
        assertEquals(1, proc.getOnScheduledInvocationCount());

        // Allow test to complete.
        proc.setAllowSleepInterrupt(true);
    }


    public static class FailingService extends AbstractControllerService {

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            throw new RuntimeException("intentional");
        }
    }

    public static class RandomShortDelayEnablingService extends AbstractControllerService {
        private final Random random = new Random();

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            try {
                Thread.sleep(random.nextInt(20));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class SimpleTestService extends AbstractControllerService {
        private final AtomicInteger enableCounter = new AtomicInteger();
        private final AtomicInteger disableCounter = new AtomicInteger();

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            this.enableCounter.incrementAndGet();
        }

        @OnDisabled
        public void disable(final ConfigurationContext context) {
            this.disableCounter.incrementAndGet();
        }

        public int enableInvocationCount() {
            return this.enableCounter.get();
        }

        public int disableInvocationCount() {
            return this.disableCounter.get();
        }
    }

    private StandardProcessScheduler createScheduler() {
        return new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class), null, stateMgrProvider, nifiProperties);
    }
}
