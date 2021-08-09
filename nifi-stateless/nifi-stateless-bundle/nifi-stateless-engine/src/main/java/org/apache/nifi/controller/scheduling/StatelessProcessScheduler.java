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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.SchedulingAgentCallback;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StatelessSchedulingAgent;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A ProcessScheduler that handles the lifecycle management of components but does not
 * schedule the triggering of components.
 */
public class StatelessProcessScheduler implements ProcessScheduler {
    private static final Logger logger = LoggerFactory.getLogger(StatelessProcessScheduler.class);
    private static final int ADMINISTRATIVE_YIELD_MILLIS = 1000;
    private static final int PROCESSOR_START_TIMEOUT_MILLIS = 10_000;

    private final SchedulingAgent schedulingAgent;
    private final ExtensionManager extensionManager;

    private FlowEngine componentLifeCycleThreadPool;
    private ScheduledExecutorService componentMonitoringThreadPool;
    private ProcessContextFactory processContextFactory;

    public StatelessProcessScheduler(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
        schedulingAgent = new StatelessSchedulingAgent(extensionManager);
    }

    @Override
    public void shutdown() {
        if (componentLifeCycleThreadPool != null) {
            componentLifeCycleThreadPool.shutdown();
        }

        if (componentMonitoringThreadPool != null) {
            componentMonitoringThreadPool.shutdown();
        }
    }

    @Override
    public void shutdownControllerService(final ControllerServiceNode serviceNode, final ControllerServiceProvider controllerServiceProvider) {
        final Class<?> serviceImplClass = serviceNode.getControllerServiceImplementation().getClass();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, serviceImplClass, serviceNode.getIdentifier())) {
            final ConfigurationContext configContext = new StandardConfigurationContext(serviceNode, controllerServiceProvider, null, VariableRegistry.EMPTY_REGISTRY);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, serviceNode.getControllerServiceImplementation(), configContext);
        }
    }

    @Override
    public void shutdownReportingTask(final ReportingTaskNode taskNode) {
        final ConfigurationContext configContext = taskNode.getConfigurationContext();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, taskNode.getReportingTask(), configContext);
        }
    }

    public void initialize(final ProcessContextFactory processContextFactory, final DataflowDefinition<?> dataflowDefinition) {
        this.processContextFactory = processContextFactory;

        final String threadNameSuffix = dataflowDefinition.getFlowName() == null ? "" : " for dataflow " + dataflowDefinition.getFlowName();
        componentLifeCycleThreadPool = new FlowEngine(8, "Component Lifecycle" + threadNameSuffix, true);
        componentMonitoringThreadPool = new FlowEngine(2, "Monitor Processor Lifecycle" + threadNameSuffix, true);
    }

    @Override
    public Future<Void> startProcessor(final ProcessorNode procNode, final boolean failIfStopping) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final SchedulingAgentCallback callback = new SchedulingAgentCallback() {
            @Override
            public void trigger() {
                // Initialization / scheduling has completed.
                future.complete(null);
            }

            @Override
            public Future<?> scheduleTask(final Callable<?> task) {
                return componentLifeCycleThreadPool.submit(task);
            }

            @Override
            public void onTaskComplete() {
            }
        };

        logger.info("Starting {}", procNode);

        final Supplier<ProcessContext> processContextSupplier = () -> processContextFactory.createProcessContext(procNode);
        procNode.start(componentMonitoringThreadPool, ADMINISTRATIVE_YIELD_MILLIS, PROCESSOR_START_TIMEOUT_MILLIS, processContextSupplier, callback, failIfStopping);
        return future;

    }

    @Override
    public Future<Void> runProcessorOnce(ProcessorNode procNode, final Callable<Future<Void>> stopCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> stopProcessor(final ProcessorNode procNode) {
        logger.info("Stopping {}", procNode);
        final ProcessContext processContext = processContextFactory.createProcessContext(procNode);
        final LifecycleState lifecycleState = new LifecycleState();
        lifecycleState.setScheduled(false);
        return procNode.stop(this, this.componentLifeCycleThreadPool, processContext, schedulingAgent, lifecycleState);
    }

    @Override
    public void terminateProcessor(final ProcessorNode procNode) {
    }

    @Override
    public void onProcessorRemoved(final ProcessorNode procNode) {
    }

    @Override
    public void onPortRemoved(final Port port) {
    }

    @Override
    public void onFunnelRemoved(final Funnel funnel) {
    }

    @Override
    public void onReportingTaskRemoved(final ReportingTaskNode reportingTask) {
    }

    @Override
    public void startPort(final Port port) {
        if (!port.isValid()) {
            throw new IllegalStateException("Port " + port.getIdentifier() + " is not in a valid state");
        }

        port.onSchedulingStart();
    }

    @Override
    public void stopPort(final Port port) {
    }

    @Override
    public void startFunnel(final Funnel funnel) {
    }

    @Override
    public void stopFunnel(final Funnel funnel) {
    }

    @Override
    public void enableFunnel(final Funnel funnel) {
    }

    @Override
    public void enablePort(final Port port) {
    }

    @Override
    public void enableProcessor(final ProcessorNode procNode) {
        procNode.enable();
    }

    @Override
    public void disableFunnel(final Funnel funnel) {
    }

    @Override
    public void disablePort(final Port port) {
    }

    @Override
    public void disableProcessor(final ProcessorNode procNode) {
        procNode.disable();
    }

    @Override
    public int getActiveThreadCount(final Object scheduled) {
        return 0;
    }

    @Override
    public boolean isScheduled(final Object scheduled) {
        return false;
    }

    @Override
    public void registerEvent(final Connectable worker) {
    }

    @Override
    public void setMaxThreadCount(final SchedulingStrategy strategy, final int maxThreadCount) {
    }

    @Override
    public void yield(final ProcessorNode procNode) {
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode) {
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode) {
        final Runnable scheduleTask = new Runnable() {
            @Override
            public void run() {
                try {
                    attemptSchedule(taskNode);
                    schedulingAgent.schedule(taskNode, new LifecycleState());
                    logger.info("Successfully scheduled {} to run every {}", taskNode, taskNode.getSchedulingPeriod());
                } catch (final Exception e) {
                    logger.error("Could not schedule {} to run. Will try again in 30 seconds.", taskNode, e);
                    componentLifeCycleThreadPool.schedule(this, 30, TimeUnit.SECONDS);
                }
            }
        };

        componentLifeCycleThreadPool.submit(scheduleTask);
    }

    private void attemptSchedule(final ReportingTaskNode taskNode) throws InvocationTargetException, IllegalAccessException {
        final ValidationStatus validation = taskNode.performValidation();
        if (validation != ValidationStatus.VALID) {
            throw new IllegalStateException("Cannot start Reporting Task " + taskNode + " because it is not valid: " + taskNode.getValidationErrors());
        }

        final ReportingTask reportingTask = taskNode.getReportingTask();
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, reportingTask.getClass(), taskNode.getIdentifier())) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, reportingTask, taskNode.getConfigurationContext());
        }
    }

    @Override
    public CompletableFuture<Void> enableControllerService(final ControllerServiceNode service) {
        logger.info("Enabling {}", service);
        return service.enable(componentLifeCycleThreadPool, ADMINISTRATIVE_YIELD_MILLIS);
    }

    @Override
    public CompletableFuture<Void> disableControllerService(final ControllerServiceNode service) {
        logger.info("Disabling {}", service);
        return service.disable(componentLifeCycleThreadPool);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final List<ControllerServiceNode> services) {
        if (services == null || services.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = null;
        for (ControllerServiceNode controllerServiceNode : services) {
            final CompletableFuture<Void> serviceFuture = this.disableControllerService(controllerServiceNode);

            if (future == null) {
                future = serviceFuture;
            } else {
                future = CompletableFuture.allOf(future, serviceFuture);
            }
        }

        return future;
    }

    @Override
    public Future<?> submitFrameworkTask(final Runnable task) {
        return null;
    }
}
