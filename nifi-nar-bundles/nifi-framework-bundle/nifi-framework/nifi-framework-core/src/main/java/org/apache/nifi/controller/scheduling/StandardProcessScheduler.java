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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.SchedulingAgentCallback;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Responsible for scheduling Processors, Ports, and Funnels to run at regular intervals
 */
public final class StandardProcessScheduler implements ProcessScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessScheduler.class);

    private final FlowController flowController;
    private final long administrativeYieldMillis;
    private final String administrativeYieldDuration;
    private final StateManagerProvider stateManagerProvider;
    private final long processorStartTimeoutMillis;

    private final ConcurrentMap<Object, LifecycleState> lifecycleStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService frameworkTaskExecutor;
    private final ConcurrentMap<SchedulingStrategy, SchedulingAgent> strategyAgentMap = new ConcurrentHashMap<>();

    // thread pool for starting/stopping components
    private final ScheduledExecutorService componentLifeCycleThreadPool;
    private final ScheduledExecutorService componentMonitoringThreadPool = new FlowEngine(2, "Monitor Processor Lifecycle", true);

    private final StringEncryptor encryptor;

    public StandardProcessScheduler(final FlowEngine componentLifecycleThreadPool, final FlowController flowController, final StringEncryptor encryptor,
        final StateManagerProvider stateManagerProvider, final NiFiProperties nifiProperties) {
        this.componentLifeCycleThreadPool = componentLifecycleThreadPool;
        this.flowController = flowController;
        this.encryptor = encryptor;
        this.stateManagerProvider = stateManagerProvider;

        administrativeYieldDuration = nifiProperties.getAdministrativeYieldDuration();
        administrativeYieldMillis = FormatUtils.getTimeDuration(administrativeYieldDuration, TimeUnit.MILLISECONDS);

        final String timeoutString = nifiProperties.getProperty(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT);
        processorStartTimeoutMillis = timeoutString == null ? 60000 : FormatUtils.getTimeDuration(timeoutString.trim(), TimeUnit.MILLISECONDS);

        frameworkTaskExecutor = new FlowEngine(4, "Framework Task Thread");
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return flowController.getControllerServiceProvider();
    }

    private StateManager getStateManager(final String componentId) {
        return stateManagerProvider.getStateManager(componentId);
    }

    public void scheduleFrameworkTask(final Runnable command, final String taskName, final long initialDelay, final long delay, final TimeUnit timeUnit) {
        frameworkTaskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    command.run();
                } catch (final Throwable t) {
                    LOG.error("Failed to run Framework Task {} due to {}", taskName, t.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.error("", t);
                    }
                }
            }
        }, initialDelay, delay, timeUnit);
    }

    /**
     * Submits the given task to be executed exactly once in a background thread
     *
     * @param task the task to perform
     */
    public Future<?> submitFrameworkTask(final Runnable task) {
        return frameworkTaskExecutor.submit(task);
    }

    @Override
    public void setMaxThreadCount(final SchedulingStrategy schedulingStrategy, final int maxThreadCount) {
        final SchedulingAgent agent = getSchedulingAgent(schedulingStrategy);
        if (agent == null) {
            return;
        }

        agent.setMaxThreadCount(maxThreadCount);
    }

    public void setSchedulingAgent(final SchedulingStrategy strategy, final SchedulingAgent agent) {
        strategyAgentMap.put(strategy, agent);
    }

    public SchedulingAgent getSchedulingAgent(final SchedulingStrategy strategy) {
        return strategyAgentMap.get(strategy);
    }

    private SchedulingAgent getSchedulingAgent(final Connectable connectable) {
        return getSchedulingAgent(connectable.getSchedulingStrategy());
    }

    @Override
    public void shutdown() {
        for (final SchedulingAgent schedulingAgent : strategyAgentMap.values()) {
            try {
                schedulingAgent.shutdown();
            } catch (final Throwable t) {
                LOG.error("Failed to shutdown Scheduling Agent {} due to {}", schedulingAgent, t.toString());
                LOG.error("", t);
            }
        }

        frameworkTaskExecutor.shutdown();
        componentLifeCycleThreadPool.shutdown();
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode) {
        final LifecycleState lifecycleState = getLifecycleState(requireNonNull(taskNode), true);
        if (lifecycleState.isScheduled()) {
            return;
        }

        final int activeThreadCount = lifecycleState.getActiveThreadCount();
        if (activeThreadCount > 0) {
            throw new IllegalStateException("Reporting Task " + taskNode.getName() + " cannot be started because it has " + activeThreadCount + " threads still running");
        }

        final SchedulingAgent agent = getSchedulingAgent(taskNode.getSchedulingStrategy());
        lifecycleState.setScheduled(true);

        final Runnable startReportingTaskRunnable = new Runnable() {
            @Override
            public void run() {
                final long lastStopTime = lifecycleState.getLastStopTime();
                final ReportingTask reportingTask = taskNode.getReportingTask();

                // Attempt to start the Reporting Task, and if we fail re-schedule the task again after #administrativeYielMillis milliseconds
                try {
                    synchronized (lifecycleState) {
                        // if no longer scheduled to run, then we're finished. This can happen, for example,
                        // if the @OnScheduled method throws an Exception and the user stops the reporting task
                        // while we're administratively yielded.
                        // we also check if the schedule state's last start time is equal to what it was before.
                        // if not, then means that the reporting task has been stopped and started again, so we should just
                        // bail; another thread will be responsible for invoking the @OnScheduled methods.
                        if (!lifecycleState.isScheduled() || lifecycleState.getLastStopTime() != lastStopTime) {
                            LOG.debug("Did not complete invocation of @OnScheduled task for {} but Lifecycle State is no longer scheduled. Will not attempt to invoke task anymore", reportingTask);
                            return;
                        }

                        final ValidationStatus validationStatus = taskNode.getValidationStatus();
                        if (validationStatus != ValidationStatus.VALID) {
                            LOG.debug("Cannot schedule {} to run because it is currently invalid. Will try again in 5 seconds", taskNode);
                            componentLifeCycleThreadPool.schedule(this, 5, TimeUnit.SECONDS);
                            return;
                        }

                        try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), reportingTask.getClass(), reportingTask.getIdentifier())) {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, reportingTask, taskNode.getConfigurationContext());
                        }

                        agent.schedule(taskNode, lifecycleState);
                    }
                } catch (final Exception e) {
                    final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                    final ComponentLog componentLog = new SimpleProcessLogger(reportingTask.getIdentifier(), reportingTask);
                    componentLog.error("Failed to invoke @OnScheduled method due to {}", cause);

                    LOG.error("Failed to invoke the On-Scheduled Lifecycle methods of {} due to {}; administratively yielding this "
                            + "ReportingTask and will attempt to schedule it again after {}",
                            new Object[]{reportingTask, e.toString(), administrativeYieldDuration}, e);


                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), reportingTask.getClass(), reportingTask.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, reportingTask, taskNode.getConfigurationContext());
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, reportingTask, taskNode.getConfigurationContext());
                    }

                    componentLifeCycleThreadPool.schedule(this, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                }
            }
        };

        componentLifeCycleThreadPool.execute(startReportingTaskRunnable);
        taskNode.setScheduledState(ScheduledState.RUNNING);
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode) {
        final LifecycleState lifecycleState = getLifecycleState(requireNonNull(taskNode), false);
        if (!lifecycleState.isScheduled()) {
            return;
        }

        taskNode.verifyCanStop();
        final SchedulingAgent agent = getSchedulingAgent(taskNode.getSchedulingStrategy());
        final ReportingTask reportingTask = taskNode.getReportingTask();
        taskNode.setScheduledState(ScheduledState.STOPPED);

        final Runnable unscheduleReportingTaskRunnable = new Runnable() {
            @Override
            public void run() {
                final ConfigurationContext configurationContext = taskNode.getConfigurationContext();

                synchronized (lifecycleState) {
                    lifecycleState.setScheduled(false);

                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), reportingTask.getClass(), reportingTask.getIdentifier())) {
                        ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, reportingTask, configurationContext);
                    } catch (final Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(reportingTask.getIdentifier(), reportingTask);
                        componentLog.error("Failed to invoke @OnUnscheduled method due to {}", cause);

                        LOG.error("Failed to invoke the @OnUnscheduled methods of {} due to {}; administratively yielding this ReportingTask and will attempt to schedule it again after {}",
                                reportingTask, cause.toString(), administrativeYieldDuration);
                        LOG.error("", cause);
                    }

                    agent.unschedule(taskNode, lifecycleState);

                    if (lifecycleState.getActiveThreadCount() == 0 && lifecycleState.mustCallOnStoppedMethods()) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, reportingTask, configurationContext);
                    }
                }
            }
        };

        componentLifeCycleThreadPool.execute(unscheduleReportingTaskRunnable);
    }

    /**
     * Starts the given {@link Processor} by invoking its
     * {@link ProcessorNode#start(ScheduledExecutorService, long, long, ProcessContext, SchedulingAgentCallback, boolean)}
     * method.
     *
     * @see StandardProcessorNode#start(ScheduledExecutorService, long, long, ProcessContext, SchedulingAgentCallback, boolean)
     */
    @Override
    public synchronized CompletableFuture<Void> startProcessor(final ProcessorNode procNode, final boolean failIfStopping) {
        final LifecycleState lifecycleState = getLifecycleState(requireNonNull(procNode), true);

        final StandardProcessContext processContext = new StandardProcessContext(procNode, getControllerServiceProvider(),
            this.encryptor, getStateManager(procNode.getIdentifier()), lifecycleState::isTerminated);

        final CompletableFuture<Void> future = new CompletableFuture<>();
        final SchedulingAgentCallback callback = new SchedulingAgentCallback() {
            @Override
            public void trigger() {
                lifecycleState.clearTerminationFlag();
                getSchedulingAgent(procNode).schedule(procNode, lifecycleState);
                future.complete(null);
            }

            @Override
            public Future<?> scheduleTask(final Callable<?> task) {
                lifecycleState.incrementActiveThreadCount(null);
                return componentLifeCycleThreadPool.submit(task);
            }

            @Override
            public void onTaskComplete() {
                lifecycleState.decrementActiveThreadCount(null);
            }
        };

        LOG.info("Starting {}", procNode);
        procNode.start(componentMonitoringThreadPool, administrativeYieldMillis, processorStartTimeoutMillis, processContext, callback, failIfStopping);
        return future;
    }

    /**
     * Stops the given {@link Processor} by invoking its
     * {@link ProcessorNode#stop(ProcessScheduler, ScheduledExecutorService, ProcessContext, SchedulingAgent, LifecycleState)}
     * method.
     *
     * @see StandardProcessorNode#stop(ProcessScheduler, ScheduledExecutorService, ProcessContext, SchedulingAgent, LifecycleState)
     */
    @Override
    public synchronized CompletableFuture<Void> stopProcessor(final ProcessorNode procNode) {
        final LifecycleState lifecycleState = getLifecycleState(procNode, false);

        StandardProcessContext processContext = new StandardProcessContext(procNode, getControllerServiceProvider(),
            this.encryptor, getStateManager(procNode.getIdentifier()), lifecycleState::isTerminated);

        LOG.info("Stopping {}", procNode);
        return procNode.stop(this, this.componentLifeCycleThreadPool, processContext, getSchedulingAgent(procNode), lifecycleState);
    }

    @Override
    public synchronized void terminateProcessor(final ProcessorNode procNode) {
        if (procNode.getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Cannot terminate " + procNode + " because it is not currently stopped");
        }

        final LifecycleState state = getLifecycleState(procNode, false);
        if (state.getActiveThreadCount() == 0) {
            LOG.debug("Will not terminate {} because it has no active threads", procNode);
            return;
        }

        LOG.debug("Terminating {}", procNode);

        final int tasksTerminated = procNode.terminate();
        state.terminate();

        getSchedulingAgent(procNode).incrementMaxThreadCount(tasksTerminated);

        try {
            flowController.getReloadComponent().reload(procNode, procNode.getProcessor().getClass().getName(), procNode.getBundleCoordinate(), Collections.emptySet());
        } catch (final ProcessorInstantiationException e) {
            // This shouldn't happen because we already have been able to instantiate the processor before
            LOG.error("Failed to replace instance of Processor for {} when terminating Processor", procNode);
        }

        LOG.info("Successfully terminated {} with {} active threads", procNode, tasksTerminated);
    }

    @Override
    public void onProcessorRemoved(final ProcessorNode procNode) {
        lifecycleStates.remove(procNode);
    }

    @Override
    public void onPortRemoved(final Port port) {
        lifecycleStates.remove(port);
    }

    @Override
    public void onFunnelRemoved(Funnel funnel) {
        lifecycleStates.remove(funnel);
    }

    @Override
    public void onReportingTaskRemoved(final ReportingTaskNode reportingTask) {
        lifecycleStates.remove(reportingTask);
    }

    @Override
    public void yield(final ProcessorNode procNode) {
        // This exists in the ProcessScheduler so that the scheduler can take
        // advantage of the fact that
        // the Processor was yielded and, as a result, avoid scheduling the
        // Processor to potentially run
        // (thereby skipping the overhead of the Context Switches) if nothing
        // can be done.
        //
        // We used to implement this feature by canceling all futures for the
        // given Processor and
        // re-submitting them with a delay. However, this became problematic,
        // because we have situations where
        // a Processor will wait several seconds (often 30 seconds in the case
        // of a network timeout), and then yield
        // the context. If this Processor has X number of threads, we end up
        // submitting X new tasks while the previous
        // X-1 tasks are still running. At this point, another thread could
        // finish and do the same thing, resulting in
        // an additional X-1 extra tasks being submitted.
        //
        // As a result, we simply removed this buggy implementation, as it was a
        // very minor performance optimization
        // that gave very bad results.
    }

    @Override
    public void registerEvent(final Connectable worker) {
        getSchedulingAgent(worker).onEvent(worker);
    }

    @Override
    public int getActiveThreadCount(final Object scheduled) {
        return getLifecycleState(scheduled, false).getActiveThreadCount();
    }

    @Override
    public void startPort(final Port port) {
        if (!port.isValid()) {
            throw new IllegalStateException("Port " + port.getIdentifier() + " is not in a valid state");
        }

        port.onSchedulingStart();
        startConnectable(port);
    }

    @Override
    public void startFunnel(final Funnel funnel) {
        startConnectable(funnel);
        funnel.setScheduledState(ScheduledState.RUNNING);
    }

    @Override
    public void stopPort(final Port port) {
        stopConnectable(port);
        port.shutdown();
    }

    @Override
    public void stopFunnel(final Funnel funnel) {
        stopConnectable(funnel);
        funnel.setScheduledState(ScheduledState.STOPPED);
    }

    private synchronized void startConnectable(final Connectable connectable) {
        if (connectable.getScheduledState() == ScheduledState.DISABLED) {
            throw new IllegalStateException(connectable.getIdentifier() + " is disabled, so it cannot be started");
        }

        final LifecycleState lifecycleState = getLifecycleState(requireNonNull(connectable), true);
        if (lifecycleState.isScheduled()) {
            return;
        }

        final int activeThreads = lifecycleState.getActiveThreadCount();
        if (activeThreads > 0) {
            throw new IllegalStateException("Port cannot be scheduled to run until its last " + activeThreads + " threads finish");
        }

        lifecycleState.clearTerminationFlag();
        getSchedulingAgent(connectable).schedule(connectable, lifecycleState);
        lifecycleState.setScheduled(true);
    }

    private synchronized void stopConnectable(final Connectable connectable) {
        final LifecycleState state = getLifecycleState(requireNonNull(connectable), false);
        if (!state.isScheduled()) {
            return;
        }

        state.setScheduled(false);
        getSchedulingAgent(connectable).unschedule(connectable, state);

        if (!state.isScheduled() && state.getActiveThreadCount() == 0 && state.mustCallOnStoppedMethods()) {
            final ConnectableProcessContext processContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), connectable.getClass(), connectable.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, connectable, processContext);
            }
        }
    }

    @Override
    public synchronized void enableFunnel(final Funnel funnel) {
        if (funnel.getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException("Funnel cannot be enabled because it is not disabled");
        }
        funnel.setScheduledState(ScheduledState.STOPPED);
    }

    @Override
    public synchronized void disableFunnel(final Funnel funnel) {
        if (funnel.getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Funnel cannot be disabled because its state its state is set to " + funnel.getScheduledState());
        }
        funnel.setScheduledState(ScheduledState.DISABLED);
    }

    @Override
    public synchronized void disablePort(final Port port) {
        if (port.getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Port cannot be disabled because its state is set to " + port.getScheduledState());
        }

        if (!(port instanceof AbstractPort)) {
            throw new IllegalArgumentException();
        }

        ((AbstractPort) port).disable();
    }

    @Override
    public synchronized void enablePort(final Port port) {
        if (port.getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException("Funnel cannot be enabled because it is not disabled");
        }

        if (!(port instanceof AbstractPort)) {
            throw new IllegalArgumentException();
        }

        ((AbstractPort) port).enable();
    }

    @Override
    public synchronized void enableProcessor(final ProcessorNode procNode) {
        procNode.enable();
    }

    @Override
    public synchronized void disableProcessor(final ProcessorNode procNode) {
        procNode.disable();
    }

    public synchronized void enableReportingTask(final ReportingTaskNode taskNode) {
        if (taskNode.getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException("Reporting Task cannot be enabled because it is not disabled");
        }

        taskNode.setScheduledState(ScheduledState.STOPPED);
    }

    public synchronized void disableReportingTask(final ReportingTaskNode taskNode) {
        if (taskNode.getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Reporting Task cannot be disabled because its state is set to " + taskNode.getScheduledState()
                    + " but transition to DISABLED state is allowed only from the STOPPED state");
        }

        taskNode.setScheduledState(ScheduledState.DISABLED);
    }

    @Override
    public boolean isScheduled(final Object scheduled) {
        final LifecycleState lifecycleState = lifecycleStates.get(scheduled);
        return lifecycleState == null ? false : lifecycleState.isScheduled();
    }

    /**
     * Returns the LifecycleState that is registered for the given component; if
     * no LifecycleState current is registered, one is created and registered
     * atomically, and then that value is returned.
     *
     * @param schedulable schedulable
     * @return scheduled state
     */
    private LifecycleState getLifecycleState(final Object schedulable, final boolean replaceTerminatedState) {
        LifecycleState lifecycleState;
        while (true) {
            lifecycleState = this.lifecycleStates.get(schedulable);

            if (lifecycleState == null) {
                lifecycleState = new LifecycleState();
                final LifecycleState existing = this.lifecycleStates.putIfAbsent(schedulable, lifecycleState);

                if (existing == null) {
                    break;
                } else {
                    continue;
                }
            } else if (replaceTerminatedState && lifecycleState.isTerminated()) {
                final LifecycleState newLifecycleState = new LifecycleState();
                final boolean replaced = this.lifecycleStates.replace(schedulable, lifecycleState, newLifecycleState);

                if (replaced) {
                    lifecycleState = newLifecycleState;
                    break;
                } else {
                    continue;
                }
            } else {
                break;
            }
        }

        return lifecycleState;
    }

    @Override
    public CompletableFuture<Void> enableControllerService(final ControllerServiceNode service) {
        LOG.info("Enabling " + service);
        return service.enable(this.componentLifeCycleThreadPool, this.administrativeYieldMillis);
    }

    @Override
    public CompletableFuture<Void> disableControllerService(final ControllerServiceNode service) {
        LOG.info("Disabling {}", service);
        return service.disable(this.componentLifeCycleThreadPool);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final List<ControllerServiceNode> services) {
        if (services == null || services.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = null;
        if (!requireNonNull(services).isEmpty()) {
            for (ControllerServiceNode controllerServiceNode : services) {
                final CompletableFuture<Void> serviceFuture = this.disableControllerService(controllerServiceNode);

                if (future == null) {
                    future = serviceFuture;
                } else {
                    future = CompletableFuture.allOf(future, serviceFuture);
                }
            }
        }

        return future;
    }
}
