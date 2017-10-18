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

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.SchedulingAgentCallback;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
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

/**
 * Responsible for scheduling Processors, Ports, and Funnels to run at regular
 * intervals
 */
public final class StandardProcessScheduler implements ProcessScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessScheduler.class);

    private final ControllerServiceProvider controllerServiceProvider;
    private final long administrativeYieldMillis;
    private final String administrativeYieldDuration;
    private final StateManagerProvider stateManagerProvider;

    private final ConcurrentMap<Object, ScheduleState> scheduleStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService frameworkTaskExecutor;
    private final ConcurrentMap<SchedulingStrategy, SchedulingAgent> strategyAgentMap = new ConcurrentHashMap<>();
    // thread pool for starting/stopping components

    private final ScheduledExecutorService componentLifeCycleThreadPool = new FlowEngine(8, "StandardProcessScheduler", true);
    private final ScheduledExecutorService componentMonitoringThreadPool = new FlowEngine(8, "StandardProcessScheduler", true);

    private final StringEncryptor encryptor;

    public StandardProcessScheduler(
            final ControllerServiceProvider controllerServiceProvider,
            final StringEncryptor encryptor,
            final StateManagerProvider stateManagerProvider,
            final NiFiProperties nifiProperties
    ) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.encryptor = encryptor;
        this.stateManagerProvider = stateManagerProvider;

        administrativeYieldDuration = nifiProperties.getAdministrativeYieldDuration();
        administrativeYieldMillis = FormatUtils.getTimeDuration(administrativeYieldDuration, TimeUnit.MILLISECONDS);

        frameworkTaskExecutor = new FlowEngine(4, "Framework Task Thread");
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
        componentMonitoringThreadPool.shutdown();
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode) {
        final ScheduleState scheduleState = getScheduleState(requireNonNull(taskNode));
        if (scheduleState.isScheduled()) {
            return;
        }

        final int activeThreadCount = scheduleState.getActiveThreadCount();
        if (activeThreadCount > 0) {
            throw new IllegalStateException("Reporting Task " + taskNode.getName() + " cannot be started because it has " + activeThreadCount + " threads still running");
        }

        if (!taskNode.isValid()) {
            throw new IllegalStateException("Reporting Task " + taskNode.getName() + " is not in a valid state for the following reasons: " + taskNode.getValidationErrors());
        }

        final SchedulingAgent agent = getSchedulingAgent(taskNode.getSchedulingStrategy());
        scheduleState.setScheduled(true);

        final Runnable startReportingTaskRunnable = new Runnable() {
            @Override
            public void run() {
                final long lastStopTime = scheduleState.getLastStopTime();
                final ReportingTask reportingTask = taskNode.getReportingTask();

                // Continually attempt to start the Reporting Task, and if we fail sleep for a bit each time.
                while (true) {
                    try {
                        synchronized (scheduleState) {
                            // if no longer scheduled to run, then we're finished. This can happen, for example,
                            // if the @OnScheduled method throws an Exception and the user stops the reporting task
                            // while we're administratively yielded.
                            // we also check if the schedule state's last start time is equal to what it was before.
                            // if not, then means that the reporting task has been stopped and started again, so we should just
                            // bail; another thread will be responsible for invoking the @OnScheduled methods.
                            if (!scheduleState.isScheduled() || scheduleState.getLastStopTime() != lastStopTime) {
                                return;
                            }

                            try (final NarCloseable x = NarCloseable.withComponentNarLoader(reportingTask.getClass(), reportingTask.getIdentifier())) {
                                ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, reportingTask, taskNode.getConfigurationContext());
                            }

                            agent.schedule(taskNode, scheduleState);
                            return;
                        }
                    } catch (final Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(reportingTask.getIdentifier(), reportingTask);
                        componentLog.error("Failed to invoke @OnEnabled method due to {}", cause);

                        LOG.error("Failed to invoke the On-Scheduled Lifecycle methods of {} due to {}; administratively yielding this "
                                + "ReportingTask and will attempt to schedule it again after {}",
                                new Object[]{reportingTask, e.toString(), administrativeYieldDuration}, e);

                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, reportingTask, taskNode.getConfigurationContext());
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, reportingTask, taskNode.getConfigurationContext());

                        try {
                            Thread.sleep(administrativeYieldMillis);
                        } catch (final InterruptedException ie) {
                        }
                    }
                }
            }
        };

        componentLifeCycleThreadPool.execute(startReportingTaskRunnable);
        taskNode.setScheduledState(ScheduledState.RUNNING);
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode) {
        final ScheduleState scheduleState = getScheduleState(requireNonNull(taskNode));
        if (!scheduleState.isScheduled()) {
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

                synchronized (scheduleState) {
                    scheduleState.setScheduled(false);

                    try {
                        try (final NarCloseable x = NarCloseable.withComponentNarLoader(reportingTask.getClass(), reportingTask.getIdentifier())) {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, reportingTask, configurationContext);
                        }
                    } catch (final Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(reportingTask.getIdentifier(), reportingTask);
                        componentLog.error("Failed to invoke @OnUnscheduled method due to {}", cause);

                        LOG.error("Failed to invoke the @OnUnscheduled methods of {} due to {}; administratively yielding this ReportingTask and will attempt to schedule it again after {}",
                                reportingTask, cause.toString(), administrativeYieldDuration);
                        LOG.error("", cause);

                        try {
                            Thread.sleep(administrativeYieldMillis);
                        } catch (final InterruptedException ie) {
                        }
                    }

                    agent.unschedule(taskNode, scheduleState);

                    if (scheduleState.getActiveThreadCount() == 0 && scheduleState.mustCallOnStoppedMethods()) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, reportingTask, configurationContext);
                    }
                }
            }
        };

        componentLifeCycleThreadPool.execute(unscheduleReportingTaskRunnable);
    }

    /**
     * Starts the given {@link Processor} by invoking its
     * {@link ProcessorNode#start(ScheduledExecutorService, long, org.apache.nifi.processor.ProcessContext, Runnable)}
     * method.
     *
     * @see StandardProcessorNode#start(ScheduledExecutorService, long, org.apache.nifi.processor.ProcessContext, Runnable).
     */
    @Override
    public synchronized CompletableFuture<Void> startProcessor(final ProcessorNode procNode) {
        StandardProcessContext processContext = new StandardProcessContext(procNode, this.controllerServiceProvider,
            this.encryptor, getStateManager(procNode.getIdentifier()));
        final ScheduleState scheduleState = getScheduleState(requireNonNull(procNode));

        final CompletableFuture<Void> future = new CompletableFuture<>();
        SchedulingAgentCallback callback = new SchedulingAgentCallback() {
            @Override
            public void trigger() {
                getSchedulingAgent(procNode).schedule(procNode, scheduleState);
                future.complete(null);
            }

            @Override
            public Future<?> invokeMonitoringTask(Callable<?> task) {
                scheduleState.incrementActiveThreadCount();
                return componentMonitoringThreadPool.submit(task);
            }

            @Override
            public void postMonitor() {
                scheduleState.decrementActiveThreadCount();
            }
        };

        LOG.info("Starting {}", procNode);
        procNode.start(this.componentLifeCycleThreadPool, this.administrativeYieldMillis, processContext, callback);
        return future;
    }

    /**
     * Stops the given {@link Processor} by invoking its
     * {@link ProcessorNode#stop(ScheduledExecutorService, org.apache.nifi.processor.ProcessContext, SchedulingAgent, ScheduleState)}
     * method.
     *
     * @see StandardProcessorNode#stop(ScheduledExecutorService, org.apache.nifi.processor.ProcessContext, SchedulingAgent, ScheduleState)
     */
    @Override
    public synchronized CompletableFuture<Void> stopProcessor(final ProcessorNode procNode) {
        StandardProcessContext processContext = new StandardProcessContext(procNode, this.controllerServiceProvider,
            this.encryptor, getStateManager(procNode.getIdentifier()));
        final ScheduleState state = getScheduleState(procNode);

        LOG.info("Stopping {}", procNode);
        return procNode.stop(this.componentLifeCycleThreadPool, processContext, getSchedulingAgent(procNode), state);
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
        return getScheduleState(scheduled).getActiveThreadCount();
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

        final ScheduleState scheduleState = getScheduleState(requireNonNull(connectable));
        if (scheduleState.isScheduled()) {
            return;
        }

        final int activeThreads = scheduleState.getActiveThreadCount();
        if (activeThreads > 0) {
            throw new IllegalStateException("Port cannot be scheduled to run until its last " + activeThreads + " threads finish");
        }

        getSchedulingAgent(connectable).schedule(connectable, scheduleState);
        scheduleState.setScheduled(true);
    }

    private synchronized void stopConnectable(final Connectable connectable) {
        final ScheduleState state = getScheduleState(requireNonNull(connectable));
        if (!state.isScheduled()) {
            return;
        }

        state.setScheduled(false);
        getSchedulingAgent(connectable).unschedule(connectable, state);

        if (!state.isScheduled() && state.getActiveThreadCount() == 0 && state.mustCallOnStoppedMethods()) {
            final ConnectableProcessContext processContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(connectable.getClass(), connectable.getIdentifier())) {
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
        final ScheduleState scheduleState = scheduleStates.get(scheduled);
        return scheduleState == null ? false : scheduleState.isScheduled();
    }

    /**
     * Returns the ScheduleState that is registered for the given component; if
     * no ScheduleState current is registered, one is created and registered
     * atomically, and then that value is returned.
     *
     * @param schedulable schedulable
     * @return scheduled state
     */
    private ScheduleState getScheduleState(final Object schedulable) {
        ScheduleState scheduleState = this.scheduleStates.get(schedulable);
        if (scheduleState == null) {
            scheduleState = new ScheduleState();
            this.scheduleStates.putIfAbsent(schedulable, scheduleState);
        }
        return scheduleState;
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
