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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.Heartbeater;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SchedulingContext;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.StandardSchedulingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for scheduling Processors, Ports, and Funnels to run at regular intervals
 */
public final class StandardProcessScheduler implements ProcessScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessScheduler.class);

    private final ControllerServiceProvider controllerServiceProvider;
    private final Heartbeater heartbeater;
    private final long administrativeYieldMillis;
    private final String administrativeYieldDuration;

    private final ConcurrentMap<Object, ScheduleState> scheduleStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService frameworkTaskExecutor;
    private final ConcurrentMap<SchedulingStrategy, SchedulingAgent> strategyAgentMap = new ConcurrentHashMap<>();
    // thread pool for starting/stopping components
    private final ExecutorService componentLifeCycleThreadPool = new ThreadPoolExecutor(25, 50, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(5000));
    private final StringEncryptor encryptor;

    public StandardProcessScheduler(final Heartbeater heartbeater, final ControllerServiceProvider controllerServiceProvider, final StringEncryptor encryptor) {
        this.heartbeater = heartbeater;
        this.controllerServiceProvider = controllerServiceProvider;
        this.encryptor = encryptor;

        administrativeYieldDuration = NiFiProperties.getInstance().getAdministrativeYieldDuration();
        administrativeYieldMillis = FormatUtils.getTimeDuration(administrativeYieldDuration, TimeUnit.MILLISECONDS);

        frameworkTaskExecutor = new FlowEngine(4, "Framework Task Thread");
    }

    public void scheduleFrameworkTask(final Runnable command, final String taskName, final long initialDelay, final long delay, final TimeUnit timeUnit) {
        frameworkTaskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    command.run();
                } catch (final Throwable t) {
                    LOG.error("Failed to run Framework Task {} due to {}", command, t.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.error("", t);
                    }
                }
            }
        }, initialDelay, delay, timeUnit);
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
            @SuppressWarnings("deprecation")
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

                            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                                ReflectionUtils.invokeMethodsWithAnnotations(OnScheduled.class, OnConfigured.class, reportingTask, taskNode.getConfigurationContext());
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
                            new Object[] { reportingTask, e.toString(), administrativeYieldDuration }, e);

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
            @SuppressWarnings("deprecation")
            @Override
            public void run() {
                final ConfigurationContext configurationContext = taskNode.getConfigurationContext();

                synchronized (scheduleState) {
                    scheduleState.setScheduled(false);

                    try {
                        try (final NarCloseable x = NarCloseable.withNarLoader()) {
                            ReflectionUtils.invokeMethodsWithAnnotations(OnUnscheduled.class, org.apache.nifi.processor.annotation.OnUnscheduled.class, reportingTask, configurationContext);
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
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotations(OnStopped.class, org.apache.nifi.processor.annotation.OnStopped.class, reportingTask, configurationContext);
                    }
                }
            }
        };

        componentLifeCycleThreadPool.execute(unscheduleReportingTaskRunnable);
    }

    /**
     * Starts scheduling the given processor to run after invoking all methods on the underlying {@link nifi.processor.Processor
     * FlowFileProcessor} that are annotated with the {@link OnScheduled} annotation.
     */
    @Override
    public synchronized void startProcessor(final ProcessorNode procNode) {
        if (procNode.getScheduledState() == ScheduledState.DISABLED) {
            throw new IllegalStateException(procNode + " is disabled, so it cannot be started");
        }
        final ScheduleState scheduleState = getScheduleState(requireNonNull(procNode));

        if (scheduleState.isScheduled()) {
            return;
        }

        final int activeThreadCount = scheduleState.getActiveThreadCount();
        if (activeThreadCount > 0) {
            throw new IllegalStateException("Processor " + procNode.getName() + " cannot be started because it has " + activeThreadCount + " threads still running");
        }

        if (!procNode.isValid()) {
            throw new IllegalStateException("Processor " + procNode.getName() + " is not in a valid state due to " + procNode.getValidationErrors());
        }

        final Runnable startProcRunnable = new Runnable() {
            @Override
            @SuppressWarnings("deprecation")
            public void run() {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    final long lastStopTime = scheduleState.getLastStopTime();
                    final StandardProcessContext processContext = new StandardProcessContext(procNode, controllerServiceProvider, encryptor);

                    final Set<String> serviceIds = new HashSet<>();
                    for (final PropertyDescriptor descriptor : processContext.getProperties().keySet()) {
                        final Class<? extends ControllerService> serviceDefinition = descriptor.getControllerServiceDefinition();
                        if (serviceDefinition != null) {
                            final String serviceId = processContext.getProperty(descriptor).getValue();
                            if (serviceId != null) {
                                serviceIds.add(serviceId);
                            }
                        }
                    }

                    attemptOnScheduled:
                    while (true) {
                        try {
                            synchronized (scheduleState) {
                                for (final String serviceId : serviceIds) {
                                    final boolean enabled = processContext.isControllerServiceEnabled(serviceId);
                                    if (!enabled) {
                                        LOG.debug("Controller Service with ID {} is not yet enabled, so will not start {} yet", serviceId, procNode);
                                        Thread.sleep(administrativeYieldMillis);
                                        continue attemptOnScheduled;
                                    }
                                }

                                // if no longer scheduled to run, then we're finished. This can happen, for example,
                                // if the @OnScheduled method throws an Exception and the user stops the processor
                                // while we're administratively yielded.
                                // we also check if the schedule state's last start time is equal to what it was before.
                                // if not, then means that the processor has been stopped and started again, so we should just
                                // bail; another thread will be responsible for invoking the @OnScheduled methods.
                                if (!scheduleState.isScheduled() || scheduleState.getLastStopTime() != lastStopTime) {
                                    return;
                                }

                                final SchedulingContext schedulingContext = new StandardSchedulingContext(processContext, controllerServiceProvider, procNode);
                                ReflectionUtils.invokeMethodsWithAnnotations(OnScheduled.class, org.apache.nifi.processor.annotation.OnScheduled.class, procNode.getProcessor(), schedulingContext);

                                getSchedulingAgent(procNode).schedule(procNode, scheduleState);

                                heartbeater.heartbeat();
                                return;
                            }
                        } catch (final Exception e) {
                            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                            final ProcessorLog procLog = new SimpleProcessLogger(procNode.getIdentifier(), procNode.getProcessor());

                            procLog.error("{} failed to invoke @OnScheduled method due to {}; processor will not be scheduled to run for {}",
                                    new Object[]{procNode.getProcessor(), cause, administrativeYieldDuration}, cause);
                            LOG.error("Failed to invoke @OnScheduled method due to {}", cause.toString(), cause);

                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, procNode.getProcessor(), processContext);
                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, procNode.getProcessor(), processContext);

                            Thread.sleep(administrativeYieldMillis);
                            continue;
                        }
                    }
                } catch (final Throwable t) {
                    final ProcessorLog procLog = new SimpleProcessLogger(procNode.getIdentifier(), procNode.getProcessor());
                    procLog.error("{} failed to invoke @OnScheduled method due to {}; processor will not be scheduled to run", new Object[]{procNode.getProcessor(), t});
                    LOG.error("Failed to invoke @OnScheduled method due to {}", t.toString(), t);
                }
            }
        };

        scheduleState.setScheduled(true);
        procNode.setScheduledState(ScheduledState.RUNNING);

        componentLifeCycleThreadPool.execute(startProcRunnable);
    }

    @Override
    public void yield(final ProcessorNode procNode) {
        // This exists in the ProcessScheduler so that the scheduler can take advantage of the fact that
        // the Processor was yielded and, as a result, avoid scheduling the Processor to potentially run
        // (thereby skipping the overhead of the Context Switches) if nothing can be done.
        //
        // We used to implement this feature by canceling all futures for the given Processor and
        // re-submitting them with a delay. However, this became problematic, because we have situations where
        // a Processor will wait several seconds (often 30 seconds in the case of a network timeout), and then yield
        // the context. If this Processor has X number of threads, we end up submitting X new tasks while the previous
        // X-1 tasks are still running. At this point, another thread could finish and do the same thing, resulting in
        // an additional X-1 extra tasks being submitted.
        //
        // As a result, we simply removed this buggy implementation, as it was a very minor performance optimization
        // that gave very bad results.
    }

    /**
     * Stops scheduling the given processor to run and invokes all methods on the underlying {@link nifi.processor.Processor FlowFileProcessor} that are annotated with the {@link OnUnscheduled}
     * annotation.
     */
    @Override
    public synchronized void stopProcessor(final ProcessorNode procNode) {
        final ScheduleState state = getScheduleState(requireNonNull(procNode));

        synchronized (state) {
            if (!state.isScheduled()) {
                procNode.setScheduledState(ScheduledState.STOPPED);
                return;
            }

            state.setScheduled(false);
            getSchedulingAgent(procNode).unschedule(procNode, state);
            procNode.setScheduledState(ScheduledState.STOPPED);
        }

        final Runnable stopProcRunnable = new Runnable() {
            @Override
            public void run() {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    final StandardProcessContext processContext = new StandardProcessContext(procNode, controllerServiceProvider, encryptor);

                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, procNode.getProcessor(), processContext);

                    // If no threads currently running, call the OnStopped methods
                    if (state.getActiveThreadCount() == 0 && state.mustCallOnStoppedMethods()) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, procNode.getProcessor(), processContext);
                        heartbeater.heartbeat();
                    }
                }
            }
        };

        componentLifeCycleThreadPool.execute(stopProcRunnable);
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
            throw new IllegalStateException("Port " + port.getName() + " is not in a valid state");
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
            throw new IllegalStateException(connectable + " is disabled, so it cannot be started");
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
            final ConnectableProcessContext processContext = new ConnectableProcessContext(connectable, encryptor);
            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, connectable, processContext);
                heartbeater.heartbeat();
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
        if (procNode.getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException("Processor cannot be enabled because it is not disabled");
        }

        procNode.setScheduledState(ScheduledState.STOPPED);
    }

    @Override
    public synchronized void disableProcessor(final ProcessorNode procNode) {
        if (procNode.getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Processor cannot be disabled because its state is set to " + procNode.getScheduledState());
        }

        procNode.setScheduledState(ScheduledState.DISABLED);
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
     * Returns the ScheduleState that is registered for the given component; if no ScheduleState current is registered, one is created and registered atomically, and then that value is returned.
     *
     * @param schedulable schedulable
     * @return scheduled state
     */
    private ScheduleState getScheduleState(final Object schedulable) {
        ScheduleState scheduleState = scheduleStates.get(schedulable);
        if (scheduleState == null) {
            scheduleState = new ScheduleState();
            final ScheduleState previous = scheduleStates.putIfAbsent(schedulable, scheduleState);
            if (previous != null) {
                scheduleState = previous;
            }
        }
        return scheduleState;
    }

    @Override
    public void enableControllerService(final ControllerServiceNode service) {
        service.setState(ControllerServiceState.ENABLING);
        final ScheduleState scheduleState = getScheduleState(service);

        final Runnable enableRunnable = new Runnable() {
            @Override
            public void run() {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    final long lastStopTime = scheduleState.getLastStopTime();
                    final ConfigurationContext configContext = new StandardConfigurationContext(service, controllerServiceProvider, null);

                    while (true) {
                        try {
                            synchronized (scheduleState) {
                                // if no longer enabled, then we're finished. This can happen, for example,
                                // if the @OnEnabled method throws an Exception and the user disables the service
                                // while we're administratively yielded.
                                //
                                // we also check if the schedule state's last stop time is equal to what it was before.
                                // if not, then means that the service has been disabled and enabled again, so we should just
                                // bail; another thread will be responsible for invoking the @OnEnabled methods.
                                if (!scheduleState.isScheduled() || scheduleState.getLastStopTime() != lastStopTime) {
                                    return;
                                }

                                ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service.getControllerServiceImplementation(), configContext);
                                heartbeater.heartbeat();
                                service.setState(ControllerServiceState.ENABLED);
                                return;
                            }
                        } catch (final Exception e) {
                            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;

                            final ComponentLog componentLog = new SimpleProcessLogger(service.getIdentifier(), service);
                            componentLog.error("Failed to invoke @OnEnabled method due to {}", cause);
                            LOG.error("Failed to invoke @OnEnabled method of {} due to {}", service.getControllerServiceImplementation(), cause.toString());
                            if (LOG.isDebugEnabled()) {
                                LOG.error("", cause);
                            }

                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnDisabled.class, service.getControllerServiceImplementation(), configContext);
                            Thread.sleep(administrativeYieldMillis);
                            continue;
                        }
                    }
                } catch (final Throwable t) {
                    final Throwable cause = t instanceof InvocationTargetException ? t.getCause() : t;
                    final ComponentLog componentLog = new SimpleProcessLogger(service.getIdentifier(), service);
                    componentLog.error("Failed to invoke @OnEnabled method due to {}", cause);

                    LOG.error("Failed to invoke @OnEnabled method on {} due to {}", service.getControllerServiceImplementation(), cause.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.error("", cause);
                    }
                }
            }
        };

        scheduleState.setScheduled(true);
        componentLifeCycleThreadPool.execute(enableRunnable);
    }

    @Override
    public void disableControllerService(final ControllerServiceNode service) {
        service.verifyCanDisable();

        final ScheduleState state = getScheduleState(requireNonNull(service));
        final Runnable disableRunnable = new Runnable() {
            @Override
            public void run() {
                synchronized (state) {
                    state.setScheduled(false);
                }

                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    final ConfigurationContext configContext = new StandardConfigurationContext(service, controllerServiceProvider, null);

                    try {
                        ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, service.getControllerServiceImplementation(), configContext);
                    } catch (final Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(service.getIdentifier(), service);
                        componentLog.error("Failed to invoke @OnDisabled method due to {}", cause);

                        LOG.error("Failed to invoke @OnDisabled method of {} due to {}", service.getControllerServiceImplementation(), cause.toString());
                        if (LOG.isDebugEnabled()) {
                            LOG.error("", cause);
                        }

                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnDisabled.class, service.getControllerServiceImplementation(), configContext);
                        try {
                            Thread.sleep(administrativeYieldMillis);
                        } catch (final InterruptedException ie) {
                        }
                    } finally {
                        service.setState(ControllerServiceState.DISABLED);
                        heartbeater.heartbeat();
                    }
                }
            }
        };

        service.setState(ControllerServiceState.DISABLING);
        componentLifeCycleThreadPool.execute(disableRunnable);
    }
}
