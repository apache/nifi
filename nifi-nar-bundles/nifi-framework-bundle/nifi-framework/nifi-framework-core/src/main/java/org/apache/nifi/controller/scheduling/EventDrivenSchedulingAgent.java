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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.EventBasedWorker;
import org.apache.nifi.controller.EventDrivenWorkerQueue;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.repository.BatchingSessionFactory;
import org.apache.nifi.controller.repository.ProcessContext;
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Connectables;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventDrivenSchedulingAgent extends AbstractSchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(EventDrivenSchedulingAgent.class);

    private final ControllerServiceProvider serviceProvider;
    private final StateManagerProvider stateManagerProvider;
    private final EventDrivenWorkerQueue workerQueue;
    private final ProcessContextFactory contextFactory;
    private final AtomicInteger maxThreadCount;
    private final AtomicInteger activeThreadCount = new AtomicInteger(0);
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";

    private final ConcurrentMap<Connectable, AtomicLong> connectionIndexMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Connectable, ScheduleState> scheduleStates = new ConcurrentHashMap<>();

    public EventDrivenSchedulingAgent(final FlowEngine flowEngine, final ControllerServiceProvider serviceProvider, final StateManagerProvider stateManagerProvider,
        final EventDrivenWorkerQueue workerQueue, final ProcessContextFactory contextFactory, final int maxThreadCount, final StringEncryptor encryptor) {
        super(flowEngine);
        this.serviceProvider = serviceProvider;
        this.stateManagerProvider = stateManagerProvider;
        this.workerQueue = workerQueue;
        this.contextFactory = contextFactory;
        this.maxThreadCount = new AtomicInteger(maxThreadCount);
        this.encryptor = encryptor;

        for (int i = 0; i < maxThreadCount; i++) {
            final Runnable eventDrivenTask = new EventDrivenTask(workerQueue, activeThreadCount);
            flowEngine.scheduleWithFixedDelay(eventDrivenTask, 0L, 1L, TimeUnit.NANOSECONDS);
        }
    }

    public int getActiveThreadCount() {
        return activeThreadCount.get();
    }

    private StateManager getStateManager(final String componentId) {
        return stateManagerProvider.getStateManager(componentId);
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, ScheduleState scheduleState) {
        throw new UnsupportedOperationException("ReportingTasks cannot be scheduled in Event-Driven Mode");
    }

    @Override
    public void doUnschedule(ReportingTaskNode taskNode, ScheduleState scheduleState) {
        throw new UnsupportedOperationException("ReportingTasks cannot be scheduled in Event-Driven Mode");
    }

    @Override
    public void doSchedule(final Connectable connectable, final ScheduleState scheduleState) {
        workerQueue.resumeWork(connectable);
        logger.info("Scheduled {} to run in Event-Driven mode", connectable);
        scheduleStates.put(connectable, scheduleState);
    }

    @Override
    public void doUnschedule(final Connectable connectable, final ScheduleState scheduleState) {
        workerQueue.suspendWork(connectable);
        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void onEvent(final Connectable connectable) {
        workerQueue.offer(connectable);
    }

    @Override
    public void setMaxThreadCount(final int maxThreadCount) {
        final int oldMax = this.maxThreadCount.getAndSet(maxThreadCount);
        if (maxThreadCount > oldMax) {
            // if more threads have been allocated, add more tasks to the work queue
            final int tasksToAdd = maxThreadCount - oldMax;
            for (int i = 0; i < tasksToAdd; i++) {
                final Runnable eventDrivenTask = new EventDrivenTask(workerQueue, activeThreadCount);
                flowEngine.scheduleWithFixedDelay(eventDrivenTask, 0L, 1L, TimeUnit.NANOSECONDS);
            }
        }
    }

    @Override
    public void setAdministrativeYieldDuration(final String yieldDuration) {
        this.adminYieldDuration = yieldDuration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(adminYieldDuration, timeUnit);
    }

    private class EventDrivenTask implements Runnable {

        private final EventDrivenWorkerQueue workerQueue;
        private final AtomicInteger activeThreadCount;

        public EventDrivenTask(final EventDrivenWorkerQueue workerQueue, final AtomicInteger activeThreadCount) {
            this.workerQueue = workerQueue;
            this.activeThreadCount = activeThreadCount;
        }

        @Override
        public void run() {
            while (!flowEngine.isShutdown()) {
                final EventBasedWorker worker = workerQueue.poll(1, TimeUnit.SECONDS);
                if (worker == null) {
                    continue;
                }
                final Connectable connectable = worker.getConnectable();
                final ScheduleState scheduleState = scheduleStates.get(connectable);
                if (scheduleState == null) {
                    // Component not yet scheduled to run but has received events
                    continue;
                }

                activeThreadCount.incrementAndGet();
                try {
                    // get the connection index for this worker
                    AtomicLong connectionIndex = connectionIndexMap.get(connectable);
                    if (connectionIndex == null) {
                        connectionIndex = new AtomicLong(0L);
                        final AtomicLong existingConnectionIndex = connectionIndexMap.putIfAbsent(connectable, connectionIndex);
                        if (existingConnectionIndex != null) {
                            connectionIndex = existingConnectionIndex;
                        }
                    }

                    final ProcessContext context = contextFactory.newProcessContext(connectable, connectionIndex);

                    if (connectable instanceof ProcessorNode) {
                        final ProcessorNode procNode = (ProcessorNode) connectable;
                        final StandardProcessContext standardProcessContext = new StandardProcessContext(procNode, serviceProvider, encryptor, getStateManager(connectable.getIdentifier()));

                        final long runNanos = procNode.getRunDuration(TimeUnit.NANOSECONDS);
                        final ProcessSessionFactory sessionFactory;
                        final StandardProcessSession rawSession;
                        final boolean batch;
                        if (procNode.isHighThroughputSupported() && runNanos > 0L) {
                            rawSession = new StandardProcessSession(context);
                            sessionFactory = new BatchingSessionFactory(rawSession);
                            batch = true;
                        } else {
                            rawSession = null;
                            sessionFactory = new StandardProcessSessionFactory(context);
                            batch = false;
                        }

                        final long startNanos = System.nanoTime();
                        final long finishNanos = startNanos + runNanos;
                        int invocationCount = 0;
                        boolean shouldRun = true;

                        try {
                            while (shouldRun) {
                                trigger(procNode, context, scheduleState, standardProcessContext, sessionFactory);
                                invocationCount++;

                                if (!batch) {
                                    break;
                                }
                                if (System.nanoTime() > finishNanos) {
                                    break;
                                }
                                if (!scheduleState.isScheduled()) {
                                    break;
                                }

                                final int eventCount = worker.decrementEventCount();
                                if (eventCount < 0) {
                                    worker.incrementEventCount();
                                }
                                shouldRun = (eventCount > 0);
                            }
                        } finally {
                            if (batch && rawSession != null) {
                                try {
                                    rawSession.commit();
                                } catch (final RuntimeException re) {
                                    logger.error("Unable to commit process session", re);
                                }
                            }
                            try {
                                final long processingNanos = System.nanoTime() - startNanos;
                                final StandardFlowFileEvent procEvent = new StandardFlowFileEvent(connectable.getIdentifier());
                                procEvent.setProcessingNanos(processingNanos);
                                procEvent.setInvocations(invocationCount);
                                context.getFlowFileEventRepository().updateRepository(procEvent);
                            } catch (final IOException e) {
                                logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", connectable, e.toString());
                                logger.error("", e);
                            }
                        }

                        // If the Processor has FlowFiles, go ahead and register it to run again.
                        // We do this because it's possible (and fairly common) for a Processor to be triggered and then determine,
                        // for whatever reason, that it is not ready to do anything and as a result simply returns without pulling anything
                        // off of its input queue.
                        // In this case, we could just say that the Processor shouldn't be Event-Driven, but then it becomes very complex and
                        // confusing to determine whether or not a Processor is really Event-Driven. So, the solution that we will use at this
                        // point is to register the Processor to run again.
                        if (Connectables.flowFilesQueued(procNode)) {
                            onEvent(procNode);
                        }
                    } else {
                        final ProcessSessionFactory sessionFactory = new StandardProcessSessionFactory(context);
                        final ConnectableProcessContext connectableProcessContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
                        trigger(connectable, scheduleState, connectableProcessContext, sessionFactory);

                        // See explanation above for the ProcessorNode as to why we do this.
                        if (Connectables.flowFilesQueued(connectable)) {
                            onEvent(connectable);
                        }
                    }
                } finally {
                    activeThreadCount.decrementAndGet();
                }
            }
        }

        private void trigger(final Connectable worker, final ScheduleState scheduleState, final ConnectableProcessContext processContext, final ProcessSessionFactory sessionFactory) {
            final int newThreadCount = scheduleState.incrementActiveThreadCount();
            if (newThreadCount > worker.getMaxConcurrentTasks() && worker.getMaxConcurrentTasks() > 0) {
                // its possible that the worker queue could give us a worker node that is eligible to run based
                // on the number of threads but another thread has already incremented the thread count, result in
                // reaching the maximum number of threads. we won't know this until we atomically increment the thread count
                // on the Schedule State, so we check it here. in this case, we cannot trigger the Processor, as doing so would
                // result in using more than the maximum number of defined threads
                scheduleState.decrementActiveThreadCount();
                return;
            }

            try {
                try (final AutoCloseable ncl = NarCloseable.withComponentNarLoader(worker.getClass(), worker.getIdentifier())) {
                    worker.onTrigger(processContext, sessionFactory);
                } catch (final ProcessException pe) {
                    logger.error("{} failed to process session due to {}", worker, pe.toString());
                } catch (final Throwable t) {
                    logger.error("{} failed to process session due to {}", worker, t.toString());
                    logger.error("", t);

                    logger.warn("{} Administratively Pausing for {} due to processing failure: {}", worker, getAdministrativeYieldDuration(), t.toString());
                    logger.warn("", t);
                    try {
                        Thread.sleep(FormatUtils.getTimeDuration(adminYieldDuration, TimeUnit.MILLISECONDS));
                    } catch (final InterruptedException e) {
                    }

                }
            } finally {
                if (!scheduleState.isScheduled() && scheduleState.getActiveThreadCount() == 1 && scheduleState.mustCallOnStoppedMethods()) {
                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(worker.getClass(), worker.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, worker, processContext);
                    }
                }

                scheduleState.decrementActiveThreadCount();
            }
        }

        private void trigger(final ProcessorNode worker, final ProcessContext context, final ScheduleState scheduleState,
                final StandardProcessContext processContext, final ProcessSessionFactory sessionFactory) {
            final int newThreadCount = scheduleState.incrementActiveThreadCount();
            if (newThreadCount > worker.getMaxConcurrentTasks() && worker.getMaxConcurrentTasks() > 0) {
                // its possible that the worker queue could give us a worker node that is eligible to run based
                // on the number of threads but another thread has already incremented the thread count, result in
                // reaching the maximum number of threads. we won't know this until we atomically increment the thread count
                // on the Schedule State, so we check it here. in this case, we cannot trigger the Processor, as doing so would
                // result in using more than the maximum number of defined threads
                scheduleState.decrementActiveThreadCount();
                return;
            }

            try {
                try (final AutoCloseable ncl = NarCloseable.withComponentNarLoader(worker.getProcessor().getClass(), worker.getIdentifier())) {
                    worker.onTrigger(processContext, sessionFactory);
                } catch (final ProcessException pe) {
                    final ComponentLog procLog = new SimpleProcessLogger(worker.getIdentifier(), worker.getProcessor());
                    procLog.error("Failed to process session due to {}", new Object[]{pe});
                } catch (final Throwable t) {
                    // Use ComponentLog to log the event so that a bulletin will be created for this processor
                    final ComponentLog procLog = new SimpleProcessLogger(worker.getIdentifier(), worker.getProcessor());
                    procLog.error("{} failed to process session due to {}", new Object[]{worker.getProcessor(), t});
                    procLog.warn("Processor Administratively Yielded for {} due to processing failure", new Object[]{adminYieldDuration});
                    logger.warn("Administratively Yielding {} due to uncaught Exception: ", worker.getProcessor());
                    logger.warn("", t);

                    worker.yield(FormatUtils.getTimeDuration(adminYieldDuration, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
                }
            } finally {
                // if the processor is no longer scheduled to run and this is the last thread,
                // invoke the OnStopped methods
                if (!scheduleState.isScheduled() && scheduleState.getActiveThreadCount() == 1 && scheduleState.mustCallOnStoppedMethods()) {
                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(worker.getProcessor().getClass(), worker.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, worker.getProcessor(), processContext);
                    }
                }

                scheduleState.decrementActiveThreadCount();
            }
        }
    }
}
