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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.lifecycle.TaskTerminationAwareStateManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ActiveProcessSessionFactory;
import org.apache.nifi.controller.repository.BatchingSessionFactory;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.repository.WeakHashMapProcessSessionFactory;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.scheduling.ConnectableProcessContext;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Continually runs a <code>{@link Connectable}</code> component as long as the component has work to do.
 * {@link #invoke()} ()} will return <code>{@link InvocationResult}</code> telling if the component should be yielded.
 */
public class ConnectableTask {

    private static final Logger logger = LoggerFactory.getLogger(ConnectableTask.class);

    private final SchedulingAgent schedulingAgent;
    private final Connectable connectable;
    private final RepositoryContext repositoryContext;
    private final LifecycleState scheduleState;
    private final ProcessContext processContext;
    private final FlowController flowController;
    private final int numRelationships;


    public ConnectableTask(final SchedulingAgent schedulingAgent, final Connectable connectable,
            final FlowController flowController, final RepositoryContextFactory contextFactory, final LifecycleState scheduleState,
            final StringEncryptor encryptor) {

        this.schedulingAgent = schedulingAgent;
        this.connectable = connectable;
        this.scheduleState = scheduleState;
        this.numRelationships = connectable.getRelationships().size();
        this.flowController = flowController;

        final StateManager stateManager = new TaskTerminationAwareStateManager(flowController.getStateManagerProvider().getStateManager(connectable.getIdentifier()), scheduleState::isTerminated);
        if (connectable instanceof ProcessorNode) {
            processContext = new StandardProcessContext((ProcessorNode) connectable, flowController.getControllerServiceProvider(), encryptor, stateManager, scheduleState::isTerminated);
        } else {
            processContext = new ConnectableProcessContext(connectable, encryptor, stateManager);
        }

        repositoryContext = contextFactory.newProcessContext(connectable, new AtomicLong(0L));
    }

    public Connectable getConnectable() {
        return connectable;
    }

    private boolean isRunOnCluster(final FlowController flowController) {
        return !connectable.isIsolated() || !flowController.isConfiguredForClustering() || flowController.isPrimary();
    }

    private boolean isYielded() {
        // after one yield period, the scheduling agent could call this again when
        // yieldExpiration == currentTime, and we don't want that to still be considered 'yielded'
        // so this uses ">" instead of ">="
        return connectable.getYieldExpiration() > System.currentTimeMillis();
    }

    /**
     * Make sure processor has work to do. This means that it meets one of these criteria:
     * <ol>
     * <li>It is a Funnel and has incoming FlowFiles from other components, and and at least one outgoing connection.</li>
     * <li>It is a 'source' component, meaning:<ul>
     *   <li>It is annotated with @TriggerWhenEmpty</li>
     *   <li>It has no incoming connections</li>
     *   <li>All incoming connections are self-loops</li>
     * </ul></li>
     * <li>It has data in incoming connections to process</li>
     * </ol>
     * @return true if there is work to do, otherwise false
     */
    private boolean isWorkToDo() {
        boolean hasNonLoopConnection = Connectables.hasNonLoopConnection(connectable);

        if (connectable.getConnectableType() == ConnectableType.FUNNEL) {
            // Handle Funnel as a special case because it will never be a 'source' component,
            // and also its outgoing connections can not be terminated.
            // Incoming FlowFiles from other components, and at least one outgoing connection are required.
            return connectable.hasIncomingConnection()
                    && hasNonLoopConnection
                    && !connectable.getConnections().isEmpty()
                    && Connectables.flowFilesQueued(connectable);
        }

        final boolean isSourceComponent = connectable.isTriggerWhenEmpty()
                // No input connections
                || !connectable.hasIncomingConnection()
                // Every incoming connection loops back to itself, no inputs from other components
                || !hasNonLoopConnection;

        // If it is not a 'source' component, it requires a FlowFile to process.
        return isSourceComponent || Connectables.flowFilesQueued(connectable);
    }

    private boolean isBackPressureEngaged() {
        return connectable.getIncomingConnections().stream()
            .filter(con -> con.getSource() == connectable)
            .map(Connection::getFlowFileQueue)
            .anyMatch(FlowFileQueue::isFull);
    }

    public InvocationResult invoke() {
        if (scheduleState.isTerminated()) {
            return InvocationResult.DO_NOT_YIELD;
        }

        // make sure processor is not yielded
        if (isYielded()) {
            return InvocationResult.DO_NOT_YIELD;
        }

        // make sure that either we're not clustered or this processor runs on all nodes or that this is the primary node
        if (!isRunOnCluster(flowController)) {
            return InvocationResult.DO_NOT_YIELD;
        }

        // Make sure processor has work to do.
        if (!isWorkToDo()) {
            return InvocationResult.yield("No work to do");
        }

        if (numRelationships > 0) {
            final int requiredNumberOfAvailableRelationships = connectable.isTriggerWhenAnyDestinationAvailable() ? 1 : numRelationships;
            if (!repositoryContext.isRelationshipAvailabilitySatisfied(requiredNumberOfAvailableRelationships)) {
                return InvocationResult.yield("Backpressure Applied");
            }
        }

        final long batchNanos = connectable.getRunDuration(TimeUnit.NANOSECONDS);
        final ProcessSessionFactory sessionFactory;
        final StandardProcessSession rawSession;
        final boolean batch;
        if (connectable.isSessionBatchingSupported() && batchNanos > 0L) {
            rawSession = new StandardProcessSession(repositoryContext, scheduleState::isTerminated);
            sessionFactory = new BatchingSessionFactory(rawSession);
            batch = true;
        } else {
            rawSession = null;
            sessionFactory = new StandardProcessSessionFactory(repositoryContext, scheduleState::isTerminated);
            batch = false;
        }

        final ActiveProcessSessionFactory activeSessionFactory = new WeakHashMapProcessSessionFactory(sessionFactory);
        scheduleState.incrementActiveThreadCount(activeSessionFactory);

        final long startNanos = System.nanoTime();
        final long finishIfBackpressureEngaged = startNanos + (batchNanos / 25L);
        final long finishNanos = startNanos + batchNanos;
        int invocationCount = 0;

        final String originalThreadName = Thread.currentThread().getName();
        try {
            try (final AutoCloseable ncl = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), connectable.getRunnableComponent().getClass(), connectable.getIdentifier())) {
                boolean shouldRun = connectable.getScheduledState() == ScheduledState.RUNNING;
                while (shouldRun) {
                    connectable.onTrigger(processContext, activeSessionFactory);
                    invocationCount++;

                    if (!batch) {
                        return InvocationResult.DO_NOT_YIELD;
                    }

                    final long nanoTime = System.nanoTime();
                    if (nanoTime > finishNanos) {
                        return InvocationResult.DO_NOT_YIELD;
                    }

                    if (nanoTime > finishIfBackpressureEngaged && isBackPressureEngaged()) {
                        return InvocationResult.DO_NOT_YIELD;
                    }

                    if (connectable.getScheduledState() != ScheduledState.RUNNING) {
                        break;
                    }

                    if (!isWorkToDo()) {
                        break;
                    }
                    if (isYielded()) {
                        break;
                    }

                    if (numRelationships > 0) {
                        final int requiredNumberOfAvailableRelationships = connectable.isTriggerWhenAnyDestinationAvailable() ? 1 : numRelationships;
                        shouldRun = repositoryContext.isRelationshipAvailabilitySatisfied(requiredNumberOfAvailableRelationships);
                    }
                }
            } catch (final TerminatedTaskException tte) {
                final ComponentLog procLog = new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent());
                procLog.info("Failed to process session due to task being terminated", new Object[] {tte});
            } catch (final ProcessException pe) {
                final ComponentLog procLog = new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent());
                procLog.error("Failed to process session due to {}", new Object[] {pe});
            } catch (final Throwable t) {
                // Use ComponentLog to log the event so that a bulletin will be created for this processor
                final ComponentLog procLog = new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent());
                procLog.error("{} failed to process session due to {}; Processor Administratively Yielded for {}",
                    new Object[] {connectable.getRunnableComponent(), t, schedulingAgent.getAdministrativeYieldDuration()}, t);
                logger.warn("Administratively Yielding {} due to uncaught Exception: {}", connectable.getRunnableComponent(), t.toString(), t);

                connectable.yield(schedulingAgent.getAdministrativeYieldDuration(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            }
        } finally {
            try {
                if (batch) {
                    try {
                        rawSession.commit();
                    } catch (final Exception e) {
                        final ComponentLog procLog = new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent());
                        procLog.error("Failed to commit session {} due to {}; rolling back", new Object[] { rawSession, e.toString() }, e);

                        try {
                            rawSession.rollback(true);
                        } catch (final Exception e1) {
                            procLog.error("Failed to roll back session {} due to {}", new Object[] { rawSession, e.toString() }, e);
                        }
                    }
                }

                final long processingNanos = System.nanoTime() - startNanos;

                try {
                    final StandardFlowFileEvent procEvent = new StandardFlowFileEvent();
                    procEvent.setProcessingNanos(processingNanos);
                    procEvent.setInvocations(invocationCount);
                    repositoryContext.getFlowFileEventRepository().updateRepository(procEvent, connectable.getIdentifier());
                } catch (final IOException e) {
                    logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", connectable.getRunnableComponent(), e.toString());
                    logger.error("", e);
                }
            } finally {
                scheduleState.decrementActiveThreadCount(activeSessionFactory);
                Thread.currentThread().setName(originalThreadName);
            }
        }

        return InvocationResult.DO_NOT_YIELD;
    }

}
