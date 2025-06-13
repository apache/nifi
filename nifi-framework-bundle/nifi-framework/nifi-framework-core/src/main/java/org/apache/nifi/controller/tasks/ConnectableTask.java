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
import org.apache.nifi.controller.repository.metrics.tracking.StandardStatsTracker;
import org.apache.nifi.controller.repository.metrics.tracking.StatsTracker;
import org.apache.nifi.controller.repository.metrics.tracking.TrackedStats;
import org.apache.nifi.controller.repository.scheduling.ConnectableProcessContext;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.StandardLoggingContext;
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
    private final LifecycleState lifecycleState;
    private final ProcessContext processContext;
    private final FlowController flowController;
    private final int numRelationships;
    private final StatsTracker statsTracker;

    public ConnectableTask(final SchedulingAgent schedulingAgent, final Connectable connectable,
                           final FlowController flowController, final RepositoryContextFactory contextFactory, final LifecycleState lifecycleState) {

        this.schedulingAgent = schedulingAgent;
        this.connectable = connectable;
        this.lifecycleState = lifecycleState;
        this.numRelationships = connectable.getRelationships().size();
        this.flowController = flowController;

        final StateManager stateManager = new TaskTerminationAwareStateManager(flowController.getStateManagerProvider().getStateManager(connectable.getIdentifier()), lifecycleState::isTerminated);
        if (connectable instanceof ProcessorNode) {
            processContext = new StandardProcessContext(
                    (ProcessorNode) connectable, flowController.getControllerServiceProvider(), stateManager, lifecycleState::isTerminated, flowController);
        } else {
            processContext = new ConnectableProcessContext(connectable, stateManager);
        }

        repositoryContext = contextFactory.newProcessContext(connectable, new AtomicLong(0L));

        statsTracker = new StandardStatsTracker(flowController.getGarbageCollectionLog()::getTotalGarbageCollectionMillis,
            flowController.getPerformanceTrackingPercentage());
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
        if (lifecycleState.isTerminated()) {
            logger.debug("Will not trigger {} because task is terminated", connectable);
            return InvocationResult.DO_NOT_YIELD;
        }

        // make sure processor is not yielded
        if (isYielded()) {
            logger.debug("Will not trigger {} because component is yielded", connectable);
            return InvocationResult.DO_NOT_YIELD;
        }

        // make sure that either we're not clustered or this processor runs on all nodes or that this is the primary node
        if (!isRunOnCluster(flowController)) {
            logger.debug("Will not trigger {} because this is not the primary node", connectable);
            return InvocationResult.yield("This node is not the primary node");
        }

        // Make sure processor has work to do.
        if (!isWorkToDo()) {
            logger.debug("Yielding {} because it has no work to do", connectable);
            return InvocationResult.yield("No work to do");
        }

        if (numRelationships > 0) {
            final int requiredNumberOfAvailableRelationships = connectable.isTriggerWhenAnyDestinationAvailable() ? 1 : numRelationships;
            if (!repositoryContext.isRelationshipAvailabilitySatisfied(requiredNumberOfAvailableRelationships)) {
                logger.debug("Yielding {} because Backpressure is Applied", connectable);
                return InvocationResult.yield("Backpressure Applied");
            }
        }

        logger.debug("Triggering {}", connectable);
        final TrackedStats stats = statsTracker.startTracking();

        final long batchNanos = connectable.getRunDuration(TimeUnit.NANOSECONDS);
        final ProcessSessionFactory sessionFactory;
        final StandardProcessSession rawSession;
        final boolean batch;
        if (connectable.isSessionBatchingSupported() && batchNanos > 0L) {
            rawSession = new StandardProcessSession(repositoryContext, lifecycleState::isTerminated, stats.getPerformanceTracker());
            sessionFactory = new BatchingSessionFactory(rawSession);
            batch = true;
        } else {
            rawSession = null;
            sessionFactory = new StandardProcessSessionFactory(repositoryContext, lifecycleState::isTerminated, stats.getPerformanceTracker());
            batch = false;
        }

        final ActiveProcessSessionFactory activeSessionFactory = new WeakHashMapProcessSessionFactory(sessionFactory);
        lifecycleState.incrementActiveThreadCount(activeSessionFactory);

        final long startNanos = System.nanoTime();
        final long finishIfBackpressureEngaged = startNanos + (batchNanos / 25L);
        final long finishNanos = startNanos + batchNanos;
        int invocationCount = 0;

        final String originalThreadName = Thread.currentThread().getName();
        try {
            try (final AutoCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), connectable.getRunnableComponent().getClass(), connectable.getIdentifier())) {
                boolean shouldRun = connectable.getScheduledState() == ScheduledState.RUNNING || connectable.getScheduledState() == ScheduledState.RUN_ONCE;
                while (shouldRun) {
                    invocationCount++;
                    connectable.onTrigger(processContext, activeSessionFactory);

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
            } catch (final TerminatedTaskException e) {
                final ComponentLog componentLog = getComponentLog();
                componentLog.info("Processing terminated", e);
            } catch (final ProcessException e) {
                final ComponentLog componentLog = getComponentLog();
                componentLog.error("Processing failed", e);
            } catch (final Throwable e) {
                final ComponentLog componentLog = getComponentLog();
                componentLog.error("Processing halted: yielding [{}]", schedulingAgent.getAdministrativeYieldDuration(), e);
                logger.warn("Processing halted: uncaught exception in Component [{}]", connectable.getRunnableComponent(), e);
                connectable.yield(schedulingAgent.getAdministrativeYieldDuration(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            }
        } finally {
            try {
                if (batch) {
                    final ComponentLog procLog = new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent(), new StandardLoggingContext(connectable));

                    try {
                        rawSession.commitAsync(null, t -> {
                            procLog.error("Failed to commit session {}; rolling back", rawSession, t);
                        });
                    } catch (final TerminatedTaskException tte) {
                        procLog.debug("Cannot commit Batch Process Session because the Task was forcefully terminated", tte);
                    }
                }

                try {
                    updateEventRepo(stats, invocationCount);
                } catch (final IOException e) {
                    logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate.", connectable.getRunnableComponent(), e);
                }
            } finally {
                lifecycleState.decrementActiveThreadCount();
                Thread.currentThread().setName(originalThreadName);
            }
        }

        return InvocationResult.DO_NOT_YIELD;
    }

    private void updateEventRepo(final TrackedStats stats, final int invocationCount) throws IOException {
        final StandardFlowFileEvent flowFileEvent = stats.end();
        flowFileEvent.setInvocations(invocationCount);
        repositoryContext.getFlowFileEventRepository().updateRepository(flowFileEvent, connectable.getIdentifier());
    }

    private ComponentLog getComponentLog() {
        return new SimpleProcessLogger(connectable.getIdentifier(), connectable.getRunnableComponent(), new StandardLoggingContext(connectable));
    }

}
