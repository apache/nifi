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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.BatchingSessionFactory;
import org.apache.nifi.controller.repository.ProcessContext;
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continually runs a processor as long as the processor has work to do. {@link #call()} will return <code>true</code> if the processor should be yielded, <code>false</code> otherwise.
 */
public class ContinuallyRunProcessorTask implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(ContinuallyRunProcessorTask.class);

    private final SchedulingAgent schedulingAgent;
    private final ProcessorNode procNode;
    private final ProcessContext context;
    private final ScheduleState scheduleState;
    private final StandardProcessContext processContext;
    private final FlowController flowController;
    private final int numRelationships;

    public ContinuallyRunProcessorTask(final SchedulingAgent schedulingAgent, final ProcessorNode procNode,
            final FlowController flowController, final ProcessContextFactory contextFactory, final ScheduleState scheduleState,
            final StandardProcessContext processContext) {

        this.schedulingAgent = schedulingAgent;
        this.procNode = procNode;
        this.scheduleState = scheduleState;
        this.numRelationships = procNode.getRelationships().size();
        this.flowController = flowController;

        context = contextFactory.newProcessContext(procNode, new AtomicLong(0L));
        this.processContext = processContext;
    }

    static boolean isRunOnCluster(final ProcessorNode procNode, FlowController flowController) {
        return !procNode.isIsolated() || !flowController.isConfiguredForClustering() || flowController.isPrimary();
    }

    static boolean isYielded(final ProcessorNode procNode) {
        // after one yield period, the scheduling agent could call this again when
        // yieldExpiration == currentTime, and we don't want that to still be considered 'yielded'
        // so this uses ">" instead of ">="
        return procNode.getYieldExpiration() > System.currentTimeMillis();
    }

    static boolean isWorkToDo(final ProcessorNode procNode) {
        return procNode.isTriggerWhenEmpty() || !procNode.hasIncomingConnection() || !Connectables.hasNonLoopConnection(procNode) || Connectables.flowFilesQueued(procNode);
    }

    private boolean isBackPressureEngaged() {
        return procNode.getIncomingConnections().stream()
            .filter(con -> con.getSource() == procNode)
            .map(con -> con.getFlowFileQueue())
            .anyMatch(queue -> queue.isFull());
    }

    @Override
    public Boolean call() {
        // make sure processor is not yielded
        if (isYielded(procNode)) {
            return false;
        }

        // make sure that either we're not clustered or this processor runs on all nodes or that this is the primary node
        if (!isRunOnCluster(procNode, flowController)) {
            return false;
        }

        // Make sure processor has work to do. This means that it meets one of these criteria:
        // * It is annotated with @TriggerWhenEmpty
        // * It has data in an incoming Connection
        // * It has no incoming connections
        // * All incoming connections are self-loops
        if (!isWorkToDo(procNode)) {
            return true;
        }

        if (numRelationships > 0) {
            final int requiredNumberOfAvailableRelationships = procNode.isTriggerWhenAnyDestinationAvailable() ? 1 : numRelationships;
            if (!context.isRelationshipAvailabilitySatisfied(requiredNumberOfAvailableRelationships)) {
                return true;
            }
        }

        final long batchNanos = procNode.getRunDuration(TimeUnit.NANOSECONDS);
        final ProcessSessionFactory sessionFactory;
        final StandardProcessSession rawSession;
        final boolean batch;
        if (procNode.isHighThroughputSupported() && batchNanos > 0L) {
            rawSession = new StandardProcessSession(context);
            sessionFactory = new BatchingSessionFactory(rawSession);
            batch = true;
        } else {
            rawSession = null;
            sessionFactory = new StandardProcessSessionFactory(context);
            batch = false;
        }

        scheduleState.incrementActiveThreadCount();

        final long startNanos = System.nanoTime();
        final long finishIfBackpressureEngaged = startNanos + (batchNanos / 25L);
        final long finishNanos = startNanos + batchNanos;
        int invocationCount = 0;
        try {
            try (final AutoCloseable ncl = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())) {
                boolean shouldRun = true;
                while (shouldRun) {
                    procNode.onTrigger(processContext, sessionFactory);
                    invocationCount++;

                    if (!batch) {
                        return false;
                    }

                    final long nanoTime = System.nanoTime();
                    if (nanoTime > finishNanos) {
                        return false;
                    }

                    if (nanoTime > finishIfBackpressureEngaged && isBackPressureEngaged()) {
                        return false;
                    }


                    if (!isWorkToDo(procNode)) {
                        break;
                    }
                    if (isYielded(procNode)) {
                        break;
                    }

                    if (numRelationships > 0) {
                        final int requiredNumberOfAvailableRelationships = procNode.isTriggerWhenAnyDestinationAvailable() ? 1 : numRelationships;
                        shouldRun = context.isRelationshipAvailabilitySatisfied(requiredNumberOfAvailableRelationships);
                    }
                }
            } catch (final ProcessException pe) {
                final ComponentLog procLog = new SimpleProcessLogger(procNode.getIdentifier(), procNode.getProcessor());
                procLog.error("Failed to process session due to {}", new Object[]{pe});
            } catch (final Throwable t) {
                // Use ComponentLog to log the event so that a bulletin will be created for this processor
                final ComponentLog procLog = new SimpleProcessLogger(procNode.getIdentifier(), procNode.getProcessor());
                procLog.error("{} failed to process session due to {}", new Object[]{procNode.getProcessor(), t});
                procLog.warn("Processor Administratively Yielded for {} due to processing failure", new Object[]{schedulingAgent.getAdministrativeYieldDuration()});
                logger.warn("Administratively Yielding {} due to uncaught Exception: {}", procNode.getProcessor(), t.toString());
                logger.warn("", t);

                procNode.yield(schedulingAgent.getAdministrativeYieldDuration(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            }
        } finally {
            try {
                if (batch) {
                    try {
                        rawSession.commit();
                    } catch (final Exception e) {
                        final ComponentLog procLog = new SimpleProcessLogger(procNode.getIdentifier(), procNode.getProcessor());
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
                    final StandardFlowFileEvent procEvent = new StandardFlowFileEvent(procNode.getIdentifier());
                    procEvent.setProcessingNanos(processingNanos);
                    procEvent.setInvocations(invocationCount);
                    context.getFlowFileEventRepository().updateRepository(procEvent);
                } catch (final IOException e) {
                    logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", procNode.getProcessor(), e.toString());
                    logger.error("", e);
                }
            } finally {
                scheduleState.decrementActiveThreadCount();
            }
        }

        return false;
    }

}
