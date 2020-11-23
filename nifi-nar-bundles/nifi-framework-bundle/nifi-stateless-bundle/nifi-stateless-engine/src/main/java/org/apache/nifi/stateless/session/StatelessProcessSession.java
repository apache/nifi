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

package org.apache.nifi.stateless.session;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.stateless.engine.DataflowAbortedException;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StatelessProcessSession extends StandardProcessSession {
    private static final Logger logger = LoggerFactory.getLogger(StatelessProcessSession.class);
    private static final String PARENT_FLOW_GROUP_ID = "stateless-flow";

    private final RepositoryContext context;
    private final StatelessProcessSessionFactory sessionFactory;
    private final ProcessContextFactory processContextFactory;
    private final ExecutionProgress executionProgress;

    public StatelessProcessSession(final RepositoryContext context, final StatelessProcessSessionFactory sessionFactory, final ProcessContextFactory processContextFactory,
                                   final ExecutionProgress progress) {
        super(context, progress::isCanceled);
        this.context = context;
        this.sessionFactory = sessionFactory;
        this.processContextFactory = processContextFactory;
        this.executionProgress = progress;
    }

    @Override
    protected void commit(final StandardProcessSession.Checkpoint checkpoint) {
        // If task has been canceled, abort processing and throw an Exception, rather than committing the session.
        if (executionProgress.isCanceled()) {
            logger.info("Completed processing for {} but execution has been canceled. Will not commit session.", context.getConnectable());
            abortProcessing(null);
            throw new DataflowAbortedException();
        }

        // Commit the session
        super.commit(checkpoint);

        // Trigger each of the follow-on components.
        final long followOnStart = System.nanoTime();
        for (final Connection connection : context.getConnectable().getConnections()) {
            // This component may have produced multiple output FlowFiles. We want to trigger the follow-on components
            // until they have consumed all created FlowFiles.
            while (!connection.getFlowFileQueue().isEmpty()) {
                final Connectable connectable = connection.getDestination();
                if (isTerminalPort(connectable)) {
                    // If data is being transferred to a terminal port, we don't want to trigger the port,
                    // as it has nowhere to transfer the data. We simply leave it queued at the terminal port.
                    // Once the processing completes, the terminal ports' connections will be drained, when #awaitAcknowledgment is called.
                    break;
                }

                // Trigger the next component
                triggerNext(connectable);
            }
        }

        // When this component finishes running, the flowfile event repo will be updated to include the number of nanoseconds it took to
        // trigger this component. But that will include the amount of time that it took to trigger follow-on components as well.
        // Because we want to include only the time it took for this component, subtract away the amount of time that it took for
        // follow-on components.
        // Note that for a period of time, this could result in showing a negative amount of time for the current component to complete,
        // since the subtraction will be performed before the addition of the time the current component was run. But this is an approximation,
        // and it's probably the best that we can do without either introducing a very ugly hack or significantly changing the API.
        final long followOnNanos = System.nanoTime() - followOnStart;
        registerProcessEvent(context.getConnectable(), -followOnNanos);

        // Wait for acknowledgement if necessary
        awaitAcknowledgment();
    }

    private void triggerNext(final Connectable connectable) {
        if (executionProgress.isCanceled()) {
            logger.info("Completed processing for {} but execution has been canceled. Will not commit session.", context.getConnectable());
            abortProcessing(null);
            throw new DataflowAbortedException();
        }

        final ProcessContext connectableContext = processContextFactory.createProcessContext(connectable);
        final ProcessSessionFactory connectableSessionFactory = new StatelessProcessSessionFactory(connectable, this.sessionFactory.getRepositoryContextFactory(),
            processContextFactory, executionProgress);

        logger.debug("Triggering {}", connectable);
        final long start = System.nanoTime();
        try {
            connectable.onTrigger(connectableContext, connectableSessionFactory);
        } catch (final Throwable t) {
            abortProcessing(t);
            throw t;
        }

        final long nanos = System.nanoTime() - start;
        registerProcessEvent(connectable, nanos);
    }

    private void awaitAcknowledgment() {
        if (executionProgress.isDataQueued()) {
            logger.debug("Completed processing for {} but data is queued for processing so will allow Process Session to complete without waiting for acknowledgment", context.getConnectable());
            return;
        }

        logger.debug("Completed processing for {}; no data is queued for processing so will await acknowledgment of completion", context.getConnectable());
        final ExecutionProgress.CompletionAction completionAction;
        try {
            completionAction = executionProgress.awaitCompletionAction();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for dataflow completion to be acknowledged. Will roll back session.");
            abortProcessing(e);
            throw new DataflowAbortedException();
        }

        if (completionAction == ExecutionProgress.CompletionAction.CANCEL) {
            logger.info("Dataflow completed but action was canceled instead of being acknowledged. Will roll back session.");
            abortProcessing(null);
            throw new DataflowAbortedException();
        }
    }

    private void abortProcessing(final Throwable cause) {
        if (cause == null) {
            executionProgress.notifyExecutionCanceled();
        } else {
            executionProgress.notifyExecutionFailed(cause);
        }

        try {
            rollback(false, true);
        } finally {
            purgeFlowFiles();
        }
    }

    private void purgeFlowFiles() {
        final ProcessGroup rootGroup = getRootGroup();
        final List<Connection> allConnections = rootGroup.findAllConnections();
        for (final Connection connection : allConnections) {
            final DrainableFlowFileQueue flowFileQueue = (DrainableFlowFileQueue) connection.getFlowFileQueue();
            final List<FlowFileRecord> flowFileRecords = new ArrayList<>(flowFileQueue.size().getObjectCount());
            flowFileQueue.drainTo(flowFileRecords);

            for (final FlowFileRecord flowFileRecord : flowFileRecords) {
                context.getContentRepository().decrementClaimantCount(flowFileRecord.getContentClaim());
            }
        }
    }

    private ProcessGroup getRootGroup() {
        final Connectable connectable = context.getConnectable();
        final ProcessGroup group = connectable.getProcessGroup();
        return getRootGroup(group);
    }

    private ProcessGroup getRootGroup(final ProcessGroup group) {
        final ProcessGroup parent = group.getParent();
        if (parent == null) {
            return group;
        }

        return getRootGroup(parent);
    }

    private void registerProcessEvent(final Connectable connectable, final long processingNanos) {
        try {
            final StandardFlowFileEvent procEvent = new StandardFlowFileEvent();
            procEvent.setProcessingNanos(processingNanos);
            procEvent.setInvocations(1);
            context.getFlowFileEventRepository().updateRepository(procEvent, connectable.getIdentifier());
        } catch (final IOException e) {
            logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", connectable.getRunnableComponent(), e.toString(), e);
        }
    }

    private boolean isTerminalPort(final Connectable connectable) {
        final ConnectableType connectableType = connectable.getConnectableType();
        if (connectableType != ConnectableType.OUTPUT_PORT) {
            return false;
        }

        final ProcessGroup portGroup = connectable.getProcessGroup();
        if (PARENT_FLOW_GROUP_ID.equals(portGroup.getIdentifier())) {
            logger.debug("FlowFiles queued for {} but this is a Terminal Port. Will not trigger Port to run.", connectable);
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "StatelessProcessSession[id=" + getSessionId() + "]";
    }
}
