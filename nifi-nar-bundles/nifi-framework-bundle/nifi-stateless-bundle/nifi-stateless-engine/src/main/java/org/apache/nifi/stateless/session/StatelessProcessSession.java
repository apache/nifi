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
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.stateless.engine.DataflowAbortedException;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class StatelessProcessSession extends StandardProcessSession {
    private static final Logger logger = LoggerFactory.getLogger(StatelessProcessSession.class);
    private static final String PARENT_FLOW_GROUP_ID = "stateless-flow";

    private final Connectable connectable;
    private final RepositoryContextFactory repositoryContextFactory;
    private final ProcessContextFactory processContextFactory;
    private final ExecutionProgress executionProgress;
    private final AsynchronousCommitTracker tracker;

    private boolean requireSynchronousCommits;

    public StatelessProcessSession(final Connectable connectable, final RepositoryContextFactory repositoryContextFactory, final ProcessContextFactory processContextFactory,
                                   final ExecutionProgress progress, final boolean requireSynchronousCommits, final AsynchronousCommitTracker tracker) {

        super(repositoryContextFactory.createRepositoryContext(connectable), progress::isCanceled);
        this.connectable = connectable;
        this.repositoryContextFactory = repositoryContextFactory;
        this.processContextFactory = processContextFactory;
        this.executionProgress = progress;
        this.requireSynchronousCommits = requireSynchronousCommits;
        this.tracker = tracker;
    }

    @Override
    public void commitAsync() {
        // If we require a synchronous commit, we can just call super.commitAsync(), which will then call the super class's commit(),
        // which will delegate to this.commit(Checkpoint), which is the synchronous commit.
        if (!requireSynchronousCommits) {
            super.commitAsync();
            return;
        }

        super.commit();
    }

    @Override
    public void commitAsync(final Runnable onSuccess) {
        // Overridden to ensure that we properly check this.requireSynchronousCommits
        commitAsync(onSuccess, null);
    }

    @Override
    public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
        // If we don't require synchronous commits, we can trigger the async commit, but we can't call the callback yet, because we only can call the success callback when we've completed the
        // dataflow in order to ensure that we don't destroy data in a way that it can't be replayed if the downstream processors fail.
        if (!requireSynchronousCommits) {
            super.commitAsync();
            tracker.addCallback(connectable, onSuccess, onFailure);
            return;
        }

        // If we require a synchronous commit, we can just call super.commitAsync(), which will then call the super class's commit(),
        // which will delegate to this.commit(Checkpoint), which is the synchronous commit and will result in trigger downstream processors,
        // so we can then trigger the success callback.
        try {
            super.commit();
        } catch (final Throwable t) {
            logger.error("Failed to commit Process Session {} for {}", this, connectable, t);
            onFailure.accept(t);
            return;
        }

        try {
            onSuccess.run();
        } catch (final Exception e) {
            logger.error("Committed Process Session {} for {} but failed to trigger success callback", this, connectable, e);
        }
    }

    @Override
    protected void commit(final StandardProcessSession.Checkpoint checkpoint, final boolean asynchronous) {
        // If task has been canceled, abort processing and throw an Exception, rather than committing the session.
        assertProgressNotCanceled();

        // Once a synchronous commit has been made, we must require that all future commits be synchronous.
        // If we did not do this, we could have a case where a Processor reads data from a File, for example, and then calls ProcessSession.commit() and then deletes the file.
        // If the destination were to then call ProcessSession.commitAsync() and that resulted in an asynchronous commit, we would see that the data is transferred to the next queue
        // but not finished processing. Then, the GetFile processor's call to ProcessSession.commit() would return and the file would be deleted, but this would happen before the
        // data made its way to the end destination. If Stateless were then stopped, it would result in data loss.
        requireSynchronousCommits = requireSynchronousCommits || !asynchronous;

        // Check if the Processor made any progress or not. If so, record this fact so that the framework knows that this was the case.
        final int flowFileCounts = checkpoint.getFlowFilesIn() + checkpoint.getFlowFilesOut() + checkpoint.getFlowFilesRemoved();
        if (flowFileCounts > 0) {
            tracker.recordProgress(checkpoint.getFlowFilesOut() + checkpoint.getFlowFilesRemoved(), checkpoint.getBytesOut() + checkpoint.getBytesRemoved());
        }

        // Commit the session
        super.commit(checkpoint, asynchronous);

        if (!requireSynchronousCommits) {
            queueFollowOnComponents();
            return;
        }

        // Trigger each of the follow-on components.
        final long followOnStart = System.nanoTime();
        triggerFollowOnComponents();

        // When this component finishes running, the flowfile event repo will be updated to include the number of nanoseconds it took to
        // trigger this component. But that will include the amount of time that it took to trigger follow-on components as well.
        // Because we want to include only the time it took for this component, subtract away the amount of time that it took for
        // follow-on components.
        // Note that for a period of time, this could result in showing a negative amount of time for the current component to complete,
        // since the subtraction will be performed before the addition of the time the current component was run. But this is an approximation,
        // and it's probably the best that we can do without either introducing a very ugly hack or significantly changing the API.
        final long followOnNanos = System.nanoTime() - followOnStart;
        registerProcessEvent(connectable, -followOnNanos);

        // Wait for acknowledgement if necessary. This allows Stateless NiFi to be easily embedded within another
        // application in order to source data from elsewhere. The application is then able to accept the data, process it,
        // and then acknowledge it. This provides a mechanism by which the data can be sourced from a replayable source,
        // such as Kafka or JMS, and then acknowledge receipt of the data only after all processing of the data has been
        // completed both by Stateless NiFi and the application that is triggering Stateless.
        awaitAcknowledgment();
    }

    private void triggerFollowOnComponents() {
        for (final Connection connection : connectable.getConnections()) {
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
    }

    private void queueFollowOnComponents() {
        for (final Connection connection : connectable.getConnections()) {
            // This component may have produced multiple output FlowFiles. We want to trigger the follow-on components
            // until they have consumed all created FlowFiles.
            if (connection.getFlowFileQueue().isEmpty()) {
                continue;
            }

            final Connectable connectable = connection.getDestination();
            if (isTerminalPort(connectable)) {
                // If data is being transferred to a terminal port, we don't want to trigger the port,
                // as it has nowhere to transfer the data. We simply leave it queued at the terminal port.
                // Once the processing completes, the terminal ports' connections will be drained, when #awaitAcknowledgment is called.
                continue;
            }

            tracker.addConnectable(connectable);
        }
    }

    private void triggerNext(final Connectable connectable) {
        assertProgressNotCanceled();

        final ProcessContext connectableContext = processContextFactory.createProcessContext(connectable);
        final ProcessSessionFactory connectableSessionFactory = new StatelessProcessSessionFactory(connectable, repositoryContextFactory,
            processContextFactory, executionProgress, requireSynchronousCommits, new AsynchronousCommitTracker());

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

    private void assertProgressNotCanceled() {
        if (executionProgress.isCanceled()) {
            logger.info("Completed processing for {} but execution has been canceled. Will not commit session.", connectable);
            abortProcessing(null);
            throw new DataflowAbortedException();
        }
    }

    private void awaitAcknowledgment() {
        if (executionProgress.isDataQueued()) {
            logger.debug("Completed processing for {} but data is queued for processing so will allow Process Session to complete without waiting for acknowledgment", connectable);
            return;
        }

        logger.debug("Completed processing for {}; no data is queued for processing so will await acknowledgment of completion", connectable);
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
                getRepositoryContext().getContentRepository().decrementClaimantCount(flowFileRecord.getContentClaim());
            }
        }
    }

    private ProcessGroup getRootGroup() {
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
            getRepositoryContext().getFlowFileEventRepository().updateRepository(procEvent, connectable.getIdentifier());
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
