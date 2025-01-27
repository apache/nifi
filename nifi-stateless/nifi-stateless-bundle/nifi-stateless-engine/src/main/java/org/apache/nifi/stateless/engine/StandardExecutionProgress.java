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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.stateless.flow.CanceledTriggerResult;
import org.apache.nifi.stateless.flow.DataflowTriggerContext;
import org.apache.nifi.stateless.flow.ExceptionalTriggerResult;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.apache.nifi.stateless.session.StatelessProcessSession;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class StandardExecutionProgress implements ExecutionProgress {
    private final ProcessGroup rootGroup;
    private final List<FlowFileQueue> internalFlowFileQueues;
    private final ContentRepository contentRepository;
    private final BlockingQueue<TriggerResult> resultQueue;
    private final Set<String> failurePortNames;
    private final AsynchronousCommitTracker commitTracker;
    private final StatelessStateManagerProvider stateManagerProvider;
    private final DataflowTriggerContext triggerContext;
    private final FlowPurgeAction purgeAction;
    private final List<StatelessProcessSession> createdSessions = new ArrayList<>();

    private final BlockingQueue<CompletionAction> completionActionQueue;
    private volatile boolean canceled = false;
    private volatile boolean failed = false;
    private volatile CompletionAction completionAction = null;

    public StandardExecutionProgress(final ProcessGroup rootGroup, final List<FlowFileQueue> internalFlowFileQueues, final BlockingQueue<TriggerResult> resultQueue,
                                     final RepositoryContextFactory repositoryContextFactory, final Set<String> failurePortNames,
                                     final AsynchronousCommitTracker commitTracker, final StatelessStateManagerProvider stateManagerProvider, final DataflowTriggerContext triggerContext,
                                     final FlowPurgeAction purgeAction) {
        this.rootGroup = rootGroup;
        this.internalFlowFileQueues = internalFlowFileQueues;
        this.resultQueue = resultQueue;
        this.contentRepository = repositoryContextFactory.getContentRepository();
        this.failurePortNames = failurePortNames;
        this.commitTracker = commitTracker;
        this.stateManagerProvider = stateManagerProvider;
        this.triggerContext = triggerContext;
        this.purgeAction = purgeAction;

        completionActionQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean isFailurePort(final String portName) {
        return failurePortNames.contains(portName);
    }

    @Override
    public boolean isTerminalPort(final Connectable connectable) {
        if (connectable == null) {
            return false;
        }
        if (connectable.getConnectableType() != ConnectableType.OUTPUT_PORT) {
            return false;
        }
        return connectable.getProcessGroup() == rootGroup;
    }

    @Override
    public synchronized void registerCreatedSession(final StatelessProcessSession session) {
        createdSessions.add(session);
    }

    private synchronized void rollbackActiveSessions() {
        for (final StatelessProcessSession session : createdSessions) {
            try {
                session.rollback();
            } catch (final TerminatedTaskException ignored) {
                // Ignore, as the session may already be terminated, which will result in an Exception.
            }
        }

        createdSessions.clear();
    }


    @Override
    public boolean isCanceled() {
        if (canceled) {
            return true;
        }

        final boolean aborted = triggerContext.isAbort();
        if (aborted) {
            notifyExecutionCanceled();
            return true;
        }

        return false;
    }

    @Override
    public boolean isDataQueued() {
        for (final FlowFileQueue queue : internalFlowFileQueues) {
            if (!queue.isActiveQueueEmpty() || queue.isUnacknowledgedFlowFile()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public CompletionAction awaitCompletionAction() throws InterruptedException {
        if (isCanceled()) {
            return CompletionAction.CANCEL;
        }

        final CompletionAction existingAction = getExistingCompletionAction();
        if (existingAction != null) {
            return existingAction;
        }

        final TriggerResult triggerResult = createResult(null, null);
        resultQueue.offer(triggerResult);

        final CompletionAction completionAction = completionActionQueue.take();
        // Hold onto the result so that the other Process Sessions that call this method can retrieve the result.
        this.completionAction = completionAction;
        return completionAction;
    }

    public void enqueueTriggerResult(final Runnable onAcknowledge, final Consumer<Throwable> onFailure) {
        if (isCanceled()) {
            onFailure.accept(new RuntimeException("Dataflow canceled"));
            return;
        }

        final CompletionAction existingAction = getExistingCompletionAction();
        if (existingAction != null) {
            onAcknowledge.run();
            return;
        }

        final TriggerResult triggerResult = createResult(onAcknowledge, onFailure);
        resultQueue.offer(triggerResult);
    }

    private synchronized CompletionAction getExistingCompletionAction() {
        final CompletionAction existingAction = this.completionAction;
        if (existingAction != null) {
            return existingAction;
        }

        final CompletionAction enqueuedAction = this.completionActionQueue.poll();
        if (enqueuedAction != null) {
            this.completionAction = enqueuedAction;
            return enqueuedAction;
        }

        return null;
    }

    private TriggerResult createResult(final Runnable onAcknowledge, final Consumer<Throwable> onFailure) {
        final Map<String, List<FlowFile>> outputFlowFiles = drainOutputQueues();

        for (final String failurePortName : failurePortNames) {
            final List<FlowFile> flowFilesForPort = outputFlowFiles.get(failurePortName);
            if (flowFilesForPort != null && !flowFilesForPort.isEmpty()) {
                throw new FailurePortEncounteredException("FlowFile was transferred to Port " + failurePortName + ", which is marked as a Failure Port", failurePortName);
            }
        }

        final boolean canceled = isCanceled();

        return new TriggerResult() {
            private volatile Throwable abortCause = null;

            @Override
            public boolean isSuccessful() {
                return abortCause == null;
            }

            @Override
            public boolean isCanceled() {
                return canceled;
            }

            @Override
            public Optional<Throwable> getFailureCause() {
                return Optional.ofNullable(abortCause);
            }

            @Override
            public Map<String, List<FlowFile>> getOutputFlowFiles() {
                return outputFlowFiles;
            }

            @Override
            public List<FlowFile> getOutputFlowFiles(final String portName) {
                return outputFlowFiles.computeIfAbsent(portName, name -> Collections.emptyList());
            }

            @Override
            public InputStream readContent(final FlowFile flowFile) throws IOException {
                if (!(flowFile instanceof FlowFileRecord)) {
                    throw new IllegalArgumentException("FlowFile was not created by this flow");
                }

                final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;
                final ContentClaim contentClaim = flowFileRecord.getContentClaim();

                final InputStream in = contentRepository.read(contentClaim);
                final long offset = flowFileRecord.getContentClaimOffset();
                if (offset > 0) {
                    StreamUtils.skip(in, offset);
                }

                return new LimitedInputStream(in, flowFile.getSize());
            }

            @Override
            public byte[] readContentAsByteArray(final FlowFile flowFile) throws IOException {
                if (!(flowFile instanceof FlowFileRecord)) {
                    throw new IllegalArgumentException("FlowFile was not created by this flow");
                }

                if (flowFile.getSize() > Integer.MAX_VALUE) {
                    throw new IOException("Cannot return contents of " + flowFile + " as a byte array because the contents exceed the maximum length supported for byte arrays ("
                        + Integer.MAX_VALUE + " bytes)");
                }

                final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;

                final long size = flowFileRecord.getSize();
                final byte[] flowFileContents = new byte[(int) size];

                try (final InputStream in = readContent(flowFile)) {
                    StreamUtils.fillBuffer(in, flowFileContents);
                }

                return flowFileContents;
            }

            @Override
            public void acknowledge() {
                commitTracker.triggerCallbacks();
                stateManagerProvider.commitUpdates();
                completionActionQueue.offer(CompletionAction.COMPLETE);
                contentRepository.purge();

                if (onAcknowledge != null) {
                    onAcknowledge.run();
                }
            }

            @Override
            public void abort(final Throwable cause) {
                abortCause = new DataflowAbortedException("Dataflow was aborted", cause);
                notifyExecutionFailed(abortCause);

                if (onFailure != null) {
                    onFailure.accept(cause);
                }
            }
        };
    }

    @Override
    public void notifyExecutionCanceled() {
        if (canceled || failed) {
            return;
        }

        canceled = true;

        commitTracker.triggerFailureCallbacks(new RuntimeException("Dataflow Canceled"));
        stateManagerProvider.rollbackUpdates();
        completionActionQueue.offer(CompletionAction.CANCEL);
        rollbackActiveSessions();

        purgeAction.purge();
        resultQueue.offer(new CanceledTriggerResult());
    }

    @Override
    public void notifyExecutionFailed(final Throwable cause) {
        if (canceled || failed) {
            return;
        }

        failed = true;
        commitTracker.triggerFailureCallbacks(cause);
        stateManagerProvider.rollbackUpdates();
        completionActionQueue.offer(CompletionAction.CANCEL);

        rollbackActiveSessions();
        purgeAction.purge();
        resultQueue.offer(new ExceptionalTriggerResult(cause));
    }

    public Map<String, List<FlowFile>> drainOutputQueues() {
        final Map<String, List<FlowFile>> flowFileMap = new HashMap<>();

        for (final Port port : rootGroup.getOutputPorts()) {
            final List<FlowFile> flowFiles = drainOutputQueues(port);
            flowFileMap.put(port.getName(), flowFiles);
        }

        return flowFileMap;
    }

    private List<FlowFile> drainOutputQueues(final Port port) {
        final List<Connection> incomingConnections = port.getIncomingConnections();
        if (incomingConnections.isEmpty()) {
            return Collections.emptyList();
        }

        final List<FlowFile> portFlowFiles = new ArrayList<>();
        for (final Connection connection : incomingConnections) {
            final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
            if (!(flowFileQueue instanceof DrainableFlowFileQueue)) {
                continue;
            }

            final DrainableFlowFileQueue drainableQueue = (DrainableFlowFileQueue) connection.getFlowFileQueue();
            final List<FlowFileRecord> flowFileRecords = new ArrayList<>(drainableQueue.size().getObjectCount());
            drainableQueue.drainTo(flowFileRecords);
            portFlowFiles.addAll(flowFileRecords);

            for (final FlowFileRecord flowFileRecord : flowFileRecords) {
                contentRepository.decrementClaimantCount(flowFileRecord.getContentClaim());
            }
        }

        return portFlowFiles;
    }
}
