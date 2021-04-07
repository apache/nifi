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
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class StandardExecutionProgress implements ExecutionProgress {
    private final ProcessGroup rootGroup;
    private final List<FlowFileQueue> internalFlowFileQueues;
    private final ByteArrayContentRepository contentRepository;
    private final BlockingQueue<TriggerResult> resultQueue;
    private final Set<String> failurePortNames;
    private final AsynchronousCommitTracker commitTracker;
    private final StatelessStateManagerProvider stateManagerProvider;

    private final BlockingQueue<CompletionAction> completionActionQueue;
    private volatile boolean canceled = false;
    private volatile CompletionAction completionAction = null;

    public StandardExecutionProgress(final ProcessGroup rootGroup, final List<FlowFileQueue> internalFlowFileQueues, final BlockingQueue<TriggerResult> resultQueue,
                                     final ByteArrayContentRepository contentRepository, final Set<String> failurePortNames, final AsynchronousCommitTracker commitTracker,
                                     final StatelessStateManagerProvider stateManagerProvider) {
        this.rootGroup = rootGroup;
        this.internalFlowFileQueues = internalFlowFileQueues;
        this.resultQueue = resultQueue;
        this.contentRepository = contentRepository;
        this.failurePortNames = failurePortNames;
        this.commitTracker = commitTracker;
        this.stateManagerProvider = stateManagerProvider;

        completionActionQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public boolean isDataQueued() {
        for (final FlowFileQueue queue : internalFlowFileQueues) {
            if (!queue.isActiveQueueEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public CompletionAction awaitCompletionAction() throws InterruptedException {
        if (canceled) {
            return CompletionAction.CANCEL;
        }

        final CompletionAction existingAction = this.completionAction;
        if (existingAction != null) {
            return existingAction;
        }

        final TriggerResult triggerResult = createResult();
        resultQueue.offer(triggerResult);

        final CompletionAction completionAction = completionActionQueue.take();
        // Hold onto the result so that the other Process Sessions that call this method can retrieve the result.
        this.completionAction = completionAction;
        return completionAction;
    }

    private TriggerResult createResult() {
        final Map<String, List<FlowFile>> outputFlowFiles = drainOutputQueues();

        for (final String failurePortName : failurePortNames) {
            final List<FlowFile> flowFilesForPort = outputFlowFiles.get(failurePortName);
            if (flowFilesForPort != null && !flowFilesForPort.isEmpty()) {
                throw new FailurePortEncounteredException("FlowFile was transferred to Port " + failurePortName + ", which is marked as a Failure Port");
            }
        }

        final boolean canceled = isCanceled();

        return new TriggerResult() {
            @Override
            public boolean isSuccessful() {
                return true;
            }

            @Override
            public boolean isCanceled() {
                return canceled;
            }

            @Override
            public Optional<Throwable> getFailureCause() {
                return Optional.empty();
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
            public byte[] readContent(final FlowFile flowFile) {
                if (!(flowFile instanceof FlowFileRecord)) {
                    throw new IllegalArgumentException("FlowFile was not created by this flow");
                }

                final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;
                final ContentClaim contentClaim = flowFileRecord.getContentClaim();
                final byte[] contentClaimContents = contentRepository.getBytes(contentClaim);
                final long offset = flowFileRecord.getContentClaimOffset();
                final long size = flowFileRecord.getSize();

                if (offset == 0 && size == contentClaimContents.length) {
                    return contentClaimContents;
                }

                final byte[] flowFileContents = Arrays.copyOfRange(contentClaimContents, (int) offset, (int) (size + offset));
                return flowFileContents;
            }

            @Override
            public void acknowledge() {
                commitTracker.triggerCallbacks();
                stateManagerProvider.commitUpdates();
                completionActionQueue.offer(CompletionAction.COMPLETE);
            }
        };
    }

    @Override
    public void notifyExecutionCanceled() {
        canceled = true;
        commitTracker.triggerFailureCallbacks(new RuntimeException("Dataflow Canceled"));
        stateManagerProvider.rollbackUpdates();
        completionActionQueue.offer(CompletionAction.CANCEL);
    }

    @Override
    public void notifyExecutionFailed(final Throwable cause) {
        commitTracker.triggerFailureCallbacks(cause);
        stateManagerProvider.rollbackUpdates();
        completionActionQueue.offer(CompletionAction.CANCEL);
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
            final DrainableFlowFileQueue flowFileQueue = (DrainableFlowFileQueue) connection.getFlowFileQueue();
            final List<FlowFileRecord> flowFileRecords = new ArrayList<>(flowFileQueue.size().getObjectCount());
            flowFileQueue.drainTo(flowFileRecords);
            portFlowFiles.addAll(flowFileRecords);

            for (final FlowFileRecord flowFileRecord : flowFileRecords) {
                contentRepository.decrementClaimantCount(flowFileRecord.getContentClaim());
            }
        }

        return portFlowFiles;
    }
}
