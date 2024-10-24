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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.ConnectionUtils;
import org.apache.nifi.connectable.ConnectionUtils.FlowFileCloneResult;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.apache.nifi.stateless.session.StatelessProcessSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class StandardStatelessFlowCurrent implements StatelessFlowCurrent {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessFlowCurrent.class);

    private final TransactionThresholdMeter transactionThresholdMeter;
    private final AsynchronousCommitTracker tracker;
    private final ExecutionProgress executionProgress;
    private final Set<Connectable> rootConnectables;
    private final FlowFileSupplier flowFileSupplier;
    private final ProvenanceEventRepository provenanceEventRepository;
    private final Collection<Port> inputPorts;
    private final RepositoryContextFactory repositoryContextFactory;
    private final ProcessContextFactory processContextFactory;
    private final LifecycleStateManager lifecycleStateManager;


    private StandardStatelessFlowCurrent(final Builder builder) {
        this.transactionThresholdMeter = builder.transactionThresholdMeter;
        this.tracker = builder.tracker;
        this.executionProgress = builder.executionProgress;
        this.rootConnectables = builder.rootConnectables;
        this.flowFileSupplier = builder.flowFileSupplier;
        this.provenanceEventRepository = builder.provenanceEventRepository;
        this.inputPorts = builder.inputPorts;
        this.repositoryContextFactory = builder.repositoryContextFactory;
        this.processContextFactory = builder.processContextFactory;
        this.lifecycleStateManager = builder.lifecycleStateManager;
    }

    @Override
    public void triggerFlow() {
        try {
            boolean completionReached = false;
            while (!completionReached) {
                triggerRootConnectables();

                while (tracker.isAnyReady()) {
                    final Connectable connectable = tracker.getNextReady();
                    logger.debug("The next ready component to be triggered: {}", connectable);

                    // Continually trigger the given component as long as it is ready to be triggered
                    final NextConnectable nextConnectable = triggerWhileReady(connectable);

                    // If there's nothing left to do, return
                    if (nextConnectable == NextConnectable.NONE) {
                        return;
                    }

                    // If next connectable is whatever is ready, just continue loop
                    if (nextConnectable == NextConnectable.NEXT_READY) {
                        continue;
                    }

                    // Otherwise, we need to break out of this loop so that we can trigger root connectables or complete dataflow
                    break;
                }


                // We have reached completion if the tracker does not know of any components ready to be triggered AND
                // we have no data queued in the flow (with the exception of Output Ports).
                completionReached = !tracker.isAnyReady() && isFlowQueueEmpty();
            }
        } catch (final Throwable t) {
            executionProgress.notifyExecutionFailed(t);
            tracker.triggerFailureCallbacks(t);
            throw t;
        }
    }

    /**
     * Returns <code>true</code> if all data in the flow has been fully processed. This includes both 'internal queues'
     * that are available via the executionProgress, as well as considering any data that has been consumed from the queues by
     * the 'rootConnectables' that has not yet completed processing
     *
     * @return <code>true</code> if all FlowFiles have completed processing and no data is available, <code>false</code> otherwise
     */
    private boolean isFlowQueueEmpty() {
        if (executionProgress.isDataQueued()) {
            return false;
        }

        for (final Connectable rootConnectable : rootConnectables) {
            for (final Connection connection : rootConnectable.getIncomingConnections()) {
                if (connection.getFlowFileQueue().isUnacknowledgedFlowFile()) {
                    return false;
                }
            }
        }

        return true;
    }

    private void triggerRootConnectables() {
        final boolean transactionThresholdsMet = transactionThresholdMeter.isThresholdMet();
        final boolean flowFileSupplied;
        if (transactionThresholdsMet) {
            flowFileSupplied = false;
        } else {
            flowFileSupplied = triggerFlowFileSupplier();
        }

        // If we weren't able to pull in any FlowFiles from outside the Stateless Flow, go ahead and trigger the root connectables.
        if (!flowFileSupplied) {
            for (final Connectable connectable : rootConnectables) {
                if (!transactionThresholdsMet || connectable.hasIncomingConnection()) {
                    triggerRootConnectable(connectable);
                }
            }
        }
    }

    private boolean triggerFlowFileSupplier() {
        if (flowFileSupplier == null) {
            return false;
        }

        boolean flowFileSupplied = false;
        for (final Port inputPort : inputPorts) {
            final Set<Connection> outputConnections = inputPort.getConnections();
            if (outputConnections.isEmpty()) {
                continue;
            }

            final Optional<FlowFile> flowFileOptional = flowFileSupplier.getFlowFile(inputPort.getName());
            if (!flowFileOptional.isPresent()) {
                continue;
            }

            flowFileSupplied = true;

            final FlowFileRecord flowFile = (FlowFileRecord) flowFileOptional.get();
            final FlowFileCloneResult cloneResult = ConnectionUtils.clone(flowFile, outputConnections,
                repositoryContextFactory.getFlowFileRepository(), repositoryContextFactory.getContentRepository());

            cloneResult.distributeFlowFiles();

            for (final Connection connection : outputConnections) {
                tracker.addConnectable(connection.getDestination());
            }
        }

        return flowFileSupplied;
    }

    private void triggerRootConnectable(final Connectable connectable) {
        final LifecycleState lifecycleState = lifecycleStateManager.getOrRegisterLifecycleState(connectable.getIdentifier(), true, false);

        // Reset progress and trigger the component. This allows us to track whether or not any progress was made by the given connectable
        // during this invocation of its onTrigger method.
        tracker.resetProgress();

        final StatelessProcessSessionFactory statelessSessionFactory = new StatelessProcessSessionFactory(connectable, repositoryContextFactory, provenanceEventRepository, processContextFactory,
            executionProgress, false, tracker);

        lifecycleState.incrementActiveThreadCount(null);
        try {
            trigger(connectable, statelessSessionFactory);
        } finally {
            lifecycleState.decrementActiveThreadCount();
        }

        // Keep track of the output of the source component so that we can determine whether or not we've reached our transaction threshold.
        transactionThresholdMeter.incrementFlowFiles(tracker.getFlowFilesProduced());
        transactionThresholdMeter.incrementBytes(tracker.getBytesProduced());
    }

    private NextConnectable triggerWhileReady(final Connectable connectable) {
        final LifecycleState lifecycleState = lifecycleStateManager.getOrRegisterLifecycleState(connectable.getIdentifier(), true, false);

        final StatelessProcessSessionFactory statelessSessionFactory = new StatelessProcessSessionFactory(connectable, repositoryContextFactory, provenanceEventRepository, processContextFactory,
            executionProgress, false, tracker);

        lifecycleState.incrementActiveThreadCount(null);
        try {
            while (tracker.isReady(connectable)) {
                if (executionProgress.isCanceled()) {
                    logger.info("Dataflow was canceled so will not trigger any more components");
                    return NextConnectable.NONE;
                }

                // Reset progress and trigger the component. This allows us to track whether or not any progress was made by the given connectable
                // during this invocation of its onTrigger method.
                tracker.resetProgress();
                trigger(connectable, statelessSessionFactory);

                // Check if the component made any progress or not. If so, continue on. If not, we need to check if providing the component with
                // additional input would help the component to progress or not.
                final boolean progressed = tracker.isProgress();
                if (progressed) {
                    logger.debug("{} was triggered and made progress", connectable);
                    continue;
                }

                // If we've made no progress, check the condition of this being an Output Port with Batch Output. In such a case, we will make no progress
                // until data has been processed elsewhere in the flow, so return NEXT_READY.
                if (connectable.getConnectableType() == ConnectableType.OUTPUT_PORT && connectable.getProcessGroup().getFlowFileOutboundPolicy() == FlowFileOutboundPolicy.BATCH_OUTPUT
                    && connectable.getProcessGroup().isDataQueuedForProcessing()) {

                    logger.debug("{} was triggered but unable to make process. Data is still available for processing, so continue triggering components within the Process Group", connectable);
                    return NextConnectable.NEXT_READY;
                }

                // Check if we've reached out threshold for how much data we are willing to bring into a single transaction. If so, we will not drop back to
                // triggering source components
                final boolean thresholdMet = transactionThresholdMeter.isThresholdMet();
                if (thresholdMet) {
                    logger.debug("{} was triggered but unable to make progress. The transaction thresholds {} have been met (currently at {}). Will not trigger source components to run.",
                            connectable, transactionThresholdMeter.getThresholds(), transactionThresholdMeter);
                    continue;
                }

                logger.debug("{} was triggered but unable to make progress. Maximum transaction thresholds {} have not been reached (currently at {}) so will trigger source components to run.",
                        connectable, transactionThresholdMeter.getThresholds(), transactionThresholdMeter);

                return NextConnectable.SOURCE_CONNECTABLE;
            }

            return NextConnectable.NEXT_READY;
        } finally {
            lifecycleState.decrementActiveThreadCount();
        }
    }


    private void trigger(final Connectable connectable, final ProcessSessionFactory sessionFactory) {
        final ProcessContext processContext = processContextFactory.createProcessContext(connectable);

        final long start = System.nanoTime();

        // Trigger component
        logger.debug("Triggering {}", connectable);
        connectable.onTrigger(processContext, sessionFactory);

        final long processingNanos = System.nanoTime() - start;
        registerProcessEvent(connectable, 1, processingNanos);
    }

    private void registerProcessEvent(final Connectable connectable, final int invocations, final long processingNanos) {
        try {
            final StandardFlowFileEvent procEvent = new StandardFlowFileEvent();
            procEvent.setProcessingNanos(processingNanos);
            procEvent.setInvocations(invocations);
            repositoryContextFactory.getFlowFileEventRepository().updateRepository(procEvent, connectable.getIdentifier());
        } catch (final IOException e) {
            logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", connectable.getRunnableComponent(), e, e);
        }
    }

    private enum NextConnectable {
        NEXT_READY,

        SOURCE_CONNECTABLE,

        NONE
    }

    public static class Builder {
        private TransactionThresholdMeter transactionThresholdMeter;
        private AsynchronousCommitTracker tracker;
        private ExecutionProgress executionProgress;
        private Set<Connectable> rootConnectables;
        private Collection<Port> inputPorts;
        private FlowFileSupplier flowFileSupplier = null;
        private ProvenanceEventRepository provenanceEventRepository;
        private RepositoryContextFactory repositoryContextFactory;
        private ProcessContextFactory processContextFactory;
        private LifecycleStateManager lifecycleStateManager;

        public StandardStatelessFlowCurrent build() {
            Objects.requireNonNull(transactionThresholdMeter, "Transaction Threshold Meter must be set");
            Objects.requireNonNull(tracker, "Commit Tracker must be set");
            Objects.requireNonNull(executionProgress, "Execution Progress must be set");
            Objects.requireNonNull(rootConnectables, "Root Conectables must be set");
            Objects.requireNonNull(repositoryContextFactory, "Repository Context Factory must be set");
            Objects.requireNonNull(provenanceEventRepository, "Provenance Event Repository must be set");
            Objects.requireNonNull(processContextFactory, "Process Context Factory must be set");

            return new StandardStatelessFlowCurrent(this);
        }

        public Builder transactionThresholdMeter(final TransactionThresholdMeter transactionThresholdMeter) {
            this.transactionThresholdMeter = transactionThresholdMeter;
            return this;
        }

        public Builder lifecycleStateManager(final LifecycleStateManager lifecycleStateManager) {
            this.lifecycleStateManager = lifecycleStateManager;
            return this;
        }

        public Builder commitTracker(final AsynchronousCommitTracker commitTracker) {
            this.tracker = commitTracker;
            return this;
        }

        public Builder executionProgress(final ExecutionProgress executionProgress) {
            this.executionProgress = executionProgress;
            return this;
        }

        public Builder rootConnectables(final Set<Connectable> rootConnectables) {
            this.rootConnectables = rootConnectables;
            return this;
        }

        public Builder flowFileSupplier(final FlowFileSupplier flowFileSupplier) {
            this.flowFileSupplier = flowFileSupplier;
            return this;
        }

        public Builder provenanceEventRepository(final ProvenanceEventRepository provenanceEventRepository) {
            this.provenanceEventRepository = provenanceEventRepository;
            return this;
        }

        public Builder inputPorts(final Collection<Port> inputPorts) {
            this.inputPorts = inputPorts;
            return this;
        }

        public Builder repositoryContextFactory(final RepositoryContextFactory repositoryContextFactory) {
            this.repositoryContextFactory = repositoryContextFactory;
            return this;
        }

        public Builder processContextFactory(final ProcessContextFactory processContextFactory) {
            this.processContextFactory = processContextFactory;
            return this;
        }
    }
}
