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
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StatelessProcessSession extends StandardProcessSession {
    private static final Logger logger = LoggerFactory.getLogger(StatelessProcessSession.class);
    private static final String PARENT_FLOW_GROUP_ID = "stateless-flow";

    private final RepositoryContext context;
    private final StatelessProcessSessionFactory sessionFactory;
    private final ProcessContextFactory processContextFactory;
    private final Set<String> failurePortNames;

    public StatelessProcessSession(final RepositoryContext context, final StatelessProcessSessionFactory sessionFactory, final ProcessContextFactory processContextFactory,
                                   final Set<String> failurePortNames) {
        super(context, () -> false);
        this.context = context;
        this.sessionFactory = sessionFactory;
        this.processContextFactory = processContextFactory;
        this.failurePortNames = failurePortNames;
    }

    @Override
    protected void commit(final StandardProcessSession.Checkpoint checkpoint) {
        super.commit(checkpoint);

        final long followOnStart = System.nanoTime();
        for (final Connection connection : context.getConnectable().getConnections()) {
            while (!connection.getFlowFileQueue().isEmpty()) {
                final Connectable connectable = connection.getDestination();
                if (isTerminalPort(connectable)) {
                    if (failurePortNames.contains(connectable.getName())) {
                        abortProcessing();
                        throw new FailurePortEncounteredException("Flow failed because a FlowFile was routed from " + connection.getSource() + " to Failure Port " + connection.getDestination());
                    }

                    break;
                }

                final ProcessContext connectableContext = processContextFactory.createProcessContext(connectable);
                final ProcessSessionFactory connectableSessionFactory = new StatelessProcessSessionFactory(connectable, this.sessionFactory.getRepositoryContextFactory(),
                    processContextFactory, failurePortNames);

                logger.debug("Triggering {}", connectable);
                final long start = System.nanoTime();
                try {
                    connectable.onTrigger(connectableContext, connectableSessionFactory);
                } catch (final Throwable t) {
                    abortProcessing();
                    throw t;
                }

                final long nanos = System.nanoTime() - start;
                registerProcessEvent(connectable, nanos);
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
    }

    private void abortProcessing() {
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
