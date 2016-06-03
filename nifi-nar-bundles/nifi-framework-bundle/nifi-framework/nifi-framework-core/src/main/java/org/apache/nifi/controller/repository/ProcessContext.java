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
package org.apache.nifi.controller.repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.util.Connectables;

/**
 *
 */
public class ProcessContext {

    private final Connectable connectable;
    private final ContentRepository contentRepo;
    private final FlowFileRepository flowFileRepo;
    private final FlowFileEventRepository flowFileEventRepo;
    private final CounterRepository counterRepo;
    private final ProvenanceEventRepository provenanceRepo;
    private final AtomicLong connectionIndex;

    public ProcessContext(final Connectable connectable, final AtomicLong connectionIndex, final ContentRepository contentRepository,
            final FlowFileRepository flowFileRepository, final FlowFileEventRepository flowFileEventRepository,
            final CounterRepository counterRepository, final ProvenanceEventRepository provenanceRepository) {
        this.connectable = connectable;
        contentRepo = contentRepository;
        flowFileRepo = flowFileRepository;
        flowFileEventRepo = flowFileEventRepository;
        counterRepo = counterRepository;
        provenanceRepo = provenanceRepository;

        this.connectionIndex = connectionIndex;
    }

    Connectable getConnectable() {
        return connectable;
    }

    /**
     *
     * @param relationship relationship
     * @return connections for relationship
     */
    Collection<Connection> getConnections(final Relationship relationship) {
        Collection<Connection> collection = connectable.getConnections(relationship);
        if (collection == null) {
            collection = new ArrayList<>();
        }
        return Collections.unmodifiableCollection(collection);
    }

    /**
     * @return an unmodifiable list containing a copy of all incoming connections for the processor from which FlowFiles are allowed to be pulled
     */
    List<Connection> getPollableConnections() {
        if (pollFromSelfLoopsOnly()) {
            final List<Connection> selfLoops = new ArrayList<>();
            for (final Connection connection : connectable.getIncomingConnections()) {
                if (connection.getSource() == connection.getDestination()) {
                    selfLoops.add(connection);
                }
            }

            return selfLoops;
        } else {
            return connectable.getIncomingConnections();
        }
    }

    private boolean isTriggerWhenAnyDestinationAvailable() {
        if (connectable.getConnectableType() != ConnectableType.PROCESSOR) {
            return false;
        }

        final ProcessorNode procNode = (ProcessorNode) connectable;
        return procNode.isTriggerWhenAnyDestinationAvailable();
    }

    /**
     * @return true if we are allowed to take FlowFiles only from self-loops. This is the case when no Relationships are available except for self-looping Connections
     */
    private boolean pollFromSelfLoopsOnly() {
        if (isTriggerWhenAnyDestinationAvailable()) {
            // we can pull from any incoming connection, as long as at least one downstream connection
            // is available for each relationship.
            // I.e., we can poll only from self if no relationships are available
            return !Connectables.anyRelationshipAvailable(connectable);
        } else {
            for (final Connection connection : connectable.getConnections()) {
                // A downstream connection is full. We are only allowed to pull from self-loops.
                if (connection.getFlowFileQueue().isFull()) {
                    return true;
                }
            }
        }

        return false;
    }

    void adjustCounter(final String name, final long delta) {
        final String localContext = connectable.getName() + " (" + connectable.getIdentifier() + ")";
        final String globalContext = "All " + connectable.getComponentType() + "'s";

        counterRepo.adjustCounter(localContext, name, delta);
        counterRepo.adjustCounter(globalContext, name, delta);
    }

    ContentRepository getContentRepository() {
        return contentRepo;
    }

    FlowFileRepository getFlowFileRepository() {
        return flowFileRepo;
    }

    public FlowFileEventRepository getFlowFileEventRepository() {
        return flowFileEventRepo;
    }

    ProvenanceEventRepository getProvenanceRepository() {
        return provenanceRepo;
    }

    long getNextFlowFileSequence() {
        return flowFileRepo.getNextFlowFileSequence();
    }

    int getNextIncomingConnectionIndex() {
        final int numIncomingConnections = connectable.getIncomingConnections().size();
        return (int) (connectionIndex.getAndIncrement() % Math.max(1, numIncomingConnections));
    }

    public boolean isAnyRelationshipAvailable() {
        for (final Relationship relationship : getConnectable().getRelationships()) {
            final Collection<Connection> connections = getConnections(relationship);

            boolean available = true;
            for (final Connection connection : connections) {
                if (connection.getFlowFileQueue().isFull()) {
                    available = false;
                    break;
                }
            }

            if (available) {
                return true;
            }
        }

        return false;
    }

    public int getAvailableRelationshipCount() {
        int count = 0;
        for (final Relationship relationship : connectable.getRelationships()) {
            final Collection<Connection> connections = connectable.getConnections(relationship);
            if (connections == null || connections.isEmpty()) {
                count++;
            } else {
                boolean available = true;
                for (final Connection connection : connections) {
                    // consider self-loops available
                    if (connection.getSource() == connection.getDestination()) {
                        continue;
                    }

                    if (connection.getFlowFileQueue().isFull()) {
                        available = false;
                        break;
                    }
                }

                if (available) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * A Relationship is said to be Available if and only if all Connections for that Relationship are either self-loops or have non-full queues.
     *
     * @param requiredNumber minimum number of relationships that must have availability
     * @return Checks if at least <code>requiredNumber</code> of Relationationships are "available." If so, returns <code>true</code>, otherwise returns <code>false</code>
     */
    public boolean isRelationshipAvailabilitySatisfied(final int requiredNumber) {
        int unavailable = 0;

        final Collection<Relationship> allRelationships = connectable.getRelationships();
        final int numRelationships = allRelationships.size();

        // the maximum number of Relationships that can be unavailable and still return true.
        final int maxUnavailable = numRelationships - requiredNumber;

        for (final Relationship relationship : allRelationships) {
            final Collection<Connection> connections = connectable.getConnections(relationship);
            if (connections != null && !connections.isEmpty()) {
                boolean available = true;
                for (final Connection connection : connections) {
                    // consider self-loops available
                    if (connection.getSource() == connection.getDestination()) {
                        continue;
                    }

                    if (connection.getFlowFileQueue().isFull()) {
                        available = false;
                        break;
                    }
                }

                if (!available) {
                    unavailable++;
                    if (unavailable > maxUnavailable) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public Set<Relationship> getAvailableRelationships() {
        final Set<Relationship> set = new HashSet<>();
        for (final Relationship relationship : getConnectable().getRelationships()) {
            final Collection<Connection> connections = getConnections(relationship);
            if (connections.isEmpty()) {
                set.add(relationship);
            } else {
                boolean available = true;
                for (final Connection connection : connections) {
                    if (connection.getFlowFileQueue().isFull()) {
                        available = false;
                    }
                }

                if (available) {
                    set.add(relationship);
                }
            }
        }

        return set;
    }
}
