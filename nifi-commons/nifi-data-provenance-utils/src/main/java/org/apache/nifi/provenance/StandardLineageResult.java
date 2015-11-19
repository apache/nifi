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
package org.apache.nifi.provenance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.EdgeNode;
import org.apache.nifi.provenance.lineage.EventNode;
import org.apache.nifi.provenance.lineage.FlowFileNode;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;

/**
 *
 */
public class StandardLineageResult implements ComputeLineageResult {

    public static final int TTL = (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
    private static final Logger logger = LoggerFactory.getLogger(StandardLineageResult.class);

    private final Collection<String> flowFileUuids;
    private final Collection<ProvenanceEventRecord> relevantRecords = new ArrayList<>();
    private final Set<LineageNode> nodes = new HashSet<>();
    private final Set<LineageEdge> edges = new HashSet<>();
    private final int numSteps;
    private final long creationNanos;
    private long computationNanos;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private Date expirationDate = null;
    private String error = null;
    private int numCompletedSteps = 0;

    private volatile boolean canceled = false;

    public StandardLineageResult(final int numSteps, final Collection<String> flowFileUuids) {
        this.numSteps = numSteps;
        this.creationNanos = System.nanoTime();
        this.flowFileUuids = flowFileUuids;

        updateExpiration();
    }

    @Override
    public List<LineageNode> getNodes() {
        readLock.lock();
        try {
            return new ArrayList<>(nodes);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<LineageEdge> getEdges() {
        readLock.lock();
        try {
            return new ArrayList<>(edges);
        } finally {
            readLock.unlock();
        }
    }

    public int getNumberOfEdges() {
        readLock.lock();
        try {
            return edges.size();
        } finally {
            readLock.unlock();
        }
    }

    public int getNumberOfNodes() {
        readLock.lock();
        try {
            return nodes.size();
        } finally {
            readLock.unlock();
        }
    }

    public long getComputationTime(final TimeUnit timeUnit) {
        readLock.lock();
        try {
            return timeUnit.convert(computationNanos, TimeUnit.NANOSECONDS);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Date getExpiration() {
        readLock.lock();
        try {
            return expirationDate;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getError() {
        readLock.lock();
        try {
            return error;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getPercentComplete() {
        readLock.lock();
        try {
            return (numSteps < 1) ? 100 : (int) (((float) numCompletedSteps / (float) numSteps) * 100.0F);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isFinished() {
        readLock.lock();
        try {
            return numCompletedSteps >= numSteps || canceled;
        } finally {
            readLock.unlock();
        }
    }

    public void setError(final String error) {
        writeLock.lock();
        try {
            this.error = error;
            numCompletedSteps++;

            updateExpiration();

            if (numCompletedSteps >= numSteps) {
                computationNanos = System.nanoTime() - creationNanos;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void update(final Collection<ProvenanceEventRecord> records) {
        writeLock.lock();
        try {
            relevantRecords.addAll(records);

            numCompletedSteps++;
            updateExpiration();

            if (numCompletedSteps >= numSteps && error == null) {
                computeLineage();
                computationNanos = System.nanoTime() - creationNanos;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Computes the lineage from the relevant Provenance Event Records. This
     * method must be called with the write lock held and is only going to be
     * useful after all of the records have been successfully obtained
     */
    private void computeLineage() {
        final long startNanos = System.nanoTime();

        nodes.clear();
        edges.clear();

        Map<String, LineageNode> lastEventMap = new HashMap<>();    // maps FlowFile UUID to last event for that FlowFile
        final List<ProvenanceEventRecord> sortedRecords = new ArrayList<>(relevantRecords);
        Collections.sort(sortedRecords, new Comparator<ProvenanceEventRecord>() {
            @Override
            public int compare(final ProvenanceEventRecord o1, final ProvenanceEventRecord o2) {
                // Sort on Event Time, then Event ID.
                final int eventTimeComparison = Long.compare(o1.getEventTime(), o2.getEventTime());
                if (eventTimeComparison == 0) {
                    return Long.compare(o1.getEventId(), o2.getEventId());
                } else {
                    return eventTimeComparison;
                }
            }
        });

        // convert the StandardProvenanceRecord objects into Lineage nodes (FlowFileNode, EventNodes).
        for (final ProvenanceEventRecord record : sortedRecords) {
            final LineageNode lineageNode = new EventNode(record);
            final boolean added = nodes.add(lineageNode);
            if (!added) {
                logger.debug("Did not add {} because it already exists in the 'nodes' set", lineageNode);
            }

            // Create an edge that connects this node to the previous node for the same FlowFile UUID.
            final LineageNode lastNode = lastEventMap.get(record.getFlowFileUuid());
            if (lastNode != null) {
                // We calculate the Edge UUID based on whether or not this event is a SPAWN.
                // If this event is a SPAWN, then we want to use the previous node's UUID because a
                // SPAWN Event's UUID is not necessarily what we want, since a SPAWN Event's UUID pertains to
                // only one of (potentially) many UUIDs associated with the event. Otherwise, we know that
                // the UUID of this record is appropriate, so we just use it.
                final String edgeUuid;

                switch (record.getEventType()) {
                    case JOIN:
                    case CLONE:
                    case REPLAY:
                        edgeUuid = lastNode.getFlowFileUuid();
                        break;
                    default:
                        edgeUuid = record.getFlowFileUuid();
                        break;
                }

                edges.add(new EdgeNode(edgeUuid, lastNode, lineageNode));
            }

            lastEventMap.put(record.getFlowFileUuid(), lineageNode);

            switch (record.getEventType()) {
                case FORK:
                case JOIN:
                case REPLAY:
                case CLONE: {
                    // For events that create FlowFile nodes, we need to create the FlowFile Nodes and associated Edges, as appropriate
                    for (final String childUuid : record.getChildUuids()) {
                        if (flowFileUuids.contains(childUuid)) {
                            final FlowFileNode childNode = new FlowFileNode(childUuid, record.getEventTime());
                            final boolean isNewFlowFile = nodes.add(childNode);
                            if (!isNewFlowFile) {
                                final String msg = "Unable to generate Lineage Graph because multiple "
                                        + "events were registered claiming to have generated the same FlowFile (UUID = " + childNode.getFlowFileUuid() + ")";
                                logger.error(msg);
                                setError(msg);
                                return;
                            }

                            edges.add(new EdgeNode(childNode.getFlowFileUuid(), lineageNode, childNode));
                            lastEventMap.put(childUuid, childNode);
                        }
                    }
                    for (final String parentUuid : record.getParentUuids()) {
                        LineageNode lastNodeForParent = lastEventMap.get(parentUuid);
                        if (lastNodeForParent != null && !lastNodeForParent.equals(lineageNode)) {
                            edges.add(new EdgeNode(parentUuid, lastNodeForParent, lineageNode));
                        }

                        lastEventMap.put(parentUuid, lineageNode);
                    }
                }
                break;
                case RECEIVE:
                case FETCH:
                case CREATE: {
                    // for a receive event, we want to create a FlowFile Node that represents the FlowFile received
                    // and create an edge from the Receive Event to the FlowFile Node
                    final LineageNode flowFileNode = new FlowFileNode(record.getFlowFileUuid(), record.getEventTime());
                    final boolean isNewFlowFile = nodes.add(flowFileNode);
                    if (!isNewFlowFile) {
                        final String msg = "Found cycle in graph. This indicates that multiple events "
                                + "were registered claiming to have generated the same FlowFile (UUID = " + flowFileNode.getFlowFileUuid() + ")";
                        setError(msg);
                        logger.error(msg);
                        return;
                    }
                    edges.add(new EdgeNode(record.getFlowFileUuid(), lineageNode, flowFileNode));
                    lastEventMap.put(record.getFlowFileUuid(), flowFileNode);
                }
                break;
                default:
                    break;
            }
        }

        final long nanos = System.nanoTime() - startNanos;
        logger.debug("Finished building lineage with {} nodes and {} edges in {} millis", nodes.size(), edges.size(), TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    void cancel() {
        this.canceled = true;
    }

    /**
     * Must be called with write lock!
     */
    private void updateExpiration() {
        expirationDate = new Date(System.currentTimeMillis() + TTL);
    }
}
