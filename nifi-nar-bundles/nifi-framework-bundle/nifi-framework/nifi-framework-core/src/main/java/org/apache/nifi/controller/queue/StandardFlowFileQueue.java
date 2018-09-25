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
package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.util.concurrency.TimedLock;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A FlowFileQueue is used to queue FlowFile objects that are awaiting further
 * processing. Must be thread safe.
 *
 */
public class StandardFlowFileQueue extends AbstractFlowFileQueue implements FlowFileQueue {

    private final SwappablePriorityQueue queue;
    private final ConnectionEventListener eventListener;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final FlowFileSwapManager swapManager;
    private final TimedLock writeLock;


    public StandardFlowFileQueue(final String identifier, final ConnectionEventListener eventListener, final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo,
                                 final ResourceClaimManager resourceClaimManager, final ProcessScheduler scheduler, final FlowFileSwapManager swapManager, final EventReporter eventReporter,
                                 final int swapThreshold, final long defaultBackPressureObjectThreshold, final String defaultBackPressureDataSizeThreshold) {

        super(identifier, scheduler, flowFileRepo, provRepo, resourceClaimManager);
        this.swapManager = swapManager;
        this.queue = new SwappablePriorityQueue(swapManager, swapThreshold, eventReporter, this, this::drop, null);
        this.eventListener = eventListener;

        writeLock = new TimedLock(this.lock.writeLock(), getIdentifier() + " Write Lock", 100);

        setBackPressureDataSizeThreshold(defaultBackPressureDataSizeThreshold);
        setBackPressureObjectThreshold(defaultBackPressureObjectThreshold);
    }

    @Override
    public void startLoadBalancing() {
    }

    @Override
    public void stopLoadBalancing() {
    }

    @Override
    public void offloadQueue() {
    }

    @Override
    public void resetOffloadedQueue() {
    }

    @Override
    public boolean isActivelyLoadBalancing() {
        return false;
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        queue.setPriorities(newPriorities);
    }

    @Override
    public List<FlowFilePrioritizer> getPriorities() {
        return queue.getPriorities();
    }

    @Override
    protected List<FlowFileRecord> getListableFlowFiles() {
        return queue.getActiveFlowFiles();
    }

    @Override
    public QueueDiagnostics getQueueDiagnostics() {
        return new StandardQueueDiagnostics(queue.getQueueDiagnostics(), Collections.emptyList());
    }

    @Override
    public void put(final FlowFileRecord file) {
        queue.put(file);

        eventListener.triggerDestinationEvent();
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> files) {
        queue.putAll(files);

        eventListener.triggerDestinationEvent();
    }


    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        // First check if we have any records Pre-Fetched.
        final long expirationMillis = getFlowFileExpiration(TimeUnit.MILLISECONDS);
        return queue.poll(expiredRecords, expirationMillis);
    }


    @Override
    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords) {
        return queue.poll(maxResults, expiredRecords, getFlowFileExpiration(TimeUnit.MILLISECONDS));
    }



    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        queue.acknowledge(flowFile);

        eventListener.triggerSourceEvent();
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        queue.acknowledge(flowFiles);

        eventListener.triggerSourceEvent();
    }

    @Override
    public boolean isUnacknowledgedFlowFile() {
        return queue.isUnacknowledgedFlowFile();
    }

    @Override
    public QueueSize size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.getFlowFileQueueSize().isEmpty();
    }

    @Override
    public boolean isActiveQueueEmpty() {
        final FlowFileQueueSize queueSize = queue.getFlowFileQueueSize();
        return queueSize.getActiveCount() == 0 && queueSize.getSwappedCount() == 0;
    }

    @Override
    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords) {
        return queue.poll(filter, expiredRecords, getFlowFileExpiration(TimeUnit.MILLISECONDS));
    }

    @Override
    public void purgeSwapFiles() {
        swapManager.purge();
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        return queue.recoverSwappedFlowFiles();
    }

    @Override
    public String toString() {
        return "FlowFileQueue[id=" + getIdentifier() + "]";
    }


    @Override
    public FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException {
        return queue.getFlowFile(flowFileUuid);
    }


    @Override
    protected void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor) {
        queue.dropFlowFiles(dropRequest, requestor);
    }


    /**
     * Lock the queue so that other threads are unable to interact with the queue
     */
    public void lock() {
        writeLock.lock();
    }

    /**
     * Unlock the queue
     */
    public void unlock() {
        writeLock.unlock("external unlock");
    }
}
