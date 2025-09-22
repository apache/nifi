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
import org.apache.nifi.controller.status.FlowFileAvailability;
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

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final FlowFileSwapManager swapManager;
    private final TimedLock writeLock;


    public StandardFlowFileQueue(final String identifier, final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo,
                                 final ProcessScheduler scheduler, final FlowFileSwapManager swapManager, final EventReporter eventReporter,
                                 final int swapThreshold, final String expirationPeriod, final long defaultBackPressureObjectThreshold, final String defaultBackPressureDataSizeThreshold) {

        super(identifier, scheduler, flowFileRepo, provRepo);
        super.setFlowFileExpiration(expirationPeriod);
        this.swapManager = swapManager;
        this.queue = new SwappablePriorityQueue(swapManager, swapThreshold, eventReporter, this, this::drop, null);

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
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> files) {
        queue.putAll(files);
    }


    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords, final PollStrategy pollStrategy) {
        // First check if we have any records Pre-Fetched.
        final long expirationMillis = getFlowFileExpiration(TimeUnit.MILLISECONDS);
        return queue.poll(expiredRecords, expirationMillis, pollStrategy);
    }


    @Override
    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords, final PollStrategy pollStrategy) {
        return queue.poll(maxResults, expiredRecords, getFlowFileExpiration(TimeUnit.MILLISECONDS), pollStrategy);
    }



    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        queue.acknowledge(flowFile);
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        queue.acknowledge(flowFiles);
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
    public long getTotalQueuedDuration(long fromTimestamp) {
        return queue.getTotalQueuedDuration(fromTimestamp);
    }

    @Override
    public long getMinLastQueueDate() {
        return queue.getMinLastQueueDate();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public FlowFileAvailability getFlowFileAvailability() {
        return queue.getFlowFileAvailability();
    }

    @Override
    public boolean isActiveQueueEmpty() {
        final FlowFileQueueSize queueSize = queue.getFlowFileQueueSize();
        return queueSize.getActiveCount() == 0 && queueSize.getSwappedCount() == 0;
    }

    @Override
    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords, final PollStrategy pollStrategy) {
        return queue.poll(filter, expiredRecords, getFlowFileExpiration(TimeUnit.MILLISECONDS), pollStrategy);
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
    @Override
    public void lock() {
        writeLock.lock();
    }

    /**
     * Unlock the queue
     */
    @Override
    public void unlock() {
        writeLock.unlock("external unlock");
    }
}
