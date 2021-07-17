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

package org.apache.nifi.stateless.queue;

import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.util.FormatUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessFlowFileQueue implements DrainableFlowFileQueue {
    private final String identifier;
    private volatile long expirationMillis;
    private final BlockingQueue<FlowFileRecord> flowFiles = new LinkedBlockingQueue<>();
    private final AtomicInteger unacknowledgedCount = new AtomicInteger(0);
    private final AtomicLong totalBytes = new AtomicLong(0L);

    public StatelessFlowFileQueue(final String identifier) {
        this.identifier = identifier;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public List<FlowFilePrioritizer> getPriorities() {
        return Collections.emptyList();
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        return null;
    }

    @Override
    public void purgeSwapFiles() {
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
    }

    @Override
    public void setBackPressureObjectThreshold(final long maxQueueSize) {
    }

    @Override
    public long getBackPressureObjectThreshold() {
        return 0;
    }

    @Override
    public void setBackPressureDataSizeThreshold(final String maxDataSize) {
    }

    @Override
    public String getBackPressureDataSizeThreshold() {
        return "0 B";
    }

    @Override
    public QueueSize size() {
        return new QueueSize(flowFiles.size() + unacknowledgedCount.get(), totalBytes.get());
    }

    @Override
    public long getTotalQueuedDuration(long fromTimestamp) {
        long sum = 0L;
        for (FlowFileRecord flowFileRecord : flowFiles) {
            long l = fromTimestamp - flowFileRecord.getLastQueueDate();
            sum += l;
        }
        return sum;
    }

    @Override
    public long getMinLastQueueDate() {
        long min = 0;
        for (FlowFileRecord flowFile : flowFiles) {
            min = min == 0 ? flowFile.getLastQueueDate() : Long.min(min, flowFile.getLastQueueDate());
        }
        return min;
    }

    @Override
    public boolean isEmpty() {
        return flowFiles.isEmpty() && unacknowledgedCount.get() == 0;
    }

    @Override
    public boolean isActiveQueueEmpty() {
        return flowFiles.isEmpty();
    }

    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        unacknowledgedCount.decrementAndGet();
        totalBytes.addAndGet(-flowFile.getSize());
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        unacknowledgedCount.addAndGet(-flowFiles.size());
        flowFiles.forEach(ff -> totalBytes.addAndGet(-ff.getSize()));
    }

    @Override
    public boolean isUnacknowledgedFlowFile() {
        return unacknowledgedCount.get() > 0;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        flowFiles.add(flowFile);
        totalBytes.addAndGet(flowFile.getSize());
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        this.flowFiles.addAll(flowFiles);
        flowFiles.forEach(ff -> totalBytes.addAndGet(ff.getSize()));
    }

    @Override
    public synchronized FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        while (!flowFiles.isEmpty()) {
            final FlowFileRecord flowFile = flowFiles.peek();
            if (flowFile == null) {
                return null;
            }

            if (isExpired(flowFile)) {
                expiredRecords.add(flowFile);
                if (expiredRecords.size() >= 10_000) {
                    return null;
                }

                continue;
            }

            if (flowFile.isPenalized()) {
                return null;
            }

            unacknowledgedCount.incrementAndGet();
            return flowFiles.poll();
        }

        return null;
    }

    private boolean isExpired(final FlowFileRecord flowFile) {
        if (expirationMillis == 0L) {
            return false;
        }

        final long expirationTime = flowFile.getEntryDate() + expirationMillis;
        return System.currentTimeMillis() > expirationTime;
    }

    @Override
    public synchronized List<FlowFileRecord> poll(final int maxResults, final Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> selected = new ArrayList<>(Math.min(maxResults, flowFiles.size()));
        for (int i=0; i < maxResults; i++) {
            final FlowFileRecord flowFile = poll(expiredRecords);
            if (flowFile != null) {
                selected.add(flowFile);
            }

            if (flowFile == null || expiredRecords.size() >= 10_000) {
                break;
            }
        }

        return selected;
    }

    @Override
    public synchronized List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> selected = new ArrayList<>();

        // Use an iterator to iterate over all FlowFiles in the queue. This allows us to
        // Remove from the queue only those FlowFiles that are selected. This, in turn, allows
        // us to retain our FIFO ordering.
        final Iterator<FlowFileRecord> itr = flowFiles.iterator();
        while (itr.hasNext()) {
            final FlowFileRecord flowFile = itr.next();

            if (isExpired(flowFile)) {
                expiredRecords.add(flowFile);
                if (expiredRecords.size() >= 10_000) {
                    break;
                }

                continue;
            }

            if (flowFile.isPenalized()) {
                break;
            }

            final FlowFileFilter.FlowFileFilterResult filterResult = filter.filter(flowFile);
            if (filterResult.isAccept()) {
                selected.add(flowFile);
                itr.remove();
            }

            if (!filterResult.isContinue()) {
                break;
            }
        }

        unacknowledgedCount.addAndGet(selected.size());
        return selected;
    }

    @Override
    public String getFlowFileExpiration() {
        return expirationMillis + " millis";
    }

    @Override
    public int getFlowFileExpiration(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(expirationMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setFlowFileExpiration(final String flowExpirationPeriod) {
        expirationMillis = (int) FormatUtils.getPreciseTimeDuration(flowExpirationPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public DropFlowFileStatus dropFlowFiles(final String requestIdentifier, final String requestor) {
        throw new UnsupportedOperationException("Cannot drop FlowFiles from a queue in Stateless NiFi");
    }

    @Override
    public DropFlowFileStatus getDropFlowFileStatus(final String requestIdentifier) {
        throw new UnsupportedOperationException("Cannot drop FlowFiles from a queue in Stateless NiFi");
    }

    @Override
    public DropFlowFileStatus cancelDropFlowFileRequest(final String requestIdentifier) {
        throw new UnsupportedOperationException("Cannot drop FlowFiles from a queue in Stateless NiFi");
    }

    @Override
    public ListFlowFileStatus listFlowFiles(final String requestIdentifier, final int maxResults) {
        throw new UnsupportedOperationException("Cannot list FlowFiles in a queue in Stateless NiFi");
    }

    @Override
    public ListFlowFileStatus getListFlowFileStatus(final String requestIdentifier) {
        throw new UnsupportedOperationException("Cannot list FlowFiles in a queue in Stateless NiFi");
    }

    @Override
    public ListFlowFileStatus cancelListFlowFileRequest(final String requestIdentifier) {
        throw new UnsupportedOperationException("Cannot list FlowFiles in a queue in Stateless NiFi");
    }

    @Override
    public FlowFileRecord getFlowFile(final String flowFileUuid) {
        throw new UnsupportedOperationException("Cannot fetch particular FlowFile from a queue in Stateless NiFi");
    }

    @Override
    public void verifyCanList() throws IllegalStateException {
        throw new IllegalStateException("Cannot list FlowFiles in a queue in Stateless NiFi");
    }

    @Override
    public QueueDiagnostics getQueueDiagnostics() {
        return null;
    }

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void setLoadBalanceStrategy(final LoadBalanceStrategy strategy, final String partitioningAttribute) {
    }

    @Override
    public void offloadQueue() {
        throw new UnsupportedOperationException("Node Offloading is not supported in Stateless NiFi");
    }

    @Override
    public void resetOffloadedQueue() {
    }

    @Override
    public LoadBalanceStrategy getLoadBalanceStrategy() {
        return LoadBalanceStrategy.DO_NOT_LOAD_BALANCE;
    }

    @Override
    public void setLoadBalanceCompression(final LoadBalanceCompression compression) {
    }

    @Override
    public LoadBalanceCompression getLoadBalanceCompression() {
        return LoadBalanceCompression.DO_NOT_COMPRESS;
    }

    @Override
    public String getPartitioningAttribute() {
        return null;
    }

    @Override
    public void startLoadBalancing() {
    }

    @Override
    public void stopLoadBalancing() {
    }

    @Override
    public boolean isActivelyLoadBalancing() {
        return false;
    }

    @Override
    public void drainTo(final List<FlowFileRecord> destination) {
        this.flowFiles.drainTo(destination);
    }
}
