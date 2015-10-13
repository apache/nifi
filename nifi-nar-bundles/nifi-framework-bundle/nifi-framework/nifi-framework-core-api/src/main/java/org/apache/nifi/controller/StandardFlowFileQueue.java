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
package org.apache.nifi.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.concurrency.TimedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FlowFileQueue is used to queue FlowFile objects that are awaiting further
 * processing. Must be thread safe.
 *
 */
public final class StandardFlowFileQueue implements FlowFileQueue {

    public static final int MAX_EXPIRED_RECORDS_PER_ITERATION = 100000;
    public static final int SWAP_RECORD_POLL_SIZE = 10000;

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowFileQueue.class);

    private PriorityQueue<FlowFileRecord> activeQueue = null;
    private long activeQueueContentSize = 0L;
    private ArrayList<FlowFileRecord> swapQueue = null;

    private int swappedRecordCount = 0;
    private long swappedContentSize = 0L;
    private String maximumQueueDataSize;
    private long maximumQueueByteCount;
    private boolean swapMode = false;
    private long maximumQueueObjectCount;

    private final EventReporter eventReporter;
    private final AtomicLong flowFileExpirationMillis;
    private final Connection connection;
    private final AtomicReference<String> flowFileExpirationPeriod;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final List<FlowFilePrioritizer> priorities;
    private final int swapThreshold;
    private final FlowFileSwapManager swapManager;
    private final List<String> swapLocations = new ArrayList<>();
    private final TimedLock readLock;
    private final TimedLock writeLock;
    private final String identifier;
    private final FlowFileRepository flowFileRepository;
    private final ProvenanceEventRepository provRepository;
    private final ResourceClaimManager resourceClaimManager;

    private final AtomicBoolean queueFullRef = new AtomicBoolean(false);
    private final AtomicInteger activeQueueSizeRef = new AtomicInteger(0);
    private final AtomicReference<QueueSize> unacknowledgedSizeRef = new AtomicReference<>(new QueueSize(0, 0L));

    // SCHEDULER CANNOT BE NOTIFIED OF EVENTS WITH THE WRITE LOCK HELD! DOING SO WILL RESULT IN A DEADLOCK!
    private final ProcessScheduler scheduler;

    public StandardFlowFileQueue(final String identifier, final Connection connection, final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo,
        final ResourceClaimManager resourceClaimManager, final ProcessScheduler scheduler, final FlowFileSwapManager swapManager, final EventReporter eventReporter, final int swapThreshold) {
        activeQueue = new PriorityQueue<>(20, new Prioritizer(new ArrayList<FlowFilePrioritizer>()));
        priorities = new ArrayList<>();
        maximumQueueObjectCount = 0L;
        maximumQueueDataSize = "0 MB";
        maximumQueueByteCount = 0L;
        flowFileExpirationMillis = new AtomicLong(0);
        flowFileExpirationPeriod = new AtomicReference<>("0 mins");
        swapQueue = new ArrayList<>();
        this.eventReporter = eventReporter;
        this.swapManager = swapManager;
        this.flowFileRepository = flowFileRepo;
        this.provRepository = provRepo;
        this.resourceClaimManager = resourceClaimManager;

        this.identifier = identifier;
        this.swapThreshold = swapThreshold;
        this.scheduler = scheduler;
        this.connection = connection;

        readLock = new TimedLock(this.lock.readLock(), identifier + " Read Lock", 100);
        writeLock = new TimedLock(this.lock.writeLock(), identifier + " Write Lock", 100);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public List<FlowFilePrioritizer> getPriorities() {
        return Collections.unmodifiableList(priorities);
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        writeLock.lock();
        try {
            final PriorityQueue<FlowFileRecord> newQueue = new PriorityQueue<>(Math.max(20, activeQueue.size()), new Prioritizer(newPriorities));
            newQueue.addAll(activeQueue);
            activeQueue = newQueue;
            priorities.clear();
            priorities.addAll(newPriorities);
        } finally {
            writeLock.unlock("setPriorities");
        }
    }

    @Override
    public void setBackPressureObjectThreshold(final long maxQueueSize) {
        writeLock.lock();
        try {
            maximumQueueObjectCount = maxQueueSize;
            this.queueFullRef.set(determineIfFull());
        } finally {
            writeLock.unlock("setBackPressureObjectThreshold");
        }
    }

    @Override
    public long getBackPressureObjectThreshold() {
        readLock.lock();
        try {
            return maximumQueueObjectCount;
        } finally {
            readLock.unlock("getBackPressureObjectThreshold");
        }
    }

    @Override
    public void setBackPressureDataSizeThreshold(final String maxDataSize) {
        writeLock.lock();
        try {
            maximumQueueByteCount = DataUnit.parseDataSize(maxDataSize, DataUnit.B).longValue();
            maximumQueueDataSize = maxDataSize;
            this.queueFullRef.set(determineIfFull());
        } finally {
            writeLock.unlock("setBackPressureDataSizeThreshold");
        }
    }

    @Override
    public String getBackPressureDataSizeThreshold() {
        readLock.lock();
        try {
            return maximumQueueDataSize;
        } finally {
            readLock.unlock("getBackPressureDataSizeThreshold");
        }
    }

    @Override
    public QueueSize size() {
        readLock.lock();
        try {
            return getQueueSize();
        } finally {
            readLock.unlock("getSize");
        }
    }

    /**
     * MUST be called with lock held
     *
     * @return size of queue
     */
    private QueueSize getQueueSize() {
        final QueueSize unacknowledged = unacknowledgedSizeRef.get();

        return new QueueSize(activeQueue.size() + swappedRecordCount + unacknowledged.getObjectCount(),
            activeQueueContentSize + swappedContentSize + unacknowledged.getByteCount());
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return activeQueue.isEmpty() && swappedRecordCount == 0 && unacknowledgedSizeRef.get().getObjectCount() == 0;
        } finally {
            readLock.unlock("isEmpty");
        }
    }

    @Override
    public boolean isActiveQueueEmpty() {
        final int activeQueueSize = activeQueueSizeRef.get();
        return activeQueueSize == 0;
    }

    public QueueSize getActiveQueueSize() {
        readLock.lock();
        try {
            return new QueueSize(activeQueue.size(), activeQueueContentSize);
        } finally {
            readLock.unlock("getActiveQueueSize");
        }
    }

    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        if (queueFullRef.get()) {
            writeLock.lock();
            try {
                updateUnacknowledgedSize(-1, -flowFile.getSize());
                queueFullRef.set(determineIfFull());
            } finally {
                writeLock.unlock("acknowledge(FlowFileRecord)");
            }
        } else {
            updateUnacknowledgedSize(-1, -flowFile.getSize());
        }

        if (connection.getSource().getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
            // queue was full but no longer is. Notify that the source may now be available to run,
            // because of back pressure caused by this queue.
            scheduler.registerEvent(connection.getSource());
        }
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        long totalSize = 0L;
        for (final FlowFileRecord flowFile : flowFiles) {
            totalSize += flowFile.getSize();
        }

        if (queueFullRef.get()) {
            writeLock.lock();
            try {
                updateUnacknowledgedSize(-flowFiles.size(), -totalSize);
                queueFullRef.set(determineIfFull());
            } finally {
                writeLock.unlock("acknowledge(FlowFileRecord)");
            }
        } else {
            updateUnacknowledgedSize(-flowFiles.size(), -totalSize);
        }

        if (connection.getSource().getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
            // it's possible that queue was full but no longer is. Notify that the source may now be available to run,
            // because of back pressure caused by this queue.
            scheduler.registerEvent(connection.getSource());
        }
    }

    @Override
    public boolean isFull() {
        return queueFullRef.get();
    }

    /**
     * MUST be called with either the read or write lock held
     *
     * @return true if full
     */
    private boolean determineIfFull() {
        final long maxSize = maximumQueueObjectCount;
        final long maxBytes = maximumQueueByteCount;
        if (maxSize <= 0 && maxBytes <= 0) {
            return false;
        }

        final QueueSize queueSize = getQueueSize();
        if (maxSize > 0 && queueSize.getObjectCount() >= maxSize) {
            return true;
        }

        if (maxBytes > 0 && queueSize.getByteCount() >= maxBytes) {
            return true;
        }

        return false;
    }

    @Override
    public void put(final FlowFileRecord file) {
        writeLock.lock();
        try {
            if (swapMode || activeQueue.size() >= swapThreshold) {
                swapQueue.add(file);
                swappedContentSize += file.getSize();
                swappedRecordCount++;
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                activeQueueContentSize += file.getSize();
                activeQueue.add(file);
            }

            queueFullRef.set(determineIfFull());
        } finally {
            activeQueueSizeRef.set(activeQueue.size());
            writeLock.unlock("put(FlowFileRecord)");
        }

        if (connection.getDestination().getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
            scheduler.registerEvent(connection.getDestination());
        }
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> files) {
        final int numFiles = files.size();
        long bytes = 0L;
        for (final FlowFile flowFile : files) {
            bytes += flowFile.getSize();
        }

        writeLock.lock();
        try {
            if (swapMode || activeQueue.size() >= swapThreshold - numFiles) {
                swapQueue.addAll(files);
                swappedContentSize += bytes;
                swappedRecordCount += numFiles;
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                activeQueueContentSize += bytes;
                activeQueue.addAll(files);
            }

            queueFullRef.set(determineIfFull());
        } finally {
            activeQueueSizeRef.set(activeQueue.size());
            writeLock.unlock("putAll");
        }

        if (connection.getDestination().getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
            scheduler.registerEvent(connection.getDestination());
        }
    }


    private boolean isLaterThan(final Long maxAge) {
        if (maxAge == null) {
            return false;
        }
        return maxAge < System.currentTimeMillis();
    }

    private Long getExpirationDate(final FlowFile flowFile, final long expirationMillis) {
        if (flowFile == null) {
            return null;
        }
        if (expirationMillis <= 0) {
            return null;
        } else {
            final long entryDate = flowFile.getEntryDate();
            final long expirationDate = entryDate + expirationMillis;
            return expirationDate;
        }
    }

    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        FlowFileRecord flowFile = null;

        // First check if we have any records Pre-Fetched.
        final long expirationMillis = flowFileExpirationMillis.get();
        writeLock.lock();
        try {
            flowFile = doPoll(expiredRecords, expirationMillis);
            return flowFile;
        } finally {
            activeQueueSizeRef.set(activeQueue.size());
            writeLock.unlock("poll(Set)");

            if (flowFile != null) {
                updateUnacknowledgedSize(1, flowFile.getSize());
            }
        }
    }

    private FlowFileRecord doPoll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis) {
        FlowFileRecord flowFile;
        boolean isExpired;

        migrateSwapToActive();
        final boolean queueFullAtStart = queueFullRef.get();

        do {
            flowFile = this.activeQueue.poll();

            isExpired = isLaterThan(getExpirationDate(flowFile, expirationMillis));
            if (isExpired) {
                expiredRecords.add(flowFile);
                if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                    activeQueueContentSize -= flowFile.getSize();
                    break;
                }
            } else if (flowFile != null && flowFile.isPenalized()) {
                this.activeQueue.add(flowFile);
                flowFile = null;
                break;
            }

            if (flowFile != null) {
                activeQueueContentSize -= flowFile.getSize();
            }
        } while (isExpired);

        // if at least 1 FlowFile was expired & the queue was full before we started, then
        // we need to determine whether or not the queue is full again. If no FlowFile was expired,
        // then the queue will still be full until the appropriate #acknowledge method is called.
        if (queueFullAtStart && !expiredRecords.isEmpty()) {
            queueFullRef.set(determineIfFull());
        }

        return isExpired ? null : flowFile;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> records = new ArrayList<>(Math.min(1024, maxResults));

        // First check if we have any records Pre-Fetched.
        writeLock.lock();
        try {
            doPoll(records, maxResults, expiredRecords);
        } finally {
            activeQueueSizeRef.set(activeQueue.size());
            writeLock.unlock("poll(int, Set)");
        }
        return records;
    }

    private void doPoll(final List<FlowFileRecord> records, int maxResults, final Set<FlowFileRecord> expiredRecords) {
        migrateSwapToActive();

        final boolean queueFullAtStart = queueFullRef.get();

        final long bytesDrained = drainQueue(activeQueue, records, maxResults, expiredRecords);

        long expiredBytes = 0L;
        for (final FlowFileRecord record : expiredRecords) {
            expiredBytes += record.getSize();
        }

        activeQueueContentSize -= bytesDrained;
        updateUnacknowledgedSize(records.size(), bytesDrained - expiredBytes);

        // if at least 1 FlowFile was expired & the queue was full before we started, then
        // we need to determine whether or not the queue is full again. If no FlowFile was expired,
        // then the queue will still be full until the appropriate #acknowledge method is called.
        if (queueFullAtStart && !expiredRecords.isEmpty()) {
            queueFullRef.set(determineIfFull());
        }
    }

    /**
     * If there are FlowFiles waiting on the swap queue, move them to the active
     * queue until we meet our threshold. This prevents us from having to swap
     * them to disk & then back out.
     *
     * This method MUST be called with the writeLock held.
     */
    private void migrateSwapToActive() {
        // Migrate as many FlowFiles as we can from the Swap Queue to the Active Queue, so that we don't
        // have to swap them out & then swap them back in.
        // If we don't do this, we could get into a situation where we have potentially thousands of FlowFiles
        // sitting on the Swap Queue but not getting processed because there aren't enough to be swapped out.
        // In particular, this can happen if the queue is typically filled with surges.
        // For example, if the queue has 25,000 FlowFiles come in, it may process 20,000 of them and leave
        // 5,000 sitting on the Swap Queue. If it then takes an hour for an additional 5,000 FlowFiles to come in,
        // those FlowFiles sitting on the Swap Queue will sit there for an hour, waiting to be swapped out and
        // swapped back in again.
        // Calling this method when records are polled prevents this condition by migrating FlowFiles from the
        // Swap Queue to the Active Queue. However, we don't do this if there are FlowFiles already swapped out
        // to disk, because we want them to be swapped back in in the same order that they were swapped out.

        if (activeQueue.size() > swapThreshold - SWAP_RECORD_POLL_SIZE) {
            return;
        }

        // If there are swap files waiting to be swapped in, swap those in first. We do this in order to ensure that those that
        // were swapped out first are then swapped back in first. If we instead just immediately migrated the FlowFiles from the
        // swap queue to the active queue, and we never run out of FlowFiles in the active queue (because destination cannot
        // keep up with queue), we will end up always processing the new FlowFiles first instead of the FlowFiles that arrived
        // first.
        if (!swapLocations.isEmpty()) {
            final String swapLocation = swapLocations.remove(0);
            try {
                final List<FlowFileRecord> swappedIn = swapManager.swapIn(swapLocation, this);
                swappedRecordCount -= swappedIn.size();
                long swapSize = 0L;
                for (final FlowFileRecord flowFile : swappedIn) {
                    swapSize += flowFile.getSize();
                }
                swappedContentSize -= swapSize;
                activeQueueContentSize += swapSize;
                activeQueueSizeRef.set(activeQueue.size());
                activeQueue.addAll(swappedIn);
                return;
            } catch (final FileNotFoundException fnfe) {
                logger.error("Failed to swap in FlowFiles from Swap File {} because the Swap File can no longer be found", swapLocation);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Swap File", "Failed to swap in FlowFiles from Swap File " + swapLocation + " because the Swap File can no longer be found");
                }
                return;
            } catch (final IOException ioe) {
                logger.error("Failed to swap in FlowFiles from Swap File {}; Swap File appears to be corrupt!", swapLocation);
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Swap File", "Failed to swap in FlowFiles from Swap File " +
                        swapLocation + "; Swap File appears to be corrupt! Some FlowFiles in the queue may not be accessible. See logs for more information.");
                }
                return;
            }
        }

        // this is the most common condition (nothing is swapped out), so do the check first and avoid the expense
        // of other checks for 99.999% of the cases.
        if (swappedRecordCount == 0 && swapQueue.isEmpty()) {
            return;
        }

        if (swappedRecordCount > swapQueue.size()) {
            // we already have FlowFiles swapped out, so we won't migrate the queue; we will wait for
            // an external process to swap FlowFiles back in.
            return;
        }

        final Iterator<FlowFileRecord> swapItr = swapQueue.iterator();
        while (activeQueue.size() < swapThreshold && swapItr.hasNext()) {
            final FlowFileRecord toMigrate = swapItr.next();
            activeQueue.add(toMigrate);
            activeQueueContentSize += toMigrate.getSize();
            swappedContentSize -= toMigrate.getSize();
            swappedRecordCount--;

            swapItr.remove();
        }

        if (swappedRecordCount == 0) {
            swapMode = false;
        }
    }

    /**
     * This method MUST be called with the write lock held
     */
    private void writeSwapFilesIfNecessary() {
        if (swapQueue.size() < SWAP_RECORD_POLL_SIZE) {
            return;
        }

        final int numSwapFiles = swapQueue.size() / SWAP_RECORD_POLL_SIZE;

        // Create a new Priority queue with the prioritizers that are set, but reverse the
        // prioritizers because we want to pull the lowest-priority FlowFiles to swap out
        final PriorityQueue<FlowFileRecord> tempQueue = new PriorityQueue<>(activeQueue.size() + swapQueue.size(), Collections.reverseOrder(new Prioritizer(priorities)));
        tempQueue.addAll(activeQueue);
        tempQueue.addAll(swapQueue);

        final List<String> swapLocations = new ArrayList<>(numSwapFiles);
        for (int i = 0; i < numSwapFiles; i++) {
            // Create a new swap file for the next SWAP_RECORD_POLL_SIZE records
            final List<FlowFileRecord> toSwap = new ArrayList<>(SWAP_RECORD_POLL_SIZE);
            for (int j = 0; j < SWAP_RECORD_POLL_SIZE; j++) {
                toSwap.add(tempQueue.poll());
            }

            try {
                Collections.reverse(toSwap); // currently ordered in reverse priority order based on the ordering of the temp queue.
                final String swapLocation = swapManager.swapOut(toSwap, this);
                swapLocations.add(swapLocation);
            } catch (final IOException ioe) {
                tempQueue.addAll(toSwap); // if we failed, we must add the FlowFiles back to the queue.
                logger.error("FlowFile Queue with identifier {} has {} FlowFiles queued up. Attempted to spill FlowFile information over to disk in order to avoid exhausting "
                    + "the Java heap space but failed to write information to disk due to {}", getIdentifier(), getQueueSize().getObjectCount(), ioe.toString());
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Failed to Overflow to Disk", "Flowfile Queue with identifier " + getIdentifier() + " has " + getQueueSize().getObjectCount() +
                        " queued up. Attempted to spill FlowFile information over to disk in order to avoid exhausting the Java heap space but failed to write information to disk. "
                        + "See logs for more information.");
                }

                break;
            }
        }

        // Pull any records off of the temp queue that won't fit back on the active queue, and add those to the
        // swap queue. Then add the records back to the active queue.
        swapQueue.clear();
        while (tempQueue.size() > swapThreshold) {
            final FlowFileRecord record = tempQueue.poll();
            swapQueue.add(record);
        }

        Collections.reverse(swapQueue); // currently ordered in reverse priority order based on the ordering of the temp queue

        // replace the contents of the active queue, since we've merged it with the swap queue.
        activeQueue.clear();
        FlowFileRecord toRequeue;
        while ((toRequeue = tempQueue.poll()) != null) {
            activeQueue.offer(toRequeue);
        }
        this.swapLocations.addAll(swapLocations);
    }


    @Override
    public long drainQueue(final Queue<FlowFileRecord> sourceQueue, final List<FlowFileRecord> destination, int maxResults, final Set<FlowFileRecord> expiredRecords) {
        long drainedSize = 0L;
        FlowFileRecord pulled = null;

        final long expirationMillis = this.flowFileExpirationMillis.get();
        while (destination.size() < maxResults && (pulled = sourceQueue.poll()) != null) {
            if (isLaterThan(getExpirationDate(pulled, expirationMillis))) {
                expiredRecords.add(pulled);
                if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                    break;
                }
            } else {
                if (pulled.isPenalized()) {
                    sourceQueue.add(pulled);
                    break;
                }
                destination.add(pulled);
            }
            drainedSize += pulled.getSize();
        }
        return drainedSize;
    }

    @Override
    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords) {
        writeLock.lock();
        try {
            migrateSwapToActive();

            final long expirationMillis = this.flowFileExpirationMillis.get();
            final boolean queueFullAtStart = queueFullRef.get();

            final List<FlowFileRecord> selectedFlowFiles = new ArrayList<>();
            final List<FlowFileRecord> unselected = new ArrayList<>();

            while (true) {
                FlowFileRecord flowFile = this.activeQueue.poll();
                if (flowFile == null) {
                    break;
                }

                final boolean isExpired = isLaterThan(getExpirationDate(flowFile, expirationMillis));
                if (isExpired) {
                    expiredRecords.add(flowFile);
                    activeQueueContentSize -= flowFile.getSize();

                    if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                        break;
                    } else {
                        continue;
                    }
                } else if (flowFile.isPenalized()) {
                    this.activeQueue.add(flowFile);
                    flowFile = null;
                    break; // just stop searching because the rest are all penalized.
                }

                final FlowFileFilterResult result = filter.filter(flowFile);
                if (result.isAccept()) {
                    activeQueueContentSize -= flowFile.getSize();

                    updateUnacknowledgedSize(1, flowFile.getSize());
                    selectedFlowFiles.add(flowFile);
                } else {
                    unselected.add(flowFile);
                }

                if (!result.isContinue()) {
                    break;
                }
            }

            this.activeQueue.addAll(unselected);

            // if at least 1 FlowFile was expired & the queue was full before we started, then
            // we need to determine whether or not the queue is full again. If no FlowFile was expired,
            // then the queue will still be full until the appropriate #acknowledge method is called.
            if (queueFullAtStart && !expiredRecords.isEmpty()) {
                queueFullRef.set(determineIfFull());
            }

            return selectedFlowFiles;
        } finally {
            activeQueueSizeRef.set(activeQueue.size());
            writeLock.unlock("poll(Filter, Set)");
        }
    }



    private static final class Prioritizer implements Comparator<FlowFileRecord>, Serializable {

        private static final long serialVersionUID = 1L;
        private final transient List<FlowFilePrioritizer> prioritizers = new ArrayList<>();

        private Prioritizer(final List<FlowFilePrioritizer> priorities) {
            if (null != priorities) {
                prioritizers.addAll(priorities);
            }
        }

        @Override
        public int compare(final FlowFileRecord f1, final FlowFileRecord f2) {
            int returnVal = 0;
            final boolean f1Penalized = f1.isPenalized();
            final boolean f2Penalized = f2.isPenalized();

            if (f1Penalized && !f2Penalized) {
                return 1;
            } else if (!f1Penalized && f2Penalized) {
                return -1;
            }

            if (f1Penalized && f2Penalized) {
                if (f1.getPenaltyExpirationMillis() < f2.getPenaltyExpirationMillis()) {
                    return -1;
                } else if (f1.getPenaltyExpirationMillis() > f2.getPenaltyExpirationMillis()) {
                    return 1;
                }
            }

            if (!prioritizers.isEmpty()) {
                for (final FlowFilePrioritizer prioritizer : prioritizers) {
                    returnVal = prioritizer.compare(f1, f2);
                    if (returnVal != 0) {
                        return returnVal;
                    }
                }
            }

            final ContentClaim claim1 = f1.getContentClaim();
            final ContentClaim claim2 = f2.getContentClaim();

            // put the one without a claim first
            if (claim1 == null && claim2 != null) {
                return -1;
            } else if (claim1 != null && claim2 == null) {
                return 1;
            } else if (claim1 != null && claim2 != null) {
                final int claimComparison = claim1.compareTo(claim2);
                if (claimComparison != 0) {
                    return claimComparison;
                }

                final int claimOffsetComparison = Long.compare(f1.getContentClaimOffset(), f2.getContentClaimOffset());
                if (claimOffsetComparison != 0) {
                    return claimOffsetComparison;
                }
            }

            return Long.compare(f1.getId(), f2.getId());
        }
    }

    @Override
    public String getFlowFileExpiration() {
        return flowFileExpirationPeriod.get();
    }

    @Override
    public int getFlowFileExpiration(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(flowFileExpirationMillis.get(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setFlowFileExpiration(final String flowExpirationPeriod) {
        final long millis = FormatUtils.getTimeDuration(flowExpirationPeriod, TimeUnit.MILLISECONDS);
        if (millis < 0) {
            throw new IllegalArgumentException("FlowFile Expiration Period must be positive");
        }
        this.flowFileExpirationPeriod.set(flowExpirationPeriod);
        this.flowFileExpirationMillis.set(millis);
    }


    @Override
    public void purgeSwapFiles() {
        swapManager.purge();
    }

    @Override
    public Long recoverSwappedFlowFiles() {
        int swapFlowFileCount = 0;
        long swapByteCount = 0L;
        Long maxId = null;

        writeLock.lock();
        try {
            final List<String> swapLocations;
            try {
                swapLocations = swapManager.recoverSwapLocations(this);
            } catch (final IOException ioe) {
                logger.error("Failed to determine whether or not any Swap Files exist for FlowFile Queue {}", getIdentifier());
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to determine whether or not any Swap Files exist for FlowFile Queue " +
                        getIdentifier() + "; see logs for more detials");
                }
                return null;
            }

            for (final String swapLocation : swapLocations) {
                try {
                    final QueueSize queueSize = swapManager.getSwapSize(swapLocation);
                    final Long maxSwapRecordId = swapManager.getMaxRecordId(swapLocation);
                    if (maxSwapRecordId != null) {
                        if (maxId == null || maxSwapRecordId > maxId) {
                            maxId = maxSwapRecordId;
                        }
                    }

                    swapFlowFileCount += queueSize.getObjectCount();
                    swapByteCount += queueSize.getByteCount();
                } catch (final IOException ioe) {
                    logger.error("Failed to recover FlowFiles from Swap File {}; the file appears to be corrupt", swapLocation, ioe.toString());
                    logger.error("", ioe);
                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to recover FlowFiles from Swap File " + swapLocation +
                            "; the file appears to be corrupt. See logs for more details");
                    }
                }
            }

            this.swappedRecordCount = swapFlowFileCount;
            this.swappedContentSize = swapByteCount;
            this.swapLocations.addAll(swapLocations);
        } finally {
            writeLock.unlock("Recover Swap Files");
        }

        return maxId;
    }


    @Override
    public String toString() {
        return "FlowFileQueue[id=" + identifier + "]";
    }

    private final ConcurrentMap<String, DropFlowFileRequest> dropRequestMap = new ConcurrentHashMap<>();

    @Override
    public DropFlowFileStatus dropFlowFiles(final String requestIdentifier, final String requestor) {
        // purge any old requests from the map just to keep it clean. But if there are very requests, which is usually the case, then don't bother
        if (dropRequestMap.size() > 10) {
            final List<String> toDrop = new ArrayList<>();
            for (final Map.Entry<String, DropFlowFileRequest> entry : dropRequestMap.entrySet()) {
                final DropFlowFileRequest request = entry.getValue();
                final boolean completed = request.getState() == DropFlowFileState.COMPLETE || request.getState() == DropFlowFileState.FAILURE;

                if (completed && System.currentTimeMillis() - request.getLastUpdated() > TimeUnit.MINUTES.toMillis(5L)) {
                    toDrop.add(entry.getKey());
                }
            }

            for (final String requestId : toDrop) {
                dropRequestMap.remove(requestId);
            }
        }

        final DropFlowFileRequest dropRequest = new DropFlowFileRequest(requestIdentifier);
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                writeLock.lock();
                try {
                    dropRequest.setState(DropFlowFileState.DROPPING_ACTIVE_FLOWFILES);

                    try {
                        final List<FlowFileRecord> activeQueueRecords = new ArrayList<>(activeQueue);
                        QueueSize droppedSize = drop(activeQueueRecords, requestor);
                        activeQueue.clear();
                        dropRequest.setCurrentSize(getQueueSize());
                        dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));

                        droppedSize = drop(swapQueue, requestor);
                        swapQueue.clear();
                        dropRequest.setCurrentSize(getQueueSize());
                        dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));

                        final Iterator<String> swapLocationItr = swapLocations.iterator();
                        while (swapLocationItr.hasNext()) {
                            final String swapLocation = swapLocationItr.next();
                            final List<FlowFileRecord> swappedIn = swapManager.swapIn(swapLocation, StandardFlowFileQueue.this);
                            try {
                                droppedSize = drop(swappedIn, requestor);
                                dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));
                            } catch (final Exception e) {
                                activeQueue.addAll(swappedIn); // ensure that we don't lose the FlowFiles from our queue.
                                throw e;
                            }

                            dropRequest.setCurrentSize(getQueueSize());
                            swapLocationItr.remove();
                        }

                        dropRequest.setState(DropFlowFileState.COMPLETE);
                    } catch (final Exception e) {
                        // TODO: Handle adequately
                        dropRequest.setState(DropFlowFileState.FAILURE);
                    }
                } finally {
                    writeLock.unlock("Drop FlowFiles");
                }
            }
        }, "Drop FlowFiles for Connection " + getIdentifier());
        t.setDaemon(true);
        t.start();

        dropRequest.setExecutionThread(t);
        dropRequestMap.put(requestIdentifier, dropRequest);

        return dropRequest;
    }

    private QueueSize drop(final List<FlowFileRecord> flowFiles, final String requestor) throws IOException {
        // Create a Provenance Event and a FlowFile Repository record for each FlowFile
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(flowFiles.size());
        final List<RepositoryRecord> flowFileRepoRecords = new ArrayList<>(flowFiles.size());
        for (final FlowFileRecord flowFile : flowFiles) {
            provenanceEvents.add(createDropEvent(flowFile, requestor));
            flowFileRepoRecords.add(createDeleteRepositoryRecord(flowFile));
        }

        long dropContentSize = 0L;
        for (final FlowFileRecord flowFile : flowFiles) {
            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim == null) {
                continue;
            }

            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            if (resourceClaim == null) {
                continue;
            }

            resourceClaimManager.decrementClaimantCount(resourceClaim);
            dropContentSize += flowFile.getSize();
        }

        provRepository.registerEvents(provenanceEvents);
        flowFileRepository.updateRepository(flowFileRepoRecords);
        return new QueueSize(flowFiles.size(), dropContentSize);
    }

    private ProvenanceEventRecord createDropEvent(final FlowFileRecord flowFile, final String requestor) {
        final ProvenanceEventBuilder builder = provRepository.eventBuilder();
        builder.fromFlowFile(flowFile);
        builder.setEventType(ProvenanceEventType.DROP);
        builder.setLineageStartDate(flowFile.getLineageStartDate());
        builder.setComponentId(getIdentifier());
        builder.setComponentType("Connection");
        builder.setDetails("FlowFile manually dropped; request made by " + requestor);
        return builder.build();
    }

    private RepositoryRecord createDeleteRepositoryRecord(final FlowFileRecord flowFile) {
        return new RepositoryRecord() {
            @Override
            public FlowFileQueue getDestination() {
                return null;
            }

            @Override
            public FlowFileQueue getOriginalQueue() {
                return StandardFlowFileQueue.this;
            }

            @Override
            public RepositoryRecordType getType() {
                return RepositoryRecordType.DELETE;
            }

            @Override
            public ContentClaim getCurrentClaim() {
                return flowFile.getContentClaim();
            }

            @Override
            public ContentClaim getOriginalClaim() {
                return flowFile.getContentClaim();
            }

            @Override
            public long getCurrentClaimOffset() {
                return flowFile.getContentClaimOffset();
            }

            @Override
            public FlowFileRecord getCurrent() {
                return flowFile;
            }

            @Override
            public boolean isAttributesChanged() {
                return false;
            }

            @Override
            public boolean isMarkedForAbort() {
                return false;
            }

            @Override
            public String getSwapLocation() {
                return null;
            }
        };
    }


    @Override
    public boolean cancelDropFlowFileRequest(final String requestIdentifier) {
        final DropFlowFileRequest request = dropRequestMap.remove(requestIdentifier);
        if (request == null) {
            return false;
        }

        final boolean successful = request.cancel();
        return successful;
    }

    @Override
    public DropFlowFileStatus getDropFlowFileStatus(final String requestIdentifier) {
        return dropRequestMap.get(requestIdentifier);
    }

    /**
     * Lock the queue so that other threads are unable to interact with the
     * queue
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

    @Override
    public QueueSize getUnacknowledgedQueueSize() {
        return unacknowledgedSizeRef.get();
    }

    private void updateUnacknowledgedSize(final int addToCount, final long addToSize) {
        boolean updated = false;

        do {
            final QueueSize queueSize = unacknowledgedSizeRef.get();
            final QueueSize newSize = new QueueSize(queueSize.getObjectCount() + addToCount, queueSize.getByteCount() + addToSize);
            updated = unacknowledgedSizeRef.compareAndSet(queueSize, newSize);
        } while (!updated);
    }
}
