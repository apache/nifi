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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.processor.QueueSize;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.concurrency.TimedLock;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;

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

    // When we have very high contention on a FlowFile Queue, the writeLock quickly becomes the bottleneck. In order to avoid this,
    // we keep track of how often we are obtaining the write lock. If we exceed some threshold, we start performing a Pre-fetch so that
    // we can then poll many times without having to obtain the lock.
    // If lock obtained an average of more than PREFETCH_POLL_THRESHOLD times per second in order to poll from queue for last 5 seconds, do a pre-fetch.
    public static final int PREFETCH_POLL_THRESHOLD = 1000;
    public static final int PRIORITIZED_PREFETCH_SIZE = 10;
    public static final int UNPRIORITIZED_PREFETCH_SIZE = 1000;
    private volatile int prefetchSize = UNPRIORITIZED_PREFETCH_SIZE; // when we pre-fetch, how many should we pre-fetch?

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

    private final AtomicLong flowFileExpirationMillis;
    private final Connection connection;
    private final AtomicReference<String> flowFileExpirationPeriod;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final List<FlowFilePrioritizer> priorities;
    private final int swapThreshold;
    private final TimedLock readLock;
    private final TimedLock writeLock;
    private final String identifier;

    private final AtomicBoolean queueFullRef = new AtomicBoolean(false);
    private final AtomicInteger activeQueueSizeRef = new AtomicInteger(0);
    private final AtomicReference<QueueSize> unacknowledgedSizeRef = new AtomicReference<>(new QueueSize(0, 0L));

    // SCHEDULER CANNOT BE NOTIFIED OF EVENTS WITH THE WRITE LOCK HELD! DOING SO WILL RESULT IN A DEADLOCK!
    private final ProcessScheduler scheduler;

    public StandardFlowFileQueue(final String identifier, final Connection connection, final ProcessScheduler scheduler, final int swapThreshold) {
        activeQueue = new PriorityQueue<>(20, new Prioritizer(new ArrayList<FlowFilePrioritizer>()));
        priorities = new ArrayList<>();
        maximumQueueObjectCount = 0L;
        maximumQueueDataSize = "0 MB";
        maximumQueueByteCount = 0L;
        flowFileExpirationMillis = new AtomicLong(0);
        flowFileExpirationPeriod = new AtomicReference<>("0 mins");
        swapQueue = new ArrayList<>();

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
    public int getSwapThreshold() {
        return swapThreshold;
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

            if (newPriorities.isEmpty()) {
                prefetchSize = UNPRIORITIZED_PREFETCH_SIZE;
            } else {
                prefetchSize = PRIORITIZED_PREFETCH_SIZE;
            }
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
        final PreFetch preFetch = preFetchRef.get();

        final int preFetchCount;
        final long preFetchSize;
        if (preFetch == null) {
            preFetchCount = 0;
            preFetchSize = 0L;
        } else {
            final QueueSize preFetchQueueSize = preFetch.size();
            preFetchCount = preFetchQueueSize.getObjectCount();
            preFetchSize = preFetchQueueSize.getByteCount();
        }

        return new QueueSize(activeQueue.size() + swappedRecordCount + unacknowledged.getObjectCount() + preFetchCount,
            activeQueueContentSize + swappedContentSize + unacknowledged.getByteCount() + preFetchSize);
    }

    @Override
    public long contentSize() {
        readLock.lock();
        try {
            final PreFetch prefetch = preFetchRef.get();
            if (prefetch == null) {
                return activeQueueContentSize + swappedContentSize + unacknowledgedSizeRef.get().getObjectCount();
            } else {
                return activeQueueContentSize + swappedContentSize + unacknowledgedSizeRef.get().getObjectCount() + prefetch.size().getByteCount();
            }
        } finally {
            readLock.unlock("getContentSize");
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            final PreFetch prefetch = preFetchRef.get();
            if (prefetch == null) {
                return activeQueue.isEmpty() && swappedRecordCount == 0 && unacknowledgedSizeRef.get().getObjectCount() == 0;
            } else {
                return activeQueue.isEmpty() && swappedRecordCount == 0 && unacknowledgedSizeRef.get().getObjectCount() == 0 && prefetch.size().getObjectCount() == 0;
            }
        } finally {
            readLock.unlock("isEmpty");
        }
    }

    @Override
    public boolean isActiveQueueEmpty() {
        final int activeQueueSize = activeQueueSizeRef.get();
        if (activeQueueSize == 0) {
            final PreFetch preFetch = preFetchRef.get();
            if (preFetch == null) {
                return true;
            }

            final QueueSize queueSize = preFetch.size();
            return queueSize.getObjectCount() == 0;
        } else {
            return false;
        }
    }

    @Override
    public QueueSize getActiveQueueSize() {
        readLock.lock();
        try {
            final PreFetch preFetch = preFetchRef.get();
            if (preFetch == null) {
                return new QueueSize(activeQueue.size(), activeQueueContentSize);
            } else {
                final QueueSize preFetchSize = preFetch.size();
                return new QueueSize(activeQueue.size() + preFetchSize.getObjectCount(), activeQueueContentSize + preFetchSize.getByteCount());
            }
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

        if (maxBytes > 0 && (queueSize.getByteCount() >= maxBytes)) {
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

    @Override
    public List<FlowFileRecord> pollSwappableRecords() {
        writeLock.lock();
        try {
            if (swapQueue.size() < SWAP_RECORD_POLL_SIZE) {
                return null;
            }

            final List<FlowFileRecord> swapRecords = new ArrayList<>(Math.min(SWAP_RECORD_POLL_SIZE, swapQueue.size()));
            final Iterator<FlowFileRecord> itr = swapQueue.iterator();
            while (itr.hasNext() && swapRecords.size() < SWAP_RECORD_POLL_SIZE) {
                FlowFileRecord record = itr.next();
                swapRecords.add(record);
                itr.remove();
            }

            swapQueue.trimToSize();
            return swapRecords;
        } finally {
            writeLock.unlock("pollSwappableRecords");
        }
    }

    @Override
    public void putSwappedRecords(final Collection<FlowFileRecord> records) {
        writeLock.lock();
        try {
            try {
                for (final FlowFileRecord record : records) {
                    swappedContentSize -= record.getSize();
                    swappedRecordCount--;
                    activeQueueContentSize += record.getSize();
                    activeQueue.add(record);
                }

                if (swappedRecordCount > swapQueue.size()) {
                    // we have more swap files to be swapped in.
                    return;
                }

                // If a call to #pollSwappableRecords will not produce any, go ahead and roll those FlowFiles back into the mix
                if (swapQueue.size() < SWAP_RECORD_POLL_SIZE) {
                    for (final FlowFileRecord record : swapQueue) {
                        activeQueue.add(record);
                        activeQueueContentSize += record.getSize();
                    }
                    swapQueue.clear();
                    swappedContentSize = 0L;
                    swappedRecordCount = 0;
                    swapMode = false;
                }
            } finally {
                activeQueueSizeRef.set(activeQueue.size());
            }
        } finally {
            writeLock.unlock("putSwappedRecords");
            scheduler.registerEvent(connection.getDestination());
        }
    }

    @Override
    public void incrementSwapCount(final int numRecords, final long contentSize) {
        writeLock.lock();
        try {
            swappedContentSize += contentSize;
            swappedRecordCount += numRecords;
        } finally {
            writeLock.unlock("incrementSwapCount");
        }
    }

    @Override
    public int unswappedSize() {
        readLock.lock();
        try {
            return activeQueue.size() + unacknowledgedSizeRef.get().getObjectCount();
        } finally {
            readLock.unlock("unswappedSize");
        }
    }

    @Override
    public int getSwapRecordCount() {
        readLock.lock();
        try {
            return swappedRecordCount;
        } finally {
            readLock.unlock("getSwapRecordCount");
        }
    }

    @Override
    public int getSwapQueueSize() {
        readLock.lock();
        try {
            if (logger.isDebugEnabled()) {
                final long byteToMbDivisor = 1024L * 1024L;
                final QueueSize unacknowledged = unacknowledgedSizeRef.get();

                logger.debug("Total Queue Size: ActiveQueue={}/{} MB, Swap Queue={}/{} MB, Unacknowledged={}/{} MB",
                    activeQueue.size(), activeQueueContentSize / byteToMbDivisor,
                    swappedRecordCount, swappedContentSize / byteToMbDivisor,
                    unacknowledged.getObjectCount(), unacknowledged.getByteCount() / byteToMbDivisor);
            }

            return swapQueue.size();
        } finally {
            readLock.unlock("getSwapQueueSize");
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
        final PreFetch preFetch = preFetchRef.get();
        if (preFetch != null) {
            if (preFetch.isExpired()) {
                requeueExpiredPrefetch(preFetch);
            } else {
                while (true) {
                    final FlowFileRecord next = preFetch.nextRecord();
                    if (next == null) {
                        break;
                    }

                    if (isLaterThan(getExpirationDate(next, expirationMillis))) {
                        expiredRecords.add(next);
                        continue;
                    }

                    updateUnacknowledgedSize(1, next.getSize());
                    return next;
                }

                preFetchRef.compareAndSet(preFetch, null);
            }
        }

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
        boolean queueFullAtStart = queueFullRef.get();

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

        if (incrementPollCount()) {
            prefetch();
        }
        return isExpired ? null : flowFile;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> records = new ArrayList<>(Math.min(1024, maxResults));

        // First check if we have any records Pre-Fetched.
        final long expirationMillis = flowFileExpirationMillis.get();
        final PreFetch preFetch = preFetchRef.get();
        if (preFetch != null) {
            if (preFetch.isExpired()) {
                requeueExpiredPrefetch(preFetch);
            } else {
                long totalSize = 0L;
                for (int i = 0; i < maxResults; i++) {
                    final FlowFileRecord next = preFetch.nextRecord();
                    if (next == null) {
                        break;
                    }

                    if (isLaterThan(getExpirationDate(next, expirationMillis))) {
                        expiredRecords.add(next);
                        continue;
                    }

                    records.add(next);
                    totalSize += next.getSize();
                }

                // If anything was prefetched, use what we have.
                if (!records.isEmpty()) {
                    updateUnacknowledgedSize(records.size(), totalSize);
                    return records;
                }

                preFetchRef.compareAndSet(preFetch, null);
            }
        }

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

        if (incrementPollCount()) {
            prefetch();
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
            if (activeQueue.isEmpty()) {
                return Collections.emptyList();
            }

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
    public String toString() {
        return "FlowFileQueue[id=" + identifier + "]";
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
            QueueSize queueSize = unacknowledgedSizeRef.get();
            final QueueSize newSize = new QueueSize(queueSize.getObjectCount() + addToCount, queueSize.getByteCount() + addToSize);
            updated = unacknowledgedSizeRef.compareAndSet(queueSize, newSize);
        } while (!updated);
    }

    private void requeueExpiredPrefetch(final PreFetch prefetch) {
        if (prefetch == null) {
            return;
        }

        writeLock.lock();
        try {
            final long contentSizeRequeued = prefetch.requeue(activeQueue);
            this.activeQueueContentSize += contentSizeRequeued;
            this.preFetchRef.compareAndSet(prefetch, null);
        } finally {
            writeLock.unlock("requeueExpiredPrefetch");
        }
    }

    /**
     * MUST be called with write lock held.
     */
    private final AtomicReference<PreFetch> preFetchRef = new AtomicReference<>();

    private void prefetch() {
        if (activeQueue.isEmpty()) {
            return;
        }

        final int numToFetch = Math.min(prefetchSize, activeQueue.size());

        final PreFetch curPreFetch = preFetchRef.get();
        if (curPreFetch != null && curPreFetch.size().getObjectCount() > 0) {
            return;
        }

        final List<FlowFileRecord> buffer = new ArrayList<>(numToFetch);
        long contentSize = 0L;
        for (int i = 0; i < numToFetch; i++) {
            final FlowFileRecord record = activeQueue.poll();
            if (record == null || record.isPenalized()) {
                // not enough unpenalized records to pull. Put all records back and return
                activeQueue.addAll(buffer);
                if (record != null) {
                    activeQueue.add(record);
                }
                return;
            } else {
                buffer.add(record);
                contentSize += record.getSize();
            }
        }

        activeQueueContentSize -= contentSize;
        preFetchRef.set(new PreFetch(buffer));
    }

    private final TimedBuffer<TimestampedLong> pollCounts = new TimedBuffer<>(TimeUnit.SECONDS, 5, new LongEntityAccess());

    private boolean incrementPollCount() {
        pollCounts.add(new TimestampedLong(1L));
        final long totalCount = pollCounts.getAggregateValue(System.currentTimeMillis() - 5000L).getValue();
        return totalCount > PREFETCH_POLL_THRESHOLD * 5;
    }

    private static class PreFetch {

        private final List<FlowFileRecord> records;
        private final AtomicInteger pointer = new AtomicInteger(0);
        private final long expirationTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        private final AtomicLong contentSize = new AtomicLong(0L);

        public PreFetch(final List<FlowFileRecord> records) {
            this.records = records;

            long totalSize = 0L;
            for (final FlowFileRecord record : records) {
                totalSize += record.getSize();
            }
            contentSize.set(totalSize);
        }

        public FlowFileRecord nextRecord() {
            final int nextValue = pointer.getAndIncrement();
            if (nextValue >= records.size()) {
                return null;
            }

            final FlowFileRecord flowFile = records.get(nextValue);
            contentSize.addAndGet(-flowFile.getSize());
            return flowFile;
        }

        public QueueSize size() {
            final int pointerIndex = pointer.get();
            final int count = records.size() - pointerIndex;
            if (count < 0) {
                return new QueueSize(0, 0L);
            }

            final long bytes = contentSize.get();
            return new QueueSize(count, bytes);
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTime;
        }

        private long requeue(final Queue<FlowFileRecord> queue) {
            // get the current pointer and prevent any other thread from accessing the rest of the elements
            final int curPointer = pointer.getAndAdd(records.size());
            if (curPointer < records.size() - 1) {
                final List<FlowFileRecord> subList = records.subList(curPointer, records.size());
                long contentSize = 0L;
                for (final FlowFileRecord record : subList) {
                    contentSize += record.getSize();
                }

                queue.addAll(subList);

                return contentSize;
            }
            return 0L;
        }
    }
}
