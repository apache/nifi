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

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileSummary;
import org.apache.nifi.controller.queue.ListFlowFileRequest;
import org.apache.nifi.controller.queue.ListFlowFileState;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.IncompleteSwapFileException;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A FlowFileQueue is used to queue FlowFile objects that are awaiting further
 * processing. Must be thread safe.
 *
 */
public class StandardFlowFileQueue implements FlowFileQueue {

    public static final int MAX_EXPIRED_RECORDS_PER_ITERATION = 100000;
    public static final int SWAP_RECORD_POLL_SIZE = 10000;

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowFileQueue.class);

    private PriorityQueue<FlowFileRecord> activeQueue = null;

    // guarded by lock
    private ArrayList<FlowFileRecord> swapQueue = null;

    private final AtomicReference<FlowFileQueueSize> size = new AtomicReference<>(new FlowFileQueueSize(0, 0L, 0, 0L, 0, 0, 0L));

    private boolean swapMode = false;

    public static final int DEFAULT_BACKPRESSURE_COUNT = 10000;
    public static final String DEFAULT_BACKPRESSURE_SIZE = "1 GB";
    private final AtomicReference<MaxQueueSize> maxQueueSize = new AtomicReference<>(new MaxQueueSize(DEFAULT_BACKPRESSURE_SIZE,
            DataUnit.parseDataSize(DEFAULT_BACKPRESSURE_SIZE, DataUnit.B).longValue(), DEFAULT_BACKPRESSURE_COUNT));
    private final AtomicReference<TimePeriod> expirationPeriod = new AtomicReference<>(new TimePeriod("0 mins", 0L));

    private final EventReporter eventReporter;
    private final Connection connection;
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

    private final ConcurrentMap<String, DropFlowFileRequest> dropRequestMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ListFlowFileRequest> listRequestMap = new ConcurrentHashMap<>();

    // SCHEDULER CANNOT BE NOTIFIED OF EVENTS WITH THE WRITE LOCK HELD! DOING SO WILL RESULT IN A DEADLOCK!
    private final ProcessScheduler scheduler;

    public StandardFlowFileQueue(final String identifier, final Connection connection, final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo,
        final ResourceClaimManager resourceClaimManager, final ProcessScheduler scheduler, final FlowFileSwapManager swapManager, final EventReporter eventReporter, final int swapThreshold) {
        activeQueue = new PriorityQueue<>(20, new Prioritizer(new ArrayList<FlowFilePrioritizer>()));
        priorities = new ArrayList<>();
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
    public void setBackPressureObjectThreshold(final long threshold) {
        boolean updated = false;
        while (!updated) {
            MaxQueueSize maxSize = maxQueueSize.get();
            final MaxQueueSize updatedSize = new MaxQueueSize(maxSize.getMaxSize(), maxSize.getMaxBytes(), threshold);
            updated = maxQueueSize.compareAndSet(maxSize, updatedSize);
        }
    }

    @Override
    public long getBackPressureObjectThreshold() {
        return maxQueueSize.get().getMaxCount();
    }

    @Override
    public void setBackPressureDataSizeThreshold(final String maxDataSize) {
        final long maxBytes = DataUnit.parseDataSize(maxDataSize, DataUnit.B).longValue();

        boolean updated = false;
        while (!updated) {
            MaxQueueSize maxSize = maxQueueSize.get();
            final MaxQueueSize updatedSize = new MaxQueueSize(maxDataSize, maxBytes, maxSize.getMaxCount());
            updated = maxQueueSize.compareAndSet(maxSize, updatedSize);
        }
    }

    @Override
    public String getBackPressureDataSizeThreshold() {
        return maxQueueSize.get().getMaxSize();
    }

    @Override
    public QueueSize size() {
        return getQueueSize();
    }


    private QueueSize getQueueSize() {
        return size.get().toQueueSize();
    }

    @Override
    public boolean isEmpty() {
        return size.get().isEmpty();
    }

    @Override
    public boolean isActiveQueueEmpty() {
        final FlowFileQueueSize queueSize = size.get();
        return queueSize.activeQueueCount == 0 && queueSize.swappedCount == 0;
    }

    public QueueSize getActiveQueueSize() {
        return size.get().activeQueueSize();
    }

    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        incrementUnacknowledgedQueueSize(-1, -flowFile.getSize());

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

        incrementUnacknowledgedQueueSize(-flowFiles.size(), -totalSize);

        if (connection.getSource().getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
            // it's possible that queue was full but no longer is. Notify that the source may now be available to run,
            // because of back pressure caused by this queue.
            scheduler.registerEvent(connection.getSource());
        }
    }

    @Override
    public boolean isFull() {
        final MaxQueueSize maxSize = maxQueueSize.get();

        // Check if max size is set
        if (maxSize.getMaxBytes() <= 0 && maxSize.getMaxCount() <= 0) {
            return false;
        }

        final QueueSize queueSize = getQueueSize();
        if (maxSize.getMaxCount() > 0 && queueSize.getObjectCount() >= maxSize.getMaxCount()) {
            return true;
        }

        if (maxSize.getMaxBytes() > 0 && queueSize.getByteCount() >= maxSize.getMaxBytes()) {
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
                incrementSwapQueueSize(1, file.getSize(), 0);
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                incrementActiveQueueSize(1, file.getSize());
                activeQueue.add(file);
            }
        } finally {
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
                incrementSwapQueueSize(numFiles, bytes, 0);
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                incrementActiveQueueSize(numFiles, bytes);
                activeQueue.addAll(files);
            }
        } finally {
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
        final long expirationMillis = expirationPeriod.get().getMillis();
        writeLock.lock();
        try {
            flowFile = doPoll(expiredRecords, expirationMillis);
            return flowFile;
        } finally {
            writeLock.unlock("poll(Set)");

            if (flowFile != null) {
                incrementUnacknowledgedQueueSize(1, flowFile.getSize());
            }
        }
    }

    private FlowFileRecord doPoll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis) {
        FlowFileRecord flowFile;
        boolean isExpired;

        migrateSwapToActive();

        long expiredBytes = 0L;
        do {
            flowFile = this.activeQueue.poll();

            isExpired = isLaterThan(getExpirationDate(flowFile, expirationMillis));
            if (isExpired) {
                expiredRecords.add(flowFile);
                expiredBytes += flowFile.getSize();
                flowFile = null;

                if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                    break;
                }
            } else if (flowFile != null && flowFile.isPenalized()) {
                this.activeQueue.add(flowFile);
                flowFile = null;
                break;
            }

            if (flowFile != null) {
                incrementActiveQueueSize(-1, -flowFile.getSize());
            }
        }
        while (isExpired);

        if (!expiredRecords.isEmpty()) {
            incrementActiveQueueSize(-expiredRecords.size(), -expiredBytes);
        }

        return flowFile;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> records = new ArrayList<>(Math.min(1024, maxResults));

        // First check if we have any records Pre-Fetched.
        writeLock.lock();
        try {
            doPoll(records, maxResults, expiredRecords);
        } finally {
            writeLock.unlock("poll(int, Set)");
        }
        return records;
    }

    private void doPoll(final List<FlowFileRecord> records, int maxResults, final Set<FlowFileRecord> expiredRecords) {
        migrateSwapToActive();

        final long bytesDrained = drainQueue(activeQueue, records, maxResults, expiredRecords);

        long expiredBytes = 0L;
        for (final FlowFileRecord record : expiredRecords) {
            expiredBytes += record.getSize();
        }

        incrementActiveQueueSize(-(expiredRecords.size() + records.size()), -bytesDrained);
        incrementUnacknowledgedQueueSize(records.size(), bytesDrained - expiredBytes);
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

        final int activeQueueSize = activeQueue.size();
        if (activeQueueSize > 0 && activeQueueSize > swapThreshold - SWAP_RECORD_POLL_SIZE) {
            return;
        }

        // If there are swap files waiting to be swapped in, swap those in first. We do this in order to ensure that those that
        // were swapped out first are then swapped back in first. If we instead just immediately migrated the FlowFiles from the
        // swap queue to the active queue, and we never run out of FlowFiles in the active queue (because destination cannot
        // keep up with queue), we will end up always processing the new FlowFiles first instead of the FlowFiles that arrived
        // first.
        if (!swapLocations.isEmpty()) {
            final String swapLocation = swapLocations.get(0);
            boolean partialContents = false;
            SwapContents swapContents = null;
            try {
                swapContents = swapManager.swapIn(swapLocation, this);
                swapLocations.remove(0);
            } catch (final IncompleteSwapFileException isfe) {
                logger.error("Failed to swap in all FlowFiles from Swap File {}; Swap File ended prematurely. The records that were present will still be swapped in", swapLocation);
                logger.error("", isfe);
                swapContents = isfe.getPartialContents();
                partialContents = true;
                swapLocations.remove(0);
            } catch (final FileNotFoundException fnfe) {
                logger.error("Failed to swap in FlowFiles from Swap File {} because the Swap File can no longer be found", swapLocation);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Swap File", "Failed to swap in FlowFiles from Swap File " + swapLocation + " because the Swap File can no longer be found");
                }

                swapLocations.remove(0);
                return;
            } catch (final IOException ioe) {
                logger.error("Failed to swap in FlowFiles from Swap File {}; Swap File appears to be corrupt!", swapLocation);
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Swap File", "Failed to swap in FlowFiles from Swap File " +
                        swapLocation + "; Swap File appears to be corrupt! Some FlowFiles in the queue may not be accessible. See logs for more information.");
                }

                // We do not remove the Swap File from swapLocations because the IOException may be recoverable later. For instance, the file may be on a network
                // drive and we may have connectivity problems, etc.
                return;
            } catch (final Throwable t) {
                logger.error("Failed to swap in FlowFiles from Swap File {}", swapLocation, t);

                // We do not remove the Swap File from swapLocations because this is an unexpected failure that may be retry-able. For example, if there were
                // an OOME, etc. then we don't want to he queue to still reflect that the data is around but never swap it in. By leaving the Swap File
                // in swapLocations, we will continue to retry.
                throw t;
            }

            final QueueSize swapSize = swapContents.getSummary().getQueueSize();
            final long contentSize = swapSize.getByteCount();
            final int flowFileCount = swapSize.getObjectCount();
            incrementSwapQueueSize(-flowFileCount, -contentSize, -1);

            if (partialContents) {
                // if we have partial results, we need to calculate the content size of the flowfiles
                // actually swapped back in.
                long contentSizeSwappedIn = 0L;
                for (final FlowFileRecord swappedIn : swapContents.getFlowFiles()) {
                    contentSizeSwappedIn += swappedIn.getSize();
                }

                incrementActiveQueueSize(swapContents.getFlowFiles().size(), contentSizeSwappedIn);
            } else {
                // we swapped in the whole swap file. We can just use the info that we got from the summary.
                incrementActiveQueueSize(flowFileCount, contentSize);
            }

            activeQueue.addAll(swapContents.getFlowFiles());
            return;
        }

        // this is the most common condition (nothing is swapped out), so do the check first and avoid the expense
        // of other checks for 99.999% of the cases.
        if (size.get().swappedCount == 0 && swapQueue.isEmpty()) {
            return;
        }

        if (size.get().swappedCount > swapQueue.size()) {
            // we already have FlowFiles swapped out, so we won't migrate the queue; we will wait for
            // the files to be swapped back in first
            return;
        }

        int recordsMigrated = 0;
        long bytesMigrated = 0L;
        final Iterator<FlowFileRecord> swapItr = swapQueue.iterator();
        while (activeQueue.size() < swapThreshold && swapItr.hasNext()) {
            final FlowFileRecord toMigrate = swapItr.next();
            activeQueue.add(toMigrate);
            bytesMigrated += toMigrate.getSize();
            recordsMigrated++;
            swapItr.remove();
        }

        if (recordsMigrated > 0) {
            incrementActiveQueueSize(recordsMigrated, bytesMigrated);
            incrementSwapQueueSize(-recordsMigrated, -bytesMigrated, 0);
        }

        if (size.get().swappedCount == 0) {
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

        migrateSwapToActive();

        final int numSwapFiles = swapQueue.size() / SWAP_RECORD_POLL_SIZE;

        int originalSwapQueueCount = swapQueue.size();
        long originalSwapQueueBytes = 0L;
        for (final FlowFileRecord flowFile : swapQueue) {
            originalSwapQueueBytes += flowFile.getSize();
        }

        // Create a new Priority queue with the prioritizers that are set, but reverse the
        // prioritizers because we want to pull the lowest-priority FlowFiles to swap out
        final PriorityQueue<FlowFileRecord> tempQueue = new PriorityQueue<>(activeQueue.size() + swapQueue.size(), Collections.reverseOrder(new Prioritizer(priorities)));
        tempQueue.addAll(activeQueue);
        tempQueue.addAll(swapQueue);

        long bytesSwappedOut = 0L;
        int flowFilesSwappedOut = 0;
        final List<String> swapLocations = new ArrayList<>(numSwapFiles);
        for (int i = 0; i < numSwapFiles; i++) {
            // Create a new swap file for the next SWAP_RECORD_POLL_SIZE records
            final List<FlowFileRecord> toSwap = new ArrayList<>(SWAP_RECORD_POLL_SIZE);
            for (int j = 0; j < SWAP_RECORD_POLL_SIZE; j++) {
                final FlowFileRecord flowFile = tempQueue.poll();
                toSwap.add(flowFile);
                bytesSwappedOut += flowFile.getSize();
                flowFilesSwappedOut++;
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
        long updatedSwapQueueBytes = 0L;
        while (tempQueue.size() > swapThreshold) {
            final FlowFileRecord record = tempQueue.poll();
            swapQueue.add(record);
            updatedSwapQueueBytes += record.getSize();
        }

        Collections.reverse(swapQueue); // currently ordered in reverse priority order based on the ordering of the temp queue

        // replace the contents of the active queue, since we've merged it with the swap queue.
        activeQueue.clear();
        FlowFileRecord toRequeue;
        long activeQueueBytes = 0L;
        while ((toRequeue = tempQueue.poll()) != null) {
            activeQueue.offer(toRequeue);
            activeQueueBytes += toRequeue.getSize();
        }

        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize originalSize = size.get();

            final int addedSwapRecords = swapQueue.size() - originalSwapQueueCount;
            final long addedSwapBytes = updatedSwapQueueBytes - originalSwapQueueBytes;

            final FlowFileQueueSize newSize = new FlowFileQueueSize(activeQueue.size(), activeQueueBytes,
                originalSize.swappedCount + addedSwapRecords + flowFilesSwappedOut,
                originalSize.swappedBytes + addedSwapBytes + bytesSwappedOut,
                originalSize.swapFiles + numSwapFiles,
                originalSize.unacknowledgedCount, originalSize.unacknowledgedBytes);
            updated = size.compareAndSet(originalSize, newSize);
        }

        this.swapLocations.addAll(swapLocations);
    }


    @Override
    public long drainQueue(final Queue<FlowFileRecord> sourceQueue, final List<FlowFileRecord> destination, int maxResults, final Set<FlowFileRecord> expiredRecords) {
        long drainedSize = 0L;
        FlowFileRecord pulled = null;

        final long expirationMillis = expirationPeriod.get().getMillis();
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
        long bytesPulled = 0L;
        int flowFilesPulled = 0;

        writeLock.lock();
        try {
            migrateSwapToActive();

            final long expirationMillis = expirationPeriod.get().getMillis();

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
                    bytesPulled += flowFile.getSize();
                    flowFilesPulled++;

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
                    bytesPulled += flowFile.getSize();
                    flowFilesPulled++;

                    incrementUnacknowledgedQueueSize(1, flowFile.getSize());
                    selectedFlowFiles.add(flowFile);
                } else {
                    unselected.add(flowFile);
                }

                if (!result.isContinue()) {
                    break;
                }
            }

            this.activeQueue.addAll(unselected);
            incrementActiveQueueSize(-flowFilesPulled, -bytesPulled);

            return selectedFlowFiles;
        } finally {
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
        return expirationPeriod.get().getPeriod();
    }

    @Override
    public int getFlowFileExpiration(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(expirationPeriod.get().getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setFlowFileExpiration(final String flowExpirationPeriod) {
        final long millis = FormatUtils.getTimeDuration(flowExpirationPeriod, TimeUnit.MILLISECONDS);
        if (millis < 0) {
            throw new IllegalArgumentException("FlowFile Expiration Period must be positive");
        }

        expirationPeriod.set(new TimePeriod(flowExpirationPeriod, millis));
    }


    @Override
    public void purgeSwapFiles() {
        swapManager.purge();
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        int swapFlowFileCount = 0;
        long swapByteCount = 0L;
        Long maxId = null;
        List<ResourceClaim> resourceClaims = new ArrayList<>();
        final long startNanos = System.nanoTime();

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
                    final SwapSummary summary = swapManager.getSwapSummary(swapLocation);
                    final QueueSize queueSize = summary.getQueueSize();
                    final Long maxSwapRecordId = summary.getMaxFlowFileId();
                    if (maxSwapRecordId != null) {
                        if (maxId == null || maxSwapRecordId > maxId) {
                            maxId = maxSwapRecordId;
                        }
                    }

                    swapFlowFileCount += queueSize.getObjectCount();
                    swapByteCount += queueSize.getByteCount();
                    resourceClaims.addAll(summary.getResourceClaims());
                } catch (final IOException ioe) {
                    logger.error("Failed to recover FlowFiles from Swap File {}; the file appears to be corrupt", swapLocation, ioe.toString());
                    logger.error("", ioe);
                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to recover FlowFiles from Swap File " + swapLocation +
                            "; the file appears to be corrupt. See logs for more details");
                    }
                }
            }

            incrementSwapQueueSize(swapFlowFileCount, swapByteCount, swapLocations.size());
            this.swapLocations.addAll(swapLocations);
        } finally {
            writeLock.unlock("Recover Swap Files");
        }

        if (!swapLocations.isEmpty()) {
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            logger.info("Recovered {} swap files for {} in {} millis", swapLocations.size(), this, millis);
        }

        return new StandardSwapSummary(new QueueSize(swapFlowFileCount, swapByteCount), maxId, resourceClaims);
    }


    @Override
    public String toString() {
        return "FlowFileQueue[id=" + identifier + "]";
    }


    @Override
    public ListFlowFileStatus listFlowFiles(final String requestIdentifier, final int maxResults) {
        // purge any old requests from the map just to keep it clean. But if there are very few requests, which is usually the case, then don't bother
        if (listRequestMap.size() > 10) {
            final List<String> toDrop = new ArrayList<>();
            for (final Map.Entry<String, ListFlowFileRequest> entry : listRequestMap.entrySet()) {
                final ListFlowFileRequest request = entry.getValue();
                final boolean completed = request.getState() == ListFlowFileState.COMPLETE || request.getState() == ListFlowFileState.FAILURE;

                if (completed && System.currentTimeMillis() - request.getLastUpdated() > TimeUnit.MINUTES.toMillis(5L)) {
                    toDrop.add(entry.getKey());
                }
            }

            for (final String requestId : toDrop) {
                listRequestMap.remove(requestId);
            }
        }

        // numSteps = 1 for each swap location + 1 for active queue + 1 for swap queue.
        final ListFlowFileRequest listRequest = new ListFlowFileRequest(requestIdentifier, maxResults, size());

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                int position = 0;
                int resultCount = 0;
                final List<FlowFileSummary> summaries = new ArrayList<>();

                // Create an ArrayList that contains all of the contents of the active queue.
                // We do this so that we don't have to hold the lock any longer than absolutely necessary.
                // We cannot simply pull the first 'maxResults' records from the queue, however, because the
                // Iterator provided by PriorityQueue does not return records in order. So we would have to either
                // use a writeLock and 'pop' the first 'maxResults' records off the queue or use a read lock and
                // do a shallow copy of the queue. The shallow copy is generally quicker because it doesn't have to do
                // the sorting to put the records back. So even though this has an expensive of Java Heap to create the
                // extra collection, we are making this trade-off to avoid locking the queue any longer than required.
                final List<FlowFileRecord> allFlowFiles;
                final Prioritizer prioritizer;
                readLock.lock();
                try {
                    logger.debug("{} Acquired lock to perform listing of FlowFiles", StandardFlowFileQueue.this);
                    allFlowFiles = new ArrayList<>(activeQueue);
                    prioritizer = new Prioritizer(StandardFlowFileQueue.this.priorities);
                } finally {
                    readLock.unlock("List FlowFiles");
                }

                listRequest.setState(ListFlowFileState.CALCULATING_LIST);

                // sort the FlowFileRecords so that we have the list in the same order as on the queue.
                Collections.sort(allFlowFiles, prioritizer);

                for (final FlowFileRecord flowFile : allFlowFiles) {
                    summaries.add(summarize(flowFile, ++position));
                    if (summaries.size() >= maxResults) {
                        break;
                    }
                }

                logger.debug("{} Finished listing FlowFiles for active queue with a total of {} results", StandardFlowFileQueue.this, resultCount);
                listRequest.setFlowFileSummaries(summaries);
                listRequest.setState(ListFlowFileState.COMPLETE);
            }
        }, "List FlowFiles for Connection " + getIdentifier());
        t.setDaemon(true);
        t.start();

        listRequestMap.put(requestIdentifier, listRequest);
        return listRequest;
    }

    private FlowFileSummary summarize(final FlowFile flowFile, final int position) {
        // extract all of the information that we care about into new variables rather than just
        // wrapping the FlowFile object with a FlowFileSummary object. We do this because we want to
        // be able to hold many FlowFileSummary objects in memory and if we just wrap the FlowFile object,
        // we will end up holding the entire FlowFile (including all Attributes) in the Java heap as well,
        // which can be problematic if we expect them to be swapped out.
        final String uuid = flowFile.getAttribute(CoreAttributes.UUID.key());
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final long size = flowFile.getSize();
        final Long lastQueuedTime = flowFile.getLastQueueDate();
        final long lineageStart = flowFile.getLineageStartDate();
        final boolean penalized = flowFile.isPenalized();

        return new FlowFileSummary() {
            @Override
            public String getUuid() {
                return uuid;
            }

            @Override
            public String getFilename() {
                return filename;
            }

            @Override
            public int getPosition() {
                return position;
            }

            @Override
            public long getSize() {
                return size;
            }

            @Override
            public long getLastQueuedTime() {
                return lastQueuedTime == null ? 0L : lastQueuedTime;
            }

            @Override
            public long getLineageStartDate() {
                return lineageStart;
            }

            @Override
            public boolean isPenalized() {
                return penalized;
            }
        };
    }


    @Override
    public ListFlowFileStatus getListFlowFileStatus(final String requestIdentifier) {
        return listRequestMap.get(requestIdentifier);
    }

    @Override
    public ListFlowFileStatus cancelListFlowFileRequest(final String requestIdentifier) {
        logger.info("Canceling ListFlowFile Request with ID {}", requestIdentifier);
        final ListFlowFileRequest request = listRequestMap.remove(requestIdentifier);
        if (request != null) {
            request.cancel();
        }

        return request;
    }

    @Override
    public FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException {
        if (flowFileUuid == null) {
            return null;
        }

        readLock.lock();
        try {
            // read through all of the FlowFiles in the queue, looking for the FlowFile with the given ID
            for (final FlowFileRecord flowFile : activeQueue) {
                if (flowFileUuid.equals(flowFile.getAttribute(CoreAttributes.UUID.key()))) {
                    return flowFile;
                }
            }
        } finally {
            readLock.unlock("getFlowFile");
        }

        return null;
    }


    @Override
    public void verifyCanList() throws IllegalStateException {
    }

    @Override
    public DropFlowFileStatus dropFlowFiles(final String requestIdentifier, final String requestor) {
        logger.info("Initiating drop of FlowFiles from {} on behalf of {} (request identifier={})", this, requestor, requestIdentifier);

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
        final QueueSize originalSize = getQueueSize();
        dropRequest.setCurrentSize(originalSize);
        dropRequest.setOriginalSize(originalSize);
        if (originalSize.getObjectCount() == 0) {
            dropRequest.setDroppedSize(originalSize);
            dropRequest.setState(DropFlowFileState.COMPLETE);
            dropRequestMap.put(requestIdentifier, dropRequest);
            return dropRequest;
        }

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                writeLock.lock();
                try {
                    dropRequest.setState(DropFlowFileState.DROPPING_FLOWFILES);
                    logger.debug("For DropFlowFileRequest {}, original size is {}", requestIdentifier, getQueueSize());

                    try {
                        final List<FlowFileRecord> activeQueueRecords = new ArrayList<>(activeQueue);

                        QueueSize droppedSize;
                        try {
                            if (dropRequest.getState() == DropFlowFileState.CANCELED) {
                                logger.info("Cancel requested for DropFlowFileRequest {}", requestIdentifier);
                                return;
                            }

                            droppedSize = drop(activeQueueRecords, requestor);
                            logger.debug("For DropFlowFileRequest {}, Dropped {} from active queue", requestIdentifier, droppedSize);
                        } catch (final IOException ioe) {
                            logger.error("Failed to drop the FlowFiles from queue {} due to {}", StandardFlowFileQueue.this.getIdentifier(), ioe.toString());
                            logger.error("", ioe);

                            dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + ioe.toString());
                            return;
                        }

                        activeQueue.clear();
                        incrementActiveQueueSize(-droppedSize.getObjectCount(), -droppedSize.getByteCount());
                        dropRequest.setCurrentSize(getQueueSize());
                        dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));

                        final QueueSize swapSize = size.get().swapQueueSize();
                        logger.debug("For DropFlowFileRequest {}, Swap Queue has {} elements, Swapped Record Count = {}, Swapped Content Size = {}",
                            requestIdentifier, swapQueue.size(), swapSize.getObjectCount(), swapSize.getByteCount());
                        if (dropRequest.getState() == DropFlowFileState.CANCELED) {
                            logger.info("Cancel requested for DropFlowFileRequest {}", requestIdentifier);
                            return;
                        }

                        try {
                            droppedSize = drop(swapQueue, requestor);
                        } catch (final IOException ioe) {
                            logger.error("Failed to drop the FlowFiles from queue {} due to {}", StandardFlowFileQueue.this.getIdentifier(), ioe.toString());
                            logger.error("", ioe);

                            dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + ioe.toString());
                            return;
                        }

                        swapQueue.clear();
                        dropRequest.setCurrentSize(getQueueSize());
                        dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));
                        swapMode = false;
                        incrementSwapQueueSize(-droppedSize.getObjectCount(), -droppedSize.getByteCount(), 0);
                        logger.debug("For DropFlowFileRequest {}, dropped {} from Swap Queue", requestIdentifier, droppedSize);

                        final int swapFileCount = swapLocations.size();
                        final Iterator<String> swapLocationItr = swapLocations.iterator();
                        while (swapLocationItr.hasNext()) {
                            final String swapLocation = swapLocationItr.next();

                            SwapContents swapContents = null;
                            try {
                                if (dropRequest.getState() == DropFlowFileState.CANCELED) {
                                    logger.info("Cancel requested for DropFlowFileRequest {}", requestIdentifier);
                                    return;
                                }

                                swapContents = swapManager.swapIn(swapLocation, StandardFlowFileQueue.this);
                                droppedSize = drop(swapContents.getFlowFiles(), requestor);
                            } catch (final IncompleteSwapFileException isfe) {
                                swapContents = isfe.getPartialContents();
                                final String warnMsg = "Failed to swap in FlowFiles from Swap File " + swapLocation + " because the file was corrupt. "
                                    + "Some FlowFiles may not be dropped from the queue until NiFi is restarted.";

                                logger.warn(warnMsg);
                                if (eventReporter != null) {
                                    eventReporter.reportEvent(Severity.WARNING, "Drop FlowFiles", warnMsg);
                                }
                            } catch (final IOException ioe) {
                                logger.error("Failed to swap in FlowFiles from Swap File {} in order to drop the FlowFiles for Connection {} due to {}",
                                    swapLocation, StandardFlowFileQueue.this.getIdentifier(), ioe.toString());
                                logger.error("", ioe);
                                if (eventReporter != null) {
                                    eventReporter.reportEvent(Severity.ERROR, "Drop FlowFiles", "Failed to swap in FlowFiles from Swap File " + swapLocation
                                        + ". The FlowFiles contained in this Swap File will not be dropped from the queue");
                                }

                                dropRequest.setState(DropFlowFileState.FAILURE, "Failed to swap in FlowFiles from Swap File " + swapLocation + " due to " + ioe.toString());
                                if (swapContents != null) {
                                    activeQueue.addAll(swapContents.getFlowFiles()); // ensure that we don't lose the FlowFiles from our queue.
                                }

                                return;
                            }

                            dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));
                            incrementSwapQueueSize(-droppedSize.getObjectCount(), -droppedSize.getByteCount(), -1);

                            dropRequest.setCurrentSize(getQueueSize());
                            swapLocationItr.remove();
                            logger.debug("For DropFlowFileRequest {}, dropped {} for Swap File {}", requestIdentifier, droppedSize, swapLocation);
                        }

                        logger.debug("Dropped FlowFiles from {} Swap Files", swapFileCount);
                        logger.info("Successfully dropped {} FlowFiles ({} bytes) from Connection with ID {} on behalf of {}",
                            dropRequest.getDroppedSize().getObjectCount(), dropRequest.getDroppedSize().getByteCount(), StandardFlowFileQueue.this.getIdentifier(), requestor);
                        dropRequest.setState(DropFlowFileState.COMPLETE);
                    } catch (final Exception e) {
                        logger.error("Failed to drop FlowFiles from Connection with ID {} due to {}", StandardFlowFileQueue.this.getIdentifier(), e.toString());
                        logger.error("", e);
                        dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + e.toString());
                    }
                } finally {
                    writeLock.unlock("Drop FlowFiles");
                }
            }
        }, "Drop FlowFiles for Connection " + getIdentifier());
        t.setDaemon(true);
        t.start();

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
            dropContentSize += flowFile.getSize();
            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim == null) {
                continue;
            }

            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            if (resourceClaim == null) {
                continue;
            }

            resourceClaimManager.decrementClaimantCount(resourceClaim);
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
        builder.setAttributes(flowFile.getAttributes(), Collections.<String, String> emptyMap());
        builder.setDetails("FlowFile Queue emptied by " + requestor);
        builder.setSourceQueueIdentifier(getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), contentClaim.getOffset(), flowFile.getSize());
        }

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

            @Override
            public List<ContentClaim> getTransientClaims() {
                return Collections.emptyList();
            }
        };
    }


    @Override
    public DropFlowFileRequest cancelDropFlowFileRequest(final String requestIdentifier) {
        final DropFlowFileRequest request = dropRequestMap.remove(requestIdentifier);
        if (request == null) {
            return null;
        }

        request.cancel();
        return request;
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
        return size.get().unacknowledgedQueueSize();
    }


    private void incrementActiveQueueSize(final int count, final long bytes) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = size.get();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(original.activeQueueCount + count, original.activeQueueBytes + bytes,
                original.swappedCount, original.swappedBytes, original.swapFiles, original.unacknowledgedCount, original.unacknowledgedBytes);
            updated = size.compareAndSet(original, newSize);

            if (updated) {
                logIfNegative(original, newSize, "active");
            }
        }
    }

    private void incrementSwapQueueSize(final int count, final long bytes, final int fileCount) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = size.get();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(original.activeQueueCount, original.activeQueueBytes,
                original.swappedCount + count, original.swappedBytes + bytes, original.swapFiles + fileCount, original.unacknowledgedCount, original.unacknowledgedBytes);
            updated = size.compareAndSet(original, newSize);

            if (updated) {
                logIfNegative(original, newSize, "swap");
            }
        }
    }

    private void incrementUnacknowledgedQueueSize(final int count, final long bytes) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = size.get();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(original.activeQueueCount, original.activeQueueBytes,
                original.swappedCount, original.swappedBytes, original.swapFiles, original.unacknowledgedCount + count, original.unacknowledgedBytes + bytes);
            updated = size.compareAndSet(original, newSize);

            if (updated) {
                logIfNegative(original, newSize, "Unacknowledged");
            }
        }
    }

    private void logIfNegative(final FlowFileQueueSize original, final FlowFileQueueSize newSize, final String counterName) {
        if (newSize.activeQueueBytes < 0 || newSize.activeQueueCount < 0 || newSize.swappedBytes < 0 || newSize.swappedCount < 0
                || newSize.unacknowledgedBytes < 0 || newSize.unacknowledgedCount < 0) {

            logger.error("Updated Size of Queue " + counterName + " from " + original + " to " + newSize, new RuntimeException("Cannot create negative queue size"));

        }
    }


    private static class FlowFileQueueSize {
        private final int activeQueueCount;
        private final long activeQueueBytes;
        private final int swappedCount;
        private final long swappedBytes;
        private final int swapFiles;
        private final int unacknowledgedCount;
        private final long unacknowledgedBytes;

        public FlowFileQueueSize(final int activeQueueCount, final long activeQueueBytes, final int swappedCount, final long swappedBytes, final int swapFileCount,
            final int unacknowledgedCount, final long unacknowledgedBytes) {
            this.activeQueueCount = activeQueueCount;
            this.activeQueueBytes = activeQueueBytes;
            this.swappedCount = swappedCount;
            this.swappedBytes = swappedBytes;
            this.swapFiles = swapFileCount;
            this.unacknowledgedCount = unacknowledgedCount;
            this.unacknowledgedBytes = unacknowledgedBytes;
        }

        public boolean isEmpty() {
            return activeQueueCount == 0 && swappedCount == 0 && unacknowledgedCount == 0;
        }

        public QueueSize toQueueSize() {
            return new QueueSize(activeQueueCount + swappedCount + unacknowledgedCount, activeQueueBytes + swappedBytes + unacknowledgedBytes);
        }

        public QueueSize activeQueueSize() {
            return new QueueSize(activeQueueCount, activeQueueBytes);
        }

        public QueueSize unacknowledgedQueueSize() {
            return new QueueSize(unacknowledgedCount, unacknowledgedBytes);
        }

        public QueueSize swapQueueSize() {
            return new QueueSize(swappedCount, swappedBytes);
        }

        @Override
        public String toString() {
            return "FlowFile Queue Size[ ActiveQueue=[" + activeQueueCount + ", " + activeQueueBytes +
                " Bytes], Swap Queue=[" + swappedCount + ", " + swappedBytes +
                " Bytes], Swap Files=[" + swapFiles + "], Unacknowledged=[" + unacknowledgedCount + ", " + unacknowledgedBytes + " Bytes] ]";
        }
    }


    private static class MaxQueueSize {
        private final String maxSize;
        private final long maxBytes;
        private final long maxCount;

        public MaxQueueSize(final String maxSize, final long maxBytes, final long maxCount) {
            this.maxSize = maxSize;
            this.maxBytes = maxBytes;
            this.maxCount = maxCount;
        }

        public String getMaxSize() {
            return maxSize;
        }

        public long getMaxBytes() {
            return maxBytes;
        }

        public long getMaxCount() {
            return maxCount;
        }

        @Override
        public String toString() {
            return maxCount + " Objects/" + maxSize;
        }
    }

    private static class TimePeriod {
        private final String period;
        private final long millis;

        public TimePeriod(final String period, final long millis) {
            this.period = period;
            this.millis = millis;
        }

        public String getPeriod() {
            return period;
        }

        public long getMillis() {
            return millis;
        }

        @Override
        public String toString() {
            return period;
        }
    }
}
