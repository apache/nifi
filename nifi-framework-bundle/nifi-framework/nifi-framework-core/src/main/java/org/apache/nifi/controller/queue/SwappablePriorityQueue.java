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

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.IncompleteSwapFileException;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.concurrency.TimedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SwappablePriorityQueue {
    private static final Logger logger = LoggerFactory.getLogger(SwappablePriorityQueue.class);
    private static final int SWAP_RECORD_POLL_SIZE = 10_000;
    private static final int MAX_EXPIRED_RECORDS_PER_ITERATION = 10_000;

    private final int swapThreshold;
    private final FlowFileSwapManager swapManager;
    private final EventReporter eventReporter;
    private final FlowFileQueue flowFileQueue;
    private final DropFlowFileAction dropAction;
    private volatile List<FlowFilePrioritizer> priorities = new ArrayList<>();
    private final String swapPartitionName;

    private final List<String> swapLocations = new ArrayList<>();
    private final AtomicReference<FlowFileQueueSize> size = new AtomicReference<>(new FlowFileQueueSize(0, 0L, 0, 0L, 0, 0, 0L));
    private final TimedLock readLock;
    private final TimedLock writeLock;

    // We keep an "active queue" and a "swap queue" that both are able to hold records in heap. When
    // FlowFiles are added to this FlowFileQueue, we first check if we are in "swap mode" and if so
    // we add to the 'swap queue' instead of the 'active queue'. The code would be much simpler if we
    // eliminated the 'swap queue' and instead just used the active queue and swapped out the 10,000
    // lowest priority FlowFiles from that. However, doing that would cause problems with the ordering
    // of FlowFiles. If we swap out some FlowFiles, and then allow a new FlowFile to be written to the
    // active queue, then we would end up processing the newer FlowFile before the swapped FlowFile. By
    // keeping these separate, we are able to guarantee that FlowFiles are swapped in in the same order
    // that they are swapped out.
    // Guarded by lock.
    private PriorityQueue<FlowFileRecord> activeQueue;
    private ArrayList<FlowFileRecord> swapQueue;
    private boolean swapMode = false;
    private volatile long topPenaltyExpiration = -1L;

    // The following members are used to keep metrics in memory for reporting purposes so that we don't have to constantly
    // read these values from swap files on disk.
    private final Map<String, Long> minQueueDateInSwapLocation = new HashMap<>();
    private final Map<String, Long> totalQueueDateInSwapLocation = new HashMap<>();

    public SwappablePriorityQueue(final FlowFileSwapManager swapManager, final int swapThreshold, final EventReporter eventReporter, final FlowFileQueue flowFileQueue,
        final DropFlowFileAction dropAction, final String swapPartitionName) {
        this.swapManager = swapManager;
        this.swapThreshold = swapThreshold;

        this.activeQueue = new PriorityQueue<>(20, new QueuePrioritizer(Collections.emptyList()));
        this.swapQueue = new ArrayList<>();
        this.eventReporter = eventReporter;
        this.flowFileQueue = flowFileQueue;
        this.dropAction = dropAction;
        this.swapPartitionName = swapPartitionName;

        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        readLock = new TimedLock(lock.readLock(), flowFileQueue.getIdentifier() + " Read Lock", 100);
        writeLock = new TimedLock(lock.writeLock(), flowFileQueue.getIdentifier() + " Write Lock", 100);
    }

    private String getQueueIdentifier() {
        return flowFileQueue.getIdentifier();
    }

    public List<FlowFilePrioritizer> getPriorities() {
        return Collections.unmodifiableList(priorities);
    }

    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        writeLock.lock();
        try {
            this.priorities = new ArrayList<>(newPriorities);

            final PriorityQueue<FlowFileRecord> newQueue = new PriorityQueue<>(Math.max(20, activeQueue.size()), new QueuePrioritizer(newPriorities));
            newQueue.addAll(activeQueue);
            activeQueue = newQueue;
        } finally {
            writeLock.unlock("setPriorities");
        }
    }


    public LocalQueuePartitionDiagnostics getQueueDiagnostics() {
        readLock.lock();
        try {
            final boolean anyPenalized = !activeQueue.isEmpty() && activeQueue.peek().isPenalized();
            final boolean allPenalized = anyPenalized && activeQueue.stream().anyMatch(FlowFileRecord::isPenalized);

            return new StandardLocalQueuePartitionDiagnostics(getFlowFileQueueSize(), anyPenalized, allPenalized);
        } finally {
            readLock.unlock("getQueueDiagnostics");
        }
    }

    public List<FlowFileRecord> getActiveFlowFiles() {
        readLock.lock();
        try {
            return new ArrayList<>(activeQueue);
        } finally {
            readLock.unlock("getActiveFlowFiles");
        }
    }

    public boolean isUnacknowledgedFlowFile() {
        return getFlowFileQueueSize().getUnacknowledgedCount() > 0;
    }

    /**
     * This method MUST be called with the write lock held
     */
    private void writeSwapFilesIfNecessary() {
        if (swapQueue.size() < SWAP_RECORD_POLL_SIZE) {
            return;
        }

        migrateSwapToActive();
        if (swapQueue.size() < SWAP_RECORD_POLL_SIZE) {
            return;
        }

        final int numSwapFiles = swapQueue.size() / SWAP_RECORD_POLL_SIZE;

        int originalSwapQueueCount = swapQueue.size();
        long originalSwapQueueBytes = 0L;
        for (final FlowFileRecord flowFile : swapQueue) {
            originalSwapQueueBytes += flowFile.getSize();
        }

        // Create a new Priority queue with the same prioritizers that are set for this queue. We want to swap out the highest priority data first, because
        // whatever data we don't write out to a swap file (because there isn't enough to fill a swap file) will be added back to the swap queue.
        // Since the swap queue cannot be processed until all swap files, we want to ensure that only the lowest priority data goes back onto it. Which means
        // that we must swap out the highest priority data that is currently on the swap queue.
        final PriorityQueue<FlowFileRecord> tempQueue = new PriorityQueue<>(swapQueue.size(), new QueuePrioritizer(getPriorities()));
        tempQueue.addAll(swapQueue);

        long bytesSwappedOut = 0L;
        int flowFilesSwappedOut = 0;
        final List<String> swapLocations = new ArrayList<>(numSwapFiles);
        for (int i = 0; i < numSwapFiles; i++) {
            long bytesSwappedThisIteration = 0L;
            long totalSwapQueueDatesThisIteration = 0L;
            long minQueueDateThisIteration = Long.MAX_VALUE;

            // Create a new swap file for the next SWAP_RECORD_POLL_SIZE records
            final List<FlowFileRecord> toSwap = new ArrayList<>(SWAP_RECORD_POLL_SIZE);
            for (int j = 0; j < SWAP_RECORD_POLL_SIZE; j++) {
                final FlowFileRecord flowFile = tempQueue.poll();
                toSwap.add(flowFile);
                bytesSwappedThisIteration += flowFile.getSize();
                totalSwapQueueDatesThisIteration += flowFile.getLastQueueDate();
                minQueueDateThisIteration = minQueueDateThisIteration < flowFile.getLastQueueDate() ? minQueueDateThisIteration : flowFile.getLastQueueDate();
            }

            try {
                Collections.reverse(toSwap); // currently ordered in reverse priority order based on the ordering of the temp queue.
                final String swapLocation = swapManager.swapOut(toSwap, flowFileQueue, swapPartitionName);
                swapLocations.add(swapLocation);

                logger.debug("Successfully wrote out Swap File {} containing {} FlowFiles ({} bytes)", swapLocation, toSwap.size(), bytesSwappedThisIteration);

                bytesSwappedOut += bytesSwappedThisIteration;
                flowFilesSwappedOut += toSwap.size();
                minQueueDateInSwapLocation.put(swapLocation, minQueueDateThisIteration);
                totalQueueDateInSwapLocation.put(swapLocation, totalSwapQueueDatesThisIteration);
            } catch (final IOException ioe) {
                tempQueue.addAll(toSwap); // if we failed, we must add the FlowFiles back to the queue.

                final int objectCount = getFlowFileCount();
                logger.error("FlowFile Queue with identifier {} has {} FlowFiles queued up. Attempted to spill FlowFile information over to disk in order to avoid exhausting "
                    + "the Java heap space but failed to write information to disk due to {}", getQueueIdentifier(), objectCount, ioe.toString());
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "Failed to Overflow to Disk", "Flowfile Queue with identifier " + getQueueIdentifier() + " has " + objectCount +
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
        FlowFileRecord record;
        while ((record = tempQueue.poll()) != null) {
            swapQueue.add(record);
            updatedSwapQueueBytes += record.getSize();
        }

        Collections.reverse(swapQueue); // currently ordered in reverse priority order based on the ordering of the temp queue

        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize originalSize = getFlowFileQueueSize();

            final int addedSwapRecords = swapQueue.size() - originalSwapQueueCount;
            final long addedSwapBytes = updatedSwapQueueBytes - originalSwapQueueBytes;

            final FlowFileQueueSize newSize = new FlowFileQueueSize(originalSize.getActiveCount(), originalSize.getActiveBytes(),
                originalSize.getSwappedCount() + addedSwapRecords + flowFilesSwappedOut,
                originalSize.getSwappedBytes() + addedSwapBytes + bytesSwappedOut,
                originalSize.getSwapFileCount() + numSwapFiles,
                originalSize.getUnacknowledgedCount(), originalSize.getUnacknowledgedBytes());

            updated = updateSize(originalSize, newSize);
            if (updated) {
                logIfNegative(originalSize, newSize, "swap");
            }
        }

        this.swapLocations.addAll(swapLocations);
        logger.debug("After writing swap files, setting new set of Swap Locations to {}", this.swapLocations);
    }

    private int getFlowFileCount() {
        final FlowFileQueueSize size = getFlowFileQueueSize();
        return size.getActiveCount() + size.getSwappedCount() + size.getUnacknowledgedCount();
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
        if (!activeQueue.isEmpty()) {
            return;
        }

        // If there are swap files waiting to be swapped in, swap those in first. We do this in order to ensure that those that
        // were swapped out first are then swapped back in first. If we instead just immediately migrated the FlowFiles from the
        // swap queue to the active queue, and we never run out of FlowFiles in the active queue (because destination cannot
        // keep up with queue), we will end up always processing the new FlowFiles first instead of the FlowFiles that arrived
        // first.
        if (!swapLocations.isEmpty()) {
            swapIn();
            return;
        }

        // this is the most common condition (nothing is swapped out), so do the check first and avoid the expense
        // of other checks for 99.999% of the cases.
        final FlowFileQueueSize size = getFlowFileQueueSize();
        if (size.getSwappedCount() == 0 && swapQueue.isEmpty()) {
            return;
        }

        if (size.getSwappedCount() > swapQueue.size()) {
            // we already have FlowFiles swapped out, so we won't migrate the queue; we will wait for
            // the files to be swapped back in first
            return;
        }

        // Swap Queue is not currently ordered. We want to migrate the highest priority FlowFiles to the Active Queue, then re-queue the lowest priority items.
        final PriorityQueue<FlowFileRecord> tempQueue = new PriorityQueue<>(swapQueue.size(), new QueuePrioritizer(getPriorities()));
        tempQueue.addAll(swapQueue);

        int recordsMigrated = 0;
        long bytesMigrated = 0L;
        while (activeQueue.size() < swapThreshold) {
            final FlowFileRecord toMigrate = tempQueue.poll();
            if (toMigrate == null) {
                break;
            }

            activeQueue.add(toMigrate);
            bytesMigrated += toMigrate.getSize();
            recordsMigrated++;
        }

        swapQueue.clear();
        FlowFileRecord toRequeue;
        while ((toRequeue = tempQueue.poll()) != null) {
            swapQueue.add(toRequeue);
        }

        if (recordsMigrated > 0) {
            incrementActiveQueueSize(recordsMigrated, bytesMigrated);
            incrementSwapQueueSize(-recordsMigrated, -bytesMigrated, 0);
            logger.debug("Migrated {} FlowFiles from swap queue to active queue for {}", recordsMigrated, this);
        }

        final FlowFileQueueSize updatedQueueSize = getFlowFileQueueSize();
        if (updatedQueueSize.getSwappedCount() == 0) {
            swapMode = false;
        }
    }

    private void swapIn() {
        final String swapLocation = swapLocations.get(0);
        boolean partialContents = false;
        SwapContents swapContents;
        try {
            logger.debug("Attempting to swap in {}; all swap locations = {}", swapLocation, swapLocations);
            swapContents = swapManager.swapIn(swapLocation, flowFileQueue);
            swapLocations.remove(0);
            minQueueDateInSwapLocation.remove(swapLocation);
            totalQueueDateInSwapLocation.remove(swapLocation);
        } catch (final IncompleteSwapFileException isfe) {
            logger.error("Failed to swap in all FlowFiles from Swap File {}; Swap File ended prematurely. The records that were present will still be swapped in", swapLocation);
            logger.error("", isfe);
            swapContents = isfe.getPartialContents();
            partialContents = true;
            swapLocations.remove(0);
            minQueueDateInSwapLocation.remove(swapLocation);
            totalQueueDateInSwapLocation.remove(swapLocation);
        } catch (final FileNotFoundException fnfe) {
            logger.error("Failed to swap in FlowFiles from Swap File {} because the Swap File can no longer be found", swapLocation);
            if (eventReporter != null) {
                eventReporter.reportEvent(Severity.ERROR, "Swap File", "Failed to swap in FlowFiles from Swap File " + swapLocation + " because the Swap File can no longer be found");
            }

            swapLocations.remove(0);
            minQueueDateInSwapLocation.remove(swapLocation);
            totalQueueDateInSwapLocation.remove(swapLocation);
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
            logger.debug("Swapped in partial contents containing {} FlowFiles ({} bytes) from {}", swapContents.getFlowFiles().size(), contentSizeSwappedIn, swapLocation);
        } else {
            // we swapped in the whole swap file. We can just use the info that we got from the summary.
            incrementActiveQueueSize(flowFileCount, contentSize);
            logger.debug("Successfully swapped in Swap File {} containing {} FlowFiles ({} bytes)", swapLocation, flowFileCount, contentSize);
        }

        activeQueue.addAll(swapContents.getFlowFiles());
    }

    public QueueSize size() {
        return getFlowFileQueueSize().toQueueSize();
    }

    public boolean isEmpty() {
        return getFlowFileQueueSize().isEmpty();
    }

    public boolean isActiveQueueEmpty() {
        final FlowFileQueueSize queueSize = getFlowFileQueueSize();
        return queueSize.getActiveCount() == 0 && queueSize.getSwappedCount() == 0;
    }

    public FlowFileAvailability getFlowFileAvailability() {
        // If queue is empty, avoid obtaining a lock.
        if (isActiveQueueEmpty()) {
            return FlowFileAvailability.ACTIVE_QUEUE_EMPTY;
        }

        final long expiration = topPenaltyExpiration;
        if (expiration > 0 && expiration > System.currentTimeMillis()) { // compare against 0 to avoid unnecessary System call
            return FlowFileAvailability.HEAD_OF_QUEUE_PENALIZED;
        }

        return FlowFileAvailability.FLOWFILE_AVAILABLE;
    }

    public void acknowledge(final FlowFileRecord flowFile) {
        logger.trace("{} Acknowledging {}", this, flowFile);
        directlyIncrementUnacknowledgedQueueSize(-1, -flowFile.getSize());
    }

    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        if (logger.isTraceEnabled()) {
            for (final FlowFileRecord flowFile : flowFiles) {
                logger.trace("{} Acknowledging {}", this, flowFile);
            }
        }

        final long totalSize = flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum();
        directlyIncrementUnacknowledgedQueueSize(-flowFiles.size(), -totalSize);
    }


    public void put(final FlowFileRecord flowFile) {
        writeLock.lock();
        try {
            if (swapMode || activeQueue.size() >= swapThreshold) {
                swapQueue.add(flowFile);
                incrementSwapQueueSize(1, flowFile.getSize(), 0);
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                incrementActiveQueueSize(1, flowFile.getSize());
                activeQueue.add(flowFile);
            }

            updateTopPenaltyExpiration();
            logger.trace("{} put to {}", flowFile, this);
        } finally {
            writeLock.unlock("put(FlowFileRecord)");
        }
    }

    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        final int numFiles = flowFiles.size();
        long bytes = 0L;
        for (final FlowFile flowFile : flowFiles) {
            bytes += flowFile.getSize();
        }

        writeLock.lock();
        try {
            if (swapMode || activeQueue.size() >= swapThreshold - numFiles) {
                swapQueue.addAll(flowFiles);
                incrementSwapQueueSize(numFiles, bytes, 0);
                swapMode = true;
                writeSwapFilesIfNecessary();
            } else {
                incrementActiveQueueSize(numFiles, bytes);
                activeQueue.addAll(flowFiles);
            }

            updateTopPenaltyExpiration();
            logger.trace("{} put to {}", flowFiles, this);
        } finally {
            writeLock.unlock("putAll");
        }
    }

    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis) {
        return poll(expiredRecords, expirationMillis, PollStrategy.UNPENALIZED_FLOWFILES);
    }

    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final PollStrategy pollStrategy) {
        FlowFileRecord flowFile;

        // First check if we have any records Pre-Fetched.
        writeLock.lock();
        try {
            flowFile = doPoll(expiredRecords, expirationMillis, pollStrategy);

            if (flowFile != null) {
                logger.trace("{} poll() returning {}", this, flowFile);
                unacknowledge(1, flowFile.getSize());
            }

            updateTopPenaltyExpiration();

            return flowFile;
        } finally {
            writeLock.unlock("poll(Set)");
        }
    }


    private FlowFileRecord doPoll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final PollStrategy pollStrategy) {
        FlowFileRecord flowFile;
        boolean isExpired;

        migrateSwapToActive();

        long expiredBytes = 0L;
        do {
            flowFile = this.activeQueue.poll();

            isExpired = isExpired(flowFile, expirationMillis);
            if (isExpired) {
                expiredRecords.add(flowFile);
                expiredBytes += flowFile.getSize();
                flowFile = null;

                if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                    break;
                }
            } else if (flowFile != null && flowFile.isPenalized() && pollStrategy == PollStrategy.UNPENALIZED_FLOWFILES) {
                this.activeQueue.add(flowFile);
                flowFile = null;
                break;
            }
        } while (isExpired);

        if (!expiredRecords.isEmpty()) {
            incrementActiveQueueSize(-expiredRecords.size(), -expiredBytes);
        }

        return flowFile;
    }

    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords, final long expirationMillis) {
        return poll(maxResults, expiredRecords, expirationMillis, PollStrategy.UNPENALIZED_FLOWFILES);
    }

    public List<FlowFileRecord> poll(int maxResults, final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final PollStrategy pollStrategy) {
        final List<FlowFileRecord> records = new ArrayList<>(Math.min(1, maxResults));

        // First check if we have any records Pre-Fetched.
        writeLock.lock();
        try {
            doPoll(records, maxResults, expiredRecords, expirationMillis, pollStrategy);
            updateTopPenaltyExpiration();
        } finally {
            writeLock.unlock("poll(int, Set)");
        }

        if (!records.isEmpty() && logger.isTraceEnabled()) {
            for (final FlowFileRecord flowFile : records) {
                logger.trace("{} poll() returning {}", this, flowFile);
            }
        }

        return records;
    }

    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords, final long expirationMillis) {
        return poll(filter, expiredRecords, expirationMillis, PollStrategy.UNPENALIZED_FLOWFILES);
    }

    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final PollStrategy pollStrategy) {
        long bytesPulled = 0L;
        int flowFilesPulled = 0;
        long bytesExpired = 0L;
        int flowFilesExpired = 0;

        writeLock.lock();
        try {
            migrateSwapToActive();

            final List<FlowFileRecord> selectedFlowFiles = new ArrayList<>();
            final List<FlowFileRecord> unselected = new ArrayList<>();

            while (true) {
                FlowFileRecord flowFile = this.activeQueue.poll();
                if (flowFile == null) {
                    break;
                }

                final boolean isExpired = isExpired(flowFile, expirationMillis);
                if (isExpired) {
                    expiredRecords.add(flowFile);
                    bytesExpired += flowFile.getSize();
                    flowFilesExpired++;

                    if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                        break;
                    } else {
                        continue;
                    }
                } else if (flowFile.isPenalized() && pollStrategy == PollStrategy.UNPENALIZED_FLOWFILES) {
                    this.activeQueue.add(flowFile);
                    break; // just stop searching because the rest are all penalized.
                }

                final FlowFileFilterResult result;
                try {
                    result = filter.filter(flowFile);
                } catch (final Throwable t) {
                    unselected.add(flowFile);
                    activeQueue.addAll(unselected);
                    activeQueue.addAll(selectedFlowFiles);
                    throw t;
                }

                if (result.isAccept()) {
                    bytesPulled += flowFile.getSize();
                    flowFilesPulled++;

                    selectedFlowFiles.add(flowFile);
                } else {
                    unselected.add(flowFile);
                }

                if (!result.isContinue()) {
                    break;
                }
            }

            this.activeQueue.addAll(unselected);

            unacknowledge(flowFilesPulled, bytesPulled);

            if (flowFilesExpired > 0) {
                incrementActiveQueueSize(-flowFilesExpired, -bytesExpired);
            }

            if (!selectedFlowFiles.isEmpty() && logger.isTraceEnabled()) {
                for (final FlowFileRecord flowFile : selectedFlowFiles) {
                    logger.trace("{} poll() returning {}", this, flowFile);
                }
            }

            updateTopPenaltyExpiration();

            return selectedFlowFiles;
        } finally {
            writeLock.unlock("poll(Filter, Set)");
        }
    }

    // MUST be called while holding read lock or write lock
    private void updateTopPenaltyExpiration() {
        final FlowFileRecord top = activeQueue.peek();
        if (top == null) {
            topPenaltyExpiration = -1L;
            return;
        }

        topPenaltyExpiration = top.getPenaltyExpirationMillis();
    }

    private void doPoll(final List<FlowFileRecord> records, int maxResults, final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final PollStrategy pollStrategy) {
        migrateSwapToActive();

        final long bytesDrained = drainQueue(activeQueue, records, maxResults, expiredRecords, expirationMillis, pollStrategy);

        long expiredBytes = 0L;
        for (final FlowFileRecord record : expiredRecords) {
            expiredBytes += record.getSize();
        }

        unacknowledge(records.size(), bytesDrained);
        if (!expiredRecords.isEmpty()) {
            incrementActiveQueueSize(-expiredRecords.size(), -expiredBytes);
        }
    }


    protected boolean isExpired(final FlowFile flowFile, final long expirationMillis) {
        return isLaterThan(getExpirationDate(flowFile, expirationMillis));
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


    private long drainQueue(final Queue<FlowFileRecord> sourceQueue, final List<FlowFileRecord> destination,
                            int maxResults, final Set<FlowFileRecord> expiredRecords, final long expirationMillis,
                            final PollStrategy pollStrategy) {
        long drainedSize = 0L;
        FlowFileRecord pulled;

        while (destination.size() < maxResults && (pulled = sourceQueue.poll()) != null) {
            if (isExpired(pulled, expirationMillis)) {
                expiredRecords.add(pulled);
                if (expiredRecords.size() >= MAX_EXPIRED_RECORDS_PER_ITERATION) {
                    break;
                }
            } else {
                if (pulled.isPenalized() && pollStrategy == PollStrategy.UNPENALIZED_FLOWFILES) {
                    sourceQueue.add(pulled);
                    break;
                }
                destination.add(pulled);
            }
            drainedSize += pulled.getSize();
        }
        return drainedSize;
    }


    public FlowFileRecord getFlowFile(final String flowFileUuid) {
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


    public void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor) {
        final String requestIdentifier = dropRequest.getRequestIdentifier();

        writeLock.lock();
        try {
            dropRequest.setState(DropFlowFileState.DROPPING_FLOWFILES);
            logger.debug("For DropFlowFileRequest {}, original size is {}", requestIdentifier, size());

            try {
                final List<FlowFileRecord> activeQueueRecords = new ArrayList<>(activeQueue);

                QueueSize droppedSize;
                try {
                    if (dropRequest.getState() == DropFlowFileState.CANCELED) {
                        logger.info("Cancel requested for DropFlowFileRequest {}", requestIdentifier);
                        return;
                    }

                    droppedSize = dropAction.drop(activeQueueRecords, requestor);
                    logger.debug("For DropFlowFileRequest {}, Dropped {} from active queue", requestIdentifier, droppedSize);
                } catch (final IOException ioe) {
                    logger.error("Failed to drop the FlowFiles from queue {}", getQueueIdentifier(), ioe);

                    dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + ioe.toString());
                    return;
                }

                activeQueue.clear();
                incrementActiveQueueSize(-droppedSize.getObjectCount(), -droppedSize.getByteCount());
                dropRequest.setCurrentSize(size());
                dropRequest.setDroppedSize(dropRequest.getDroppedSize().add(droppedSize));

                final QueueSize swapSize = getFlowFileQueueSize().swapQueueSize();
                logger.debug("For DropFlowFileRequest {}, Swap Queue has {} elements, Swapped Record Count = {}, Swapped Content Size = {}",
                    requestIdentifier, swapQueue.size(), swapSize.getObjectCount(), swapSize.getByteCount());
                if (dropRequest.getState() == DropFlowFileState.CANCELED) {
                    logger.info("Cancel requested for DropFlowFileRequest {}", requestIdentifier);
                    return;
                }

                try {
                    droppedSize = dropAction.drop(swapQueue, requestor);
                } catch (final IOException ioe) {
                    logger.error("Failed to drop the FlowFiles from queue {}", getQueueIdentifier(), ioe);

                    dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + ioe.toString());
                    return;
                }

                swapQueue.clear();
                dropRequest.setCurrentSize(size());
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

                        swapContents = swapManager.swapIn(swapLocation, flowFileQueue);
                        droppedSize = dropAction.drop(swapContents.getFlowFiles(), requestor);
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
                            swapLocation, getQueueIdentifier(), ioe.toString());
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

                    dropRequest.setCurrentSize(size());
                    swapLocationItr.remove();
                    minQueueDateInSwapLocation.remove(swapLocation);
                    totalQueueDateInSwapLocation.remove(swapLocation);
                    logger.debug("For DropFlowFileRequest {}, dropped {} for Swap File {}", requestIdentifier, droppedSize, swapLocation);
                }

                logger.debug("Dropped FlowFiles from {} Swap Files", swapFileCount);
                logger.info("Successfully dropped {} FlowFiles ({} bytes) from Connection with ID {} on behalf of {}",
                    dropRequest.getDroppedSize().getObjectCount(), dropRequest.getDroppedSize().getByteCount(), getQueueIdentifier(), requestor);
                dropRequest.setState(DropFlowFileState.COMPLETE);
            } catch (final Exception e) {
                logger.error("Failed to drop FlowFiles from Connection with ID {}", getQueueIdentifier(), e);
                dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + e.toString());
            }
        } finally {
            writeLock.unlock("Drop FlowFiles");
        }
    }



    public SwapSummary recoverSwappedFlowFiles() {
        int swapFlowFileCount = 0;
        long swapByteCount = 0L;
        long totalSwappedQueueDate = 0L;
        Long minSwappedQueueDate = null;
        Long maxId = null;
        List<ResourceClaim> resourceClaims = new ArrayList<>();
        final long startNanos = System.nanoTime();
        int failures = 0;

        writeLock.lock();
        try {
            final List<String> swapLocationsFromSwapManager;
            try {
                swapLocationsFromSwapManager = swapManager.recoverSwapLocations(flowFileQueue, swapPartitionName);
            } catch (final IOException ioe) {
                logger.error("Failed to determine whether or not any Swap Files exist for FlowFile Queue {}", getQueueIdentifier());
                logger.error("", ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to determine whether or not any Swap Files exist for FlowFile Queue " +
                        getQueueIdentifier() + "; see logs for more detials");
                }
                return null;
            }

            // If we have a duplicate of any of the swap location that we already know about, we need to filter those out now.
            // This can happen when, upon startup, we need to swap data out during the swap file recovery. In this case, we do
            // not want to include such a swap file in those that we recover, because those have already been accounted for when
            // they were added to the queue, before being swapped out.
            final Set<String> swapLocations = new LinkedHashSet<>(swapLocationsFromSwapManager);
            swapLocations.removeAll(this.swapLocations);

            logger.debug("Swap Manager reports {} Swap Files for {}: {}", swapLocations.size(), flowFileQueue, swapLocations);
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

                    // Update class member metrics
                    minQueueDateInSwapLocation.put(swapLocation, summary.getMinLastQueueDate());
                    totalQueueDateInSwapLocation.put(swapLocation, summary.getTotalLastQueueDate());

                    // Update metrics for this method's return value
                    if (minSwappedQueueDate == null) {
                        minSwappedQueueDate = summary.getMinLastQueueDate();
                    } else {
                        if (summary.getMinLastQueueDate() != null) {
                            minSwappedQueueDate = Long.min(minSwappedQueueDate, summary.getMinLastQueueDate());
                        }
                    }
                    totalSwappedQueueDate += summary.getTotalLastQueueDate();
                } catch (final IOException ioe) {
                    failures++;
                    logger.error("Failed to recover FlowFiles from Swap File {}; the file appears to be corrupt", swapLocation);
                    logger.error("", ioe);
                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to recover FlowFiles from Swap File " + swapLocation +
                            "; the file appears to be corrupt. See logs for more details");
                    }
                }
            }

            incrementSwapQueueSize(swapFlowFileCount, swapByteCount, swapLocations.size());
            this.swapLocations.addAll(swapLocations);
            updateTopPenaltyExpiration();
        } finally {
            writeLock.unlock("Recover Swap Files");
        }

        if (swapLocations.isEmpty()) {
            logger.debug("No swap files were recovered for {}", flowFileQueue);
        } else {
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            logger.info("Recovered {} swap files for {} in {} millis", swapLocations.size() - failures, this, millis);
        }

        // minSwappedQueueDate and totalSwappedQueueDate within this particular StandardSwapSummary are not ultimately used by the FlowController. However,
        // it can't hurt to set them here accurately in case they ever are.
        return new StandardSwapSummary(new QueueSize(swapFlowFileCount, swapByteCount), maxId, resourceClaims, minSwappedQueueDate, totalSwappedQueueDate);
    }

    public long getMinLastQueueDate() {
        readLock.lock();
        try {
            // We want the oldest timestamp, which will be the min
            long min = getMinLastQueueDate(activeQueue, 0L);
            min = Long.min(min, getMinLastQueueDate(swapQueue, min));

            for (Long minSwapQueueDate: minQueueDateInSwapLocation.values()) {
                min = min == 0 ? minSwapQueueDate : Long.min(min, minSwapQueueDate);
            }

            return min;
        } finally {
            readLock.unlock("Get Min Last Queue Date");
        }
    }

    private long getMinLastQueueDate(Iterable<FlowFileRecord> iterable, long defaultMin) {
        long min = 0;

        for (FlowFileRecord flowFileRecord : iterable) {
            min = min == 0 ? flowFileRecord.getLastQueueDate() : Long.min(flowFileRecord.getLastQueueDate(), min);
        }

        return min == 0 ? defaultMin : min;
    }

    public long getTotalQueuedDuration(long fromTimestamp) {
        readLock.lock();
        try {
            long sum = 0L;
            for (FlowFileRecord flowFileRecord : activeQueue) {
                sum += (fromTimestamp - flowFileRecord.getLastQueueDate());
            }

            for (FlowFileRecord flowFileRecord : swapQueue) {
                sum += (fromTimestamp - flowFileRecord.getLastQueueDate());
            }

            long totalSwappedQueueDate = 0L;
            for (Long totalQueueDate: totalQueueDateInSwapLocation.values()) {
                totalSwappedQueueDate += totalQueueDate;
            }

            // We are only considering FlowFiles that have been swapped to disk in this calculation since we took care of the
            // in-memory swapQueue previously.
            sum += ((getFlowFileQueueSize().getSwappedCount() - swapQueue.size()) * fromTimestamp) - totalSwappedQueueDate;
            return sum;
        } finally {
            readLock.unlock("Get Total Queued Duration");
        }
    }

    protected void incrementActiveQueueSize(final int count, final long bytes) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = size.get();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(
                original.getActiveCount() + count, original.getActiveBytes() + bytes,
                original.getSwappedCount(), original.getSwappedBytes(), original.getSwapFileCount(),
                original.getUnacknowledgedCount(), original.getUnacknowledgedBytes());

            updated = updateSize(original, newSize);

            if (updated) {
                logIfNegative(original, newSize, "active");
            }
        }
    }

    private void incrementSwapQueueSize(final int count, final long bytes, final int fileCount) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = getFlowFileQueueSize();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(original.getActiveCount(), original.getActiveBytes(),
                original.getSwappedCount() + count, original.getSwappedBytes() + bytes, original.getSwapFileCount() + fileCount,
                original.getUnacknowledgedCount(), original.getUnacknowledgedBytes());

            updated = updateSize(original, newSize);
            if (updated) {
                logIfNegative(original, newSize, "swap");
            }
        }
    }

    /**
     * Increments the unacknowledged queue size and decrements the active queue size by the given values.
     * This logic is extracted into a separate method because it is critical that we always increment the unacknowledged queue size
     * before decrementing the active queue size. Failure to do so could result in the queue temporarily reporting a size of 0/empty
     * when the queue is not empty (because we could decrement active queue size to 0 before incrementing unacknowledged queue size to 1).
     * To help ensure that we don't break that ordering, we encapsulate the logic into a single method that is responsible for performing
     * these steps, and we also used the name 'directlyIncrementUnacknowledgedQueueSize' for the method that updates the unacknowledged queue size, rather than simply
     * 'incrementUnacknowledgedQueueSize' to give a hint to any callers that caution must be taken when calling the method.
     *
     * @param count the number of FlowFiles to increase the unacknowledged count by and decrement active count by
     * @param bytes the bytes to increase the unacknowledged count by and decrement the active count by
     */
    private void unacknowledge(final int count, final long bytes) {
        directlyIncrementUnacknowledgedQueueSize(count, bytes);
        incrementActiveQueueSize(-count, -bytes);
    }

    /**
     * Increments the Unacknowledged Queue Size by the given arguments. Note that when data is polled, we need to both increment the unacknowledged size
     * AND decrement the active size. But it is crucial that we perform these actions in the proper order. The Unacknowledged size must be incremented before
     * the active queue size is decremented. For this purpose, use {@link #unacknowledge(int, long)} instead of using this method directly.
     *
     * @param count the number of FlowFiles to increase the unacknowledged count by and decrement active count by
     * @param bytes the bytes to increase the unacknowledged count by and decrement the active count by
     */
    private void directlyIncrementUnacknowledgedQueueSize(final int count, final long bytes) {
        boolean updated = false;
        while (!updated) {
            final FlowFileQueueSize original = size.get();
            final FlowFileQueueSize newSize = new FlowFileQueueSize(original.getActiveCount(), original.getActiveBytes(),
                original.getSwappedCount(), original.getSwappedBytes(), original.getSwapFileCount(),
                original.getUnacknowledgedCount() + count, original.getUnacknowledgedBytes() + bytes);

            updated = updateSize(original, newSize);

            if (updated) {
                logIfNegative(original, newSize, "Unacknowledged");
            }
        }
    }

    private void logIfNegative(final FlowFileQueueSize original, final FlowFileQueueSize newSize, final String counterName) {
        if (newSize.getActiveBytes() < 0 || newSize.getActiveCount() < 0
            || newSize.getSwappedBytes() < 0 || newSize.getSwappedCount() < 0
            || newSize.getUnacknowledgedBytes() < 0 || newSize.getUnacknowledgedCount() < 0) {

            logger.error("Updated Size of Queue {} from {} to {}", counterName, original, newSize, new RuntimeException("Cannot create negative queue size"));
        }
    }


    protected boolean updateSize(final FlowFileQueueSize expected, final FlowFileQueueSize updated) {
        return size.compareAndSet(expected, updated);
    }

    public FlowFileQueueSize getFlowFileQueueSize() {
        return size.get();
    }

    public void inheritQueueContents(final FlowFileQueueContents queueContents) {
        writeLock.lock();
        try {
            putAll(queueContents.getActiveFlowFiles());

            final List<String> inheritedSwapLocations = queueContents.getSwapLocations();
            swapLocations.addAll(inheritedSwapLocations);
            incrementSwapQueueSize(queueContents.getSwapSize().getObjectCount(), queueContents.getSwapSize().getByteCount(), queueContents.getSwapLocations().size());

            if (!inheritedSwapLocations.isEmpty()) {
                logger.debug("Inherited the following swap locations: {}", inheritedSwapLocations);
            }
        } finally {
            writeLock.unlock("inheritQueueContents");
        }
    }

    public FlowFileQueueContents packageForRebalance(final String newPartitionName) {
        writeLock.lock();
        try {
            final List<FlowFileRecord> activeRecords = new ArrayList<>(this.activeQueue);

            final List<String> updatedSwapLocations = new ArrayList<>(swapLocations.size());
            for (final String swapLocation : swapLocations) {
                try {
                    final String updatedSwapLocation = swapManager.changePartitionName(swapLocation, newPartitionName);
                    updatedSwapLocations.add(updatedSwapLocation);
                } catch (final IOException ioe) {
                    logger.error("Failed to update Swap File {} to reflect that the contents are now owned by Partition '{}'", swapLocation, newPartitionName, ioe);
                }
            }

            this.swapLocations.clear();
            this.activeQueue.clear();

            final int swapQueueCount = swapQueue.size();
            final long swapQueueBytes = swapQueue.stream().mapToLong(FlowFileRecord::getSize).sum();
            activeRecords.addAll(swapQueue);
            swapQueue.clear();

            this.swapMode = false;

            QueueSize swapSize;
            boolean updated;
            do {
                final FlowFileQueueSize currentSize = getFlowFileQueueSize();
                swapSize = new QueueSize(currentSize.getSwappedCount() - swapQueueCount, currentSize.getSwappedBytes() - swapQueueBytes);

                final FlowFileQueueSize updatedSize = new FlowFileQueueSize(0, 0, 0, 0, 0, currentSize.getUnacknowledgedCount(), currentSize.getUnacknowledgedBytes());
                updated = updateSize(currentSize, updatedSize);
            } while (!updated);

            logger.debug("Cleared {} to package FlowFile for rebalance to {}", this, newPartitionName);
            return new FlowFileQueueContents(activeRecords, updatedSwapLocations, swapSize);
        } finally {
            writeLock.unlock("packageForRebalance(SwappablePriorityQueue)");
        }
    }

    @Override
    public String toString() {
        return "SwappablePriorityQueue[queueId=" + flowFileQueue.getIdentifier() + ", partition=" + swapPartitionName + "]";
    }
}
