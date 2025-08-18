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
package org.apache.nifi.processor.util.bin;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * This class is thread safe
 *
 */
public class BinManager {

    private final AtomicLong minSizeBytes = new AtomicLong(0L);
    private final AtomicLong maxSizeBytes = new AtomicLong(Long.MAX_VALUE);
    private final AtomicInteger minEntries = new AtomicInteger(0);
    private final AtomicInteger maxEntries = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicReference<String> fileCountAttribute = new AtomicReference<>(null);
    private volatile Predicate<FlowFile> binTerminationCheck = ff -> false;
    private volatile InsertionLocation insertionLocation = InsertionLocation.LAST_IN_BIN;

    private final AtomicInteger maxBinAgeSeconds = new AtomicInteger(Integer.MAX_VALUE);
    private final Map<String, List<Bin>> groupBinMap = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    private int binCount = 0;   // guarded by read/write lock

    public void purge() {
        wLock.lock();
        try {
            for (final List<Bin> binList : groupBinMap.values()) {
                for (final Bin bin : binList) {
                    bin.getSession().rollback();
                }
            }
            groupBinMap.clear();
            binCount = 0;
        } finally {
            wLock.unlock();
        }
    }

    public void setFileCountAttribute(final String fileCountAttribute) {
        this.fileCountAttribute.set(fileCountAttribute);
    }

    public String getFileCountAttribute() {
        return fileCountAttribute.get();
    }

    public void setMinimumEntries(final int minimumEntries) {
        this.minEntries.set(minimumEntries);
    }

    public void setMaximumEntries(final int maximumEntries) {
        this.maxEntries.set(maximumEntries);
    }

    /**
     * Sets the predicate that determines whether or not a FlowFile should terminate a bin and the location in the bin where the FlowFile should be inserted if so.
     * @param binTerminationCheck the predicate to use to determine if a FlowFile should terminate a bin
     * @param insertionLocation the location in the bin where the FlowFile should be inserted if it terminates the bin
     */
    public void setBinTermination(final Predicate<FlowFile> binTerminationCheck, final InsertionLocation insertionLocation) {
        this.binTerminationCheck = binTerminationCheck;
        this.insertionLocation = insertionLocation;
    }

    public int getBinCount() {
        rLock.lock();
        try {
            return binCount;
        } finally {
            rLock.unlock();
        }
    }

    public void setMinimumSize(final long numBytes) {
        minSizeBytes.set(numBytes);
    }

    public void setMaximumSize(final long numBytes) {
        maxSizeBytes.set(numBytes);
    }

    public void setMaxBinAge(final int seconds) {
        maxBinAgeSeconds.set(seconds);
    }

    /**
     * Adds the given flowFile to the first available bin in which it fits for the given group or creates a new bin in the specified group if necessary.
     * <p/>
     *
     * @param groupIdentifier the group to which the flow file belongs; can be null
     * @param flowFile the flow file to bin
     * @param session the ProcessSession to which the FlowFile belongs
     * @param sessionFactory a ProcessSessionFactory that can be used to create a new ProcessSession in order to
     *            create a new bin if necessary
     * @return true if added; false if no bin exists which can fit this item and no bin can be created based on current min/max criteria
     */
    public boolean offer(final String groupIdentifier, final FlowFile flowFile, final ProcessSession session, final ProcessSessionFactory sessionFactory) {
        final Set<FlowFile> unbinned = offer(groupIdentifier, List.of(flowFile), session, sessionFactory);
        return unbinned.isEmpty();
    }

    /**
     * Adds the given flowFiles to the first available bin in which it fits for the given group or creates a new bin in the specified group if necessary.
     * <p/>
     *
     * @param groupIdentifier the group to which the flow file belongs; can be null
     * @param flowFiles the flow files to bin
     * @param session the ProcessSession to which the FlowFiles belong
     * @param sessionFactory a ProcessSessionFactory that can be used to create a new ProcessSession in order to
     *            create a new bin if necessary
     * @return all of the FlowFiles that could not be successfully binned
     */
    public Set<FlowFile> offer(final String groupIdentifier, final Collection<FlowFile> flowFiles, final ProcessSession session, final ProcessSessionFactory sessionFactory) {
        final long currentMaxSizeBytes = maxSizeBytes.get();
        final Set<FlowFile> unbinned = new HashSet<>();

        wLock.lock();
        try {
            flowFileLoop: for (final FlowFile flowFile : flowFiles) {
                if (flowFile.getSize() > currentMaxSizeBytes) { //won't fit into any new bins (and probably none existing)
                    unbinned.add(flowFile);
                    continue;
                }

                final boolean terminatesBin = binTerminationCheck != null && binTerminationCheck.test(flowFile);

                final List<Bin> currentBins = groupBinMap.computeIfAbsent(groupIdentifier, k -> new ArrayList<>());
                if (terminatesBin) {
                    if (insertionLocation == InsertionLocation.LAST_IN_BIN) {
                        for (final Bin bin : currentBins) {
                            final boolean accepted = bin.offer(flowFile, session);
                            if (accepted) {
                                bin.complete();
                                bin.setEvictionReason(EvictionReason.BIN_TERMINATION_SIGNAL);

                                continue flowFileLoop;
                            }
                        }
                    } else if (!currentBins.isEmpty()) {
                        for (final Bin bin : currentBins) {
                            if (bin.isForcefullyCompleted()) {
                                continue;
                            }

                            bin.complete();
                            bin.setEvictionReason(EvictionReason.BIN_TERMINATION_SIGNAL);
                            break;

                            // Note that we intentionally do not continue the flowFileLoop here because we want to create a new bin
                        }
                    }
                } else {
                    for (final Bin bin : currentBins) {
                        final boolean accepted = bin.offer(flowFile, session);
                        if (accepted) {
                            continue flowFileLoop;
                        }
                    }
                }

                // if we've reached this point then the groupIdentifier was a brand new one,
                // or we couldn't fit it into any existing bins - gotta make a new one
                final Bin bin = new Bin(sessionFactory.createSession(), minSizeBytes.get(), currentMaxSizeBytes, minEntries.get(),
                    maxEntries.get(), fileCountAttribute.get());
                currentBins.add(bin);
                binCount++;
                final boolean added = bin.offer(flowFile, session);
                if (added) {
                    if (terminatesBin && (insertionLocation == InsertionLocation.ISOLATED || insertionLocation == InsertionLocation.LAST_IN_BIN)) {
                        bin.complete();
                        bin.setEvictionReason(EvictionReason.BIN_TERMINATION_SIGNAL);
                    }
                } else {
                    unbinned.add(flowFile);
                }

            }
        } finally {
            wLock.unlock();
        }

        return unbinned;
    }

    /**
     * Finds all bins that are considered full and removes them from the manager.
     * <p/>
     * @param relaxFullnessConstraint if false will require bins to be full before considered ready; if true bins only have to meet their minimum size criteria. It does not affect the 'old' criteria,
     *                                so bins that are older than maxBinAgeSeconds will always be considered ready regardless of this parameter.
     * @return bins that are considered full
     */
    public Collection<Bin> removeReadyBins(boolean relaxFullnessConstraint) {
        final Map<String, List<Bin>> newGroupMap = new HashMap<>();
        final List<Bin> readyBins = new ArrayList<>();

        wLock.lock();
        try {
            for (final Map.Entry<String, List<Bin>> group : groupBinMap.entrySet()) {
                final List<Bin> remainingBins = new ArrayList<>();
                for (final Bin bin : group.getValue()) {
                    if (relaxFullnessConstraint && bin.isFullEnough()) {
                        bin.setEvictionReason(bin.determineEvictionReason());
                        readyBins.add(bin);
                    } else if (!relaxFullnessConstraint && bin.isFull()) { //strict check
                        bin.setEvictionReason(bin.determineEvictionReason());
                        readyBins.add(bin);
                    } else if (bin.isOlderThan(maxBinAgeSeconds.get(), TimeUnit.SECONDS)) {
                        bin.setEvictionReason(EvictionReason.TIMEOUT);
                        readyBins.add(bin);
                    } else { //it isn't time yet...
                        remainingBins.add(bin);
                    }
                }
                if (!remainingBins.isEmpty()) {
                    newGroupMap.put(group.getKey(), remainingBins);
                }
            }
            groupBinMap.clear();
            groupBinMap.putAll(newGroupMap);
            binCount -= readyBins.size();
        } finally {
            wLock.unlock();
        }
        return readyBins;
    }

    public Bin removeOldestBin() {
        wLock.lock();
        try {
            Bin oldestBin = null;
            String oldestBinGroup = null;

            for (final Map.Entry<String, List<Bin>> group : groupBinMap.entrySet()) {
                for (final Bin bin : group.getValue()) {
                    if (oldestBin == null || bin.isOlderThan(oldestBin)) {
                        oldestBin = bin;
                        oldestBinGroup = group.getKey();
                    }
                }
            }

            if (oldestBin == null) {
                return null;
            }

            binCount--;
            final List<Bin> bins = groupBinMap.get(oldestBinGroup);
            bins.remove(oldestBin);
            if (bins.isEmpty()) {
                groupBinMap.remove(oldestBinGroup);
            }
            return oldestBin;
        } finally {
            wLock.unlock();
        }
    }

}
