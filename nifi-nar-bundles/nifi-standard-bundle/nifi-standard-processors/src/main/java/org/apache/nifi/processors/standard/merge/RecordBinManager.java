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

package org.apache.nifi.processors.standard.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.standard.MergeContent;
import org.apache.nifi.processors.standard.MergeRecord;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;

public class RecordBinManager {

    private final ProcessContext context;
    private final ProcessSessionFactory sessionFactory;
    private final ComponentLog logger;
    private final int maxBinCount;

    private final AtomicLong maxBinAgeNanos = new AtomicLong(Long.MAX_VALUE);
    private final Map<String, List<RecordBin>> groupBinMap = new HashMap<>(); // guarded by lock
    private final Lock lock = new ReentrantLock();

    private final AtomicInteger binCount = new AtomicInteger(0);

    public RecordBinManager(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ComponentLog logger) {
        this.context = context;
        this.sessionFactory = sessionFactory;
        this.logger = logger;

        final Integer maxBins = context.getProperty(MergeRecord.MAX_BIN_COUNT).asInteger();
        this.maxBinCount = maxBins == null ? Integer.MAX_VALUE : maxBins.intValue();
    }

    /**
     * Must be called only when there are no active threads modifying the bins.
     */
    public void purge() {
        lock.lock();
        try {
            for (final List<RecordBin> binList : groupBinMap.values()) {
                for (final RecordBin bin : binList) {
                    bin.rollback();
                }
            }
            groupBinMap.clear();
            binCount.set(0);
        } finally {
            lock.unlock();
        }
    }


    public void setMaxBinAge(final Long timePeriod, final TimeUnit timeUnit) {
        if (timePeriod == null) {
            maxBinAgeNanos.set(Long.MAX_VALUE);
        } else {
            maxBinAgeNanos.set(timeUnit.toNanos(timePeriod));
        }
    }


    public int getBinCount() {
        return binCount.get();
    }

    /**
     * Adds the given flowFiles to the first available bin in which it fits for the given group or creates a new bin in the specified group if necessary.
     * <p/>
     *
     * @param groupIdentifier the group to which the flow file belongs; can be null
     * @param flowFile flowFile to bin
     * @param reader RecordReader to use for reading FlowFile
     * @param session the ProcessSession to which the FlowFiles belong
     * @param block if another thread is already writing to the desired bin, passing <code>true</code> for this parameter will block until the other thread(s) have finished so
     *            that the records can still be added to the desired bin. Passing <code>false</code> will result in moving on to another bin.
     *
     * @throws SchemaNotFoundException if unable to find the schema for the record writer
     * @throws MalformedRecordException if unable to read a record
     * @throws IOException if there is an IO problem reading from the stream or writing to the stream
     */
    public void add(final String groupIdentifier, final FlowFile flowFile, final RecordReader reader, final ProcessSession session, final boolean block)
        throws IOException, MalformedRecordException, SchemaNotFoundException {

        final List<RecordBin> currentBins;
        lock.lock();
        try {
            // Create a new List<RecordBin> if none exists for this Group ID. We use a CopyOnWriteArrayList here because
            // we need to traverse the list in a couple of places and just below here, we call bin.offer() (which is very expensive)
            // while traversing the List, so we don't want to do this within a synchronized block. If we end up seeing poor performance
            // from this, we could look at instead using a Synchronized List and instead of calling bin.offer() while iterating allow for some
            // sort of bin.tryLock() and have that lock only if the flowfile should be added. Then if it returns true, we can stop iterating
            // and perform the expensive part and then ensure that we always unlock
            currentBins = groupBinMap.computeIfAbsent(groupIdentifier, grpId -> new CopyOnWriteArrayList<>());
        } finally {
            lock.unlock();
        }

        RecordBin acceptedBin = null;
        for (final RecordBin bin : currentBins) {
            final boolean accepted = bin.offer(flowFile, reader, session, block);

            if (accepted) {
                acceptedBin = bin;
                logger.debug("Transferred id={} to {}", new Object[] {flowFile.getId(), bin});
                break;
            }
        }

        // We have to do this outside of our for-loop above in order to avoid a concurrent modification Exception.
        if (acceptedBin != null) {
            if (acceptedBin.isComplete()) {
                removeBins(groupIdentifier, Collections.singletonList(acceptedBin));
            }

            return;
        }

        // if we've reached this point then we couldn't fit it into any existing bins - gotta make a new one
        final RecordBin bin = new RecordBin(context, sessionFactory.createSession(), logger, createThresholds());
        final boolean binAccepted = bin.offer(flowFile, reader, session, true);
        if (!binAccepted) {
            session.rollback();
            throw new RuntimeException("Attempted to add " + flowFile + " to a new bin but failed. This is unexpected. Will roll back session and try again.");
        }

        logger.debug("Transferred id={} to {}", new Object[] {flowFile.getId(), bin});

        if (!bin.isComplete()) {
            final int updatedBinCount = binCount.incrementAndGet();

            lock.lock();
            try {
                // We have already obtained the list of RecordBins from this Map above. However, we released
                // the lock in order to avoid blocking while writing to a Bin. Because of this, it is possible
                // that another thread may have already come in and removed this List from the Map, if all
                // Bins in the List have been completed. As a result, we must now obtain the write lock again
                // and obtain the List (or a new one), and then update that. This ensures that we never lose
                // track of a Bin. If we don't lose this, we could completely lose a Bin.
                final List<RecordBin> bins = groupBinMap.computeIfAbsent(groupIdentifier, grpId -> new CopyOnWriteArrayList<>());
                bins.add(bin);
            } finally {
                lock.unlock();
            }

            if (updatedBinCount > maxBinCount) {
                completeOldestBin();
            }
        }
    }


    private RecordBinThresholds createThresholds() {
        final int minRecords = context.getProperty(MergeRecord.MIN_RECORDS).asInteger();
        final int maxRecords = context.getProperty(MergeRecord.MAX_RECORDS).asInteger();
        final long minBytes = context.getProperty(MergeRecord.MIN_SIZE).asDataSize(DataUnit.B).longValue();

        final PropertyValue maxSizeValue = context.getProperty(MergeRecord.MAX_SIZE);
        final long maxBytes = maxSizeValue.isSet() ? maxSizeValue.asDataSize(DataUnit.B).longValue() : Long.MAX_VALUE;

        final PropertyValue maxMillisValue = context.getProperty(MergeRecord.MAX_BIN_AGE);
        final String maxBinAge = maxMillisValue.getValue();
        final long maxBinMillis = maxMillisValue.isSet() ? maxMillisValue.asTimePeriod(TimeUnit.MILLISECONDS).longValue() : Long.MAX_VALUE;

        final String recordCountAttribute;
        final String mergeStrategy = context.getProperty(MergeRecord.MERGE_STRATEGY).getValue();
        if (MergeRecord.MERGE_STRATEGY_DEFRAGMENT.getValue().equals(mergeStrategy)) {
            recordCountAttribute = MergeContent.FRAGMENT_COUNT_ATTRIBUTE;
        } else {
            recordCountAttribute = null;
        }

        return new RecordBinThresholds(minRecords, maxRecords, minBytes, maxBytes, maxBinMillis, maxBinAge, recordCountAttribute);
    }


    public void completeOldestBin() throws IOException {
        RecordBin oldestBin = null;

        lock.lock();
        try {
            String oldestBinGroup = null;

            for (final Map.Entry<String, List<RecordBin>> group : groupBinMap.entrySet()) {
                for (final RecordBin bin : group.getValue()) {
                    if (oldestBin == null || bin.isOlderThan(oldestBin)) {
                        oldestBin = bin;
                        oldestBinGroup = group.getKey();
                    }
                }
            }

            if (oldestBin == null) {
                return;
            }

            removeBins(oldestBinGroup, Collections.singletonList(oldestBin));
        } finally {
            lock.unlock();
        }

        logger.debug("Completing Bin " + oldestBin + " because the maximum number of bins has been exceeded");
        oldestBin.complete("Maximum number of bins has been exceeded");
    }


    public void completeExpiredBins() throws IOException {
        final long maxNanos = maxBinAgeNanos.get();
        final Map<String, List<RecordBin>> expiredBinMap = new HashMap<>();

        lock.lock();
        try {
            for (final Map.Entry<String, List<RecordBin>> entry : groupBinMap.entrySet()) {
                final String key = entry.getKey();
                final List<RecordBin> bins = entry.getValue();

                for (final RecordBin bin : bins) {
                    if (bin.isOlderThan(maxNanos, TimeUnit.NANOSECONDS)) {
                        final List<RecordBin> expiredBinsForKey = expiredBinMap.computeIfAbsent(key, ignore -> new ArrayList<>());
                        expiredBinsForKey.add(bin);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        for (final Map.Entry<String, List<RecordBin>> entry : expiredBinMap.entrySet()) {
            final String key = entry.getKey();
            final List<RecordBin> expiredBins = entry.getValue();

            for (final RecordBin bin : expiredBins) {
                logger.debug("Completing Bin {} because it has expired");
                bin.complete("Bin has reached Max Bin Age");
            }

            removeBins(key, expiredBins);
        }
    }

    private void removeBins(final String key, final List<RecordBin> bins) {
        lock.lock();
        try {
            final List<RecordBin> list = groupBinMap.get(key);
            if (list != null) {
                final int initialSize = list.size();
                list.removeAll(bins);

                // Determine how many items were removed from the list and
                // update our binCount to keep track of this.
                final int removedCount = initialSize - list.size();
                binCount.addAndGet(-removedCount);

                if (list.isEmpty()) {
                    groupBinMap.remove(key);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
