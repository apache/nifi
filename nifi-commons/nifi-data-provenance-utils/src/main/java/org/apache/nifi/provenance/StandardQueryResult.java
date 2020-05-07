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
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardQueryResult implements QueryResult, ProgressiveResult {
    private static final Logger logger = LoggerFactory.getLogger(StandardQueryResult.class);

    public static final int TTL = (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
    private final Query query;
    private final long creationNanos;

    private final int numSteps;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    private final Lock writeLock = rwLock.writeLock();
    // guarded by writeLock
    private final SortedSet<ProvenanceEventRecord> matchingRecords = new TreeSet<>(new EventIdComparator());
    private long hitCount = 0L;
    private int numCompletedSteps = 0;
    private Date expirationDate;
    private String error;
    private long queryTime;
    private final Object completionMonitor = new Object();

    private volatile boolean canceled = false;

    public StandardQueryResult(final Query query, final int numSteps) {
        this.query = query;
        this.numSteps = numSteps;
        this.creationNanos = System.nanoTime();

        updateExpiration();
    }

    @Override
    public List<ProvenanceEventRecord> getMatchingEvents() {
        readLock.lock();
        try {
            return new ArrayList<>(matchingRecords);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalHitCount() {
        readLock.lock();
        try {
            // Because we filter the results based on the user's permissions,
            // we don't want to indicate that the total hit count is 1,000+ when we
            // have 0 matching records, for instance. So, if we have fewer matching
            // records than the max specified by the query, it is either the case that
            // we truly don't have enough records to reach the max results, or that
            // the user is not authorized to see some of the results. Either way,
            // we want to report the number of events that we find AND that the user
            // is allowed to see, so we report matching record count, or up to max results.
            if (matchingRecords.size() < query.getMaxResults()) {
                return matchingRecords.size();
            } else {
                return query.getMaxResults();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getQueryTime() {
        return queryTime;
    }

    @Override
    public Date getExpiration() {
        return expirationDate;
    }

    @Override
    public String getError() {
        return error;
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
            return numCompletedSteps >= numSteps || canceled || matchingRecords.size() >= query.getMaxResults();
        } finally {
            readLock.unlock();
        }
    }

    void cancel() {
        this.canceled = true;
    }

    @Override
    public void setError(final String error) {
        writeLock.lock();
        try {
            this.error = error;
            numCompletedSteps++;

            updateExpiration();
            if (numCompletedSteps >= numSteps) {
                final long searchNanos = System.nanoTime() - creationNanos;
                queryTime = TimeUnit.MILLISECONDS.convert(searchNanos, TimeUnit.NANOSECONDS);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void update(final Collection<ProvenanceEventRecord> newEvents, final long totalHits) {
        boolean queryComplete = false;

        writeLock.lock();
        try {
            if (isFinished()) {
                return;
            }

            this.matchingRecords.addAll(newEvents);
            hitCount += totalHits;

            // If we've added more records than the query's max, then remove the trailing elements.
            // We do this, rather than avoiding the addition of the elements because we want to choose
            // the events with the largest ID.
            if (matchingRecords.size() > query.getMaxResults()) {
                final Iterator<ProvenanceEventRecord> itr = matchingRecords.iterator();
                for (int i = 0; i < query.getMaxResults(); i++) {
                    itr.next();
                }

                while (itr.hasNext()) {
                    itr.next();
                    itr.remove();
                }
            }

            numCompletedSteps++;
            updateExpiration();

            if (numCompletedSteps >= numSteps || this.matchingRecords.size() >= query.getMaxResults()) {
                final long searchNanos = System.nanoTime() - creationNanos;
                queryTime = TimeUnit.MILLISECONDS.convert(searchNanos, TimeUnit.NANOSECONDS);
                queryComplete = true;

                if (numCompletedSteps >= numSteps) {
                    logger.info("Completed {} comprised of {} steps in {} millis. Index found {} hits. Read {} events from Event Files.",
                        query, numSteps, queryTime, hitCount, matchingRecords.size());
                } else {
                    logger.info("Completed {} comprised of {} steps in {} millis. Index found {} hits. Read {} events from Event Files. "
                            + "Only completed {} steps because the maximum number of results was reached.",
                        query, numSteps, queryTime, hitCount, matchingRecords.size(), numCompletedSteps);
                }
            }
        } finally {
            writeLock.unlock();
        }

        if (queryComplete) {
            synchronized (completionMonitor) {
                completionMonitor.notifyAll();
            }
        }
    }

    @Override
    public boolean awaitCompletion(final long time, final TimeUnit unit) throws InterruptedException {
        final long finishTime = System.currentTimeMillis() + unit.toMillis(time);
        synchronized (completionMonitor) {
            while (!isFinished()) {
                final long millisToWait = finishTime - System.currentTimeMillis();
                if (millisToWait > 0) {
                    completionMonitor.wait(millisToWait);
                } else {
                    return isFinished();
                }
            }
        }

        return isFinished();
    }

    /**
     * Must be called with write lock!
     */
    private void updateExpiration() {
        expirationDate = new Date(System.currentTimeMillis() + TTL);
    }

    private static class EventIdComparator implements Comparator<ProvenanceEventRecord> {
        @Override
        public int compare(final ProvenanceEventRecord o1, final ProvenanceEventRecord o2) {
            return Long.compare(o2.getEventId(), o1.getEventId());
        }
    }
}
