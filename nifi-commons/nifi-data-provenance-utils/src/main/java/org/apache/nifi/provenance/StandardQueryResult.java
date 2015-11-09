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
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;

public class StandardQueryResult implements QueryResult {

    public static final int TTL = (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
    private final Query query;
    private final long creationNanos;

    private final int numSteps;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    private final Lock writeLock = rwLock.writeLock();
    // guarded by writeLock
    private final List<ProvenanceEventRecord> matchingRecords = new ArrayList<>();
    private long totalHitCount;
    private int numCompletedSteps = 0;
    private Date expirationDate;
    private String error;
    private long queryTime;

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
            if (matchingRecords.size() <= query.getMaxResults()) {
                return new ArrayList<>(matchingRecords);
            }

            final List<ProvenanceEventRecord> copy = new ArrayList<>(query.getMaxResults());
            for (int i = 0; i < query.getMaxResults(); i++) {
                copy.add(matchingRecords.get(i));
            }

            return copy;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalHitCount() {
        readLock.lock();
        try {
            return totalHitCount;
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
            return numCompletedSteps >= numSteps || canceled;
        } finally {
            readLock.unlock();
        }
    }

    void cancel() {
        this.canceled = true;
    }

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

    public void update(final Collection<ProvenanceEventRecord> matchingRecords, final long totalHits) {
        writeLock.lock();
        try {
            this.matchingRecords.addAll(matchingRecords);
            this.totalHitCount += totalHits;

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

    /**
     * Must be called with write lock!
     */
    private void updateExpiration() {
        expirationDate = new Date(System.currentTimeMillis() + TTL);
    }
}
