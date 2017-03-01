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

package org.apache.nifi.provenance.store;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.util.Tuple;

/**
 * The EventFileManager is responsible for maintaining locks on Event Files so that we can ensure that no thread deletes
 * an Event File while it is still being read. Without this manager, this could happen, for instance, if the Compression Thread
 * were to compress an Event File, and then delete the original/uncompressed version while a Provenance Query was reading the
 * uncompressed version of the file.
 */
public class EventFileManager {

    private final ConcurrentMap<String, Tuple<ReadWriteLock, Integer>> lockMap = new ConcurrentHashMap<>();

    private String getMapKey(final File file) {
        return LuceneUtil.substringBefore(file.getName(), ".prov");
    }

    private ReadWriteLock updateCount(final File file, final Function<Integer, Integer> update) {
        final String key = getMapKey(file);
        boolean updated = false;

        Tuple<ReadWriteLock, Integer> updatedTuple = null;
        while (!updated) {
            final Tuple<ReadWriteLock, Integer> tuple = lockMap.computeIfAbsent(key, k -> new Tuple<>(new ReentrantReadWriteLock(), 0));
            final Integer updatedCount = update.apply(tuple.getValue());
            updatedTuple = new Tuple<>(tuple.getKey(), updatedCount);
            updated = lockMap.replace(key, tuple, updatedTuple);
        }

        return updatedTuple.getKey();
    }

    private ReadWriteLock incrementCount(final File file) {
        return updateCount(file, val -> val + 1);
    }

    private ReadWriteLock decrementCount(final File file) {
        return updateCount(file, val -> val - 1);
    }


    public void obtainReadLock(final File file) {
        final ReadWriteLock rwLock = incrementCount(file);
        rwLock.readLock().lock();
    }

    public void releaseReadLock(final File file) {
        final ReadWriteLock rwLock = decrementCount(file);
        rwLock.readLock().unlock();
    }

    public void obtainWriteLock(final File file) {
        final ReadWriteLock rwLock = incrementCount(file);
        rwLock.writeLock().lock();
    }

    public void releaseWriteLock(final File file) {
        final String key = getMapKey(file);

        boolean updated = false;
        while (!updated) {
            final Tuple<ReadWriteLock, Integer> tuple = lockMap.get(key);
            if (tuple == null) {
                throw new IllegalMonitorStateException("Lock is not owned");
            }

            // If this is the only reference to the lock, remove it from the map and then unlock.
            if (tuple.getValue() <= 1) {
                updated = lockMap.remove(key, tuple);
                if (updated) {
                    tuple.getKey().writeLock().unlock();
                }
            } else {
                final Tuple<ReadWriteLock, Integer> updatedTuple = new Tuple<>(tuple.getKey(), tuple.getValue() - 1);
                updated = lockMap.replace(key, tuple, updatedTuple);
                if (updated) {
                    tuple.getKey().writeLock().unlock();
                }
            }
        }
    }

}
