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
package org.apache.nifi.distributed.cache.server;

import java.util.Comparator;

public enum EvictionPolicy {

    LFU(new LFUComparator()),
    LRU(new LRUComparator()),
    FIFO(new FIFOComparator());

    private final Comparator<CacheRecord> comparator;

    private EvictionPolicy(final Comparator<CacheRecord> comparator) {
        this.comparator = comparator;
    }

    public Comparator<CacheRecord> getComparator() {
        return comparator;
    }

    public static class LFUComparator implements Comparator<CacheRecord> {

        @Override
        public int compare(final CacheRecord o1, final CacheRecord o2) {
            if (o1.equals(o2)) {
                return 0;
            }

            final int hitCountComparison = Integer.compare(o1.getHitCount(), o2.getHitCount());
            final int entryDateComparison = (hitCountComparison == 0) ? Long.compare(o1.getEntryDate(), o2.getEntryDate()) : hitCountComparison;
            return (entryDateComparison == 0 ? Long.compare(o1.getId(), o2.getId()) : entryDateComparison);
        }
    }

    public static class LRUComparator implements Comparator<CacheRecord> {

        @Override
        public int compare(final CacheRecord o1, final CacheRecord o2) {
            if (o1.equals(o2)) {
                return 0;
            }

            final int lastHitDateComparison = Long.compare(o1.getLastHitDate(), o2.getLastHitDate());
            return (lastHitDateComparison == 0 ? Long.compare(o1.getId(), o2.getId()) : lastHitDateComparison);
        }
    }

    public static class FIFOComparator implements Comparator<CacheRecord> {

        @Override
        public int compare(final CacheRecord o1, final CacheRecord o2) {
            if (o1.equals(o2)) {
                return 0;
            }

            final int entryDateComparison = Long.compare(o1.getEntryDate(), o2.getEntryDate());
            return (entryDateComparison == 0 ? Long.compare(o1.getId(), o2.getId()) : entryDateComparison);
        }
    }

}
