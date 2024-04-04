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
package org.apache.nifi.controller.status.history;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class MetricRollingBuffer {
    private final int capacity;

    private StatusSnapshot[] snapshots;
    private int writeIndex = 0;
    private int readIndex;
    private boolean readExhausted;
    private int count = 0;

    public MetricRollingBuffer(final int maxCapacity) {
        this.capacity = maxCapacity;
    }

    public void update(final StatusSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }

        if (snapshots == null) {
            snapshots = new StatusSnapshot[Math.min(capacity, 16)];
        }

        if (snapshots[writeIndex] == null) {
            count++;
        }

        snapshots[writeIndex++] = snapshot;

        if (writeIndex >= snapshots.length) {
            if (snapshots.length < capacity) {
                grow();
            } else {
                writeIndex = 0;
            }
        }
    }

    public int size() {
        return count;
    }

    public void expireBefore(final Date date) {
        if (snapshots == null) {
            return;
        }

        int readIndex = writeIndex;
        for (int i=0; i < snapshots.length; i++) {
            final StatusSnapshot snapshot = snapshots[readIndex];
            if (snapshot == null) {
                readIndex++;
                if (readIndex >= snapshots.length) {
                    readIndex = 0;
                }

                continue;
            }

            final Date snapshotTimestamp = snapshot.getTimestamp();
            if (snapshotTimestamp.after(date)) {
                break;
            }

            snapshots[readIndex] = null;
            count--;

            readIndex++;
            if (readIndex >= snapshots.length) {
                readIndex = 0;
            }
        }

        if (count < snapshots.length / 4 || snapshots.length - count > 128) {
            // If we're using less than 1/4 of the array or we have at least 128 null entries, compact.
            compact();
        }
    }

    private void grow() {
        final int initialSize = snapshots.length;
        final int newSize = Math.min(capacity, snapshots.length + 64);
        final StatusSnapshot[] newArray = new StatusSnapshot[newSize];
        System.arraycopy(snapshots, 0, newArray, 0, snapshots.length);
        snapshots = newArray;
        writeIndex = initialSize;
    }

    private void compact() {
        final StatusSnapshot[] newArray = new StatusSnapshot[count + 1];
        int insertionIndex = 0;

        int readIndex = writeIndex;
        for (int i=0; i < snapshots.length; i++) {
            final StatusSnapshot snapshot = snapshots[readIndex];
            if (snapshot != null) {
                newArray[insertionIndex++] = snapshot;
            }

            readIndex++;
            if (readIndex >= snapshots.length) {
                readIndex = 0;
            }
        }

        snapshots = newArray;
        writeIndex = count;
        count = newArray.length - 1;
    }

    public List<StatusSnapshot> getSnapshots(final List<Date> timestamps, final boolean includeCounters, final Set<MetricDescriptor<?>> defaultStatusMetrics) {
        if (snapshots == null) {
            return Collections.emptyList();
        }

        final List<StatusSnapshot> list = new ArrayList<>(snapshots.length);

        resetRead();

        for (final Date timestamp : timestamps) {
            final StatusSnapshot snapshot = getSnapshotForTimestamp(timestamp);
            if (snapshot == null) {
                list.add(new EmptyStatusSnapshot(timestamp, defaultStatusMetrics));
            } else {
                list.add(includeCounters ? snapshot : snapshot.withoutCounters());
            }
        }

        return list;
    }

    private StatusSnapshot getSnapshotForTimestamp(final Date timestamp) {
        while (!readExhausted) {
            final StatusSnapshot snapshot = snapshots[readIndex];
            if (snapshot == null) {
                advanceRead();
                continue;
            }

            final Date snapshotTimestamp = snapshot.getTimestamp();
            if (snapshotTimestamp.before(timestamp)) {
                advanceRead();
                continue;
            }

            if (snapshotTimestamp.after(timestamp)) {
                return null;
            }

            advanceRead();
            return snapshot;
        }

        return null;
    }

    private void resetRead() {
        readIndex = writeIndex;
        readExhausted = false;
    }

    private void advanceRead() {
        readIndex++;

        if (readIndex >= snapshots.length) {
            readIndex = 0;
        }

        if (readIndex == writeIndex) {
            readExhausted = true;
        }
    }
}
