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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMetricRollingBuffer {
    private static final Set<MetricDescriptor<?>> PROCESSOR_METRICS = Arrays.stream(ProcessorStatusDescriptor.values())
        .map(ProcessorStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());

    @Test
    public void testBufferGrows() {
        final int bufferCapacity = 1000;
        final MetricRollingBuffer buffer = new MetricRollingBuffer(bufferCapacity);

        final long startTime = System.currentTimeMillis();
        final List<Date> timestamps = new ArrayList<>();

        int iterations = 1440;
        for (int i=0; i < iterations; i++) {
            final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(PROCESSOR_METRICS);
            snapshot.setTimestamp(new Date(startTime + i * 1000));
            timestamps.add(snapshot.getTimestamp());

            snapshot.addStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor(), Long.valueOf(i));

            buffer.update(snapshot);
        }

        assertEquals(bufferCapacity, buffer.size());

        final List<StatusSnapshot> snapshots = buffer.getSnapshots(timestamps, true, PROCESSOR_METRICS);
        assertEquals(iterations, snapshots.size());

        final int expectedEmptyCount = iterations - bufferCapacity;
        final long emptyCount = snapshots.stream().filter(snapshot -> snapshot instanceof EmptyStatusSnapshot).count();
        assertEquals(expectedEmptyCount, emptyCount);

        for (int i=0; i < iterations; i++) {
            final StatusSnapshot snapshot = snapshots.get(i);
            if (i < expectedEmptyCount) {
                assertTrue("Snapshot at i=" + i + " is not an EmptyStatusSnapshot", snapshot instanceof EmptyStatusSnapshot);
            } else {
                assertEquals(Long.valueOf(i), snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor()));
                assertFalse(snapshot instanceof EmptyStatusSnapshot);
            }
        }
    }

    @Test
    public void testBufferShrinks() {
        // Cause buffer to grow
        final int bufferCapacity = 1000;
        final MetricRollingBuffer buffer = new MetricRollingBuffer(bufferCapacity);

        final long startTime = System.currentTimeMillis();

        int iterations = 1440;
        for (int i=0; i < iterations; i++) {
            final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(PROCESSOR_METRICS);
            snapshot.setTimestamp(new Date(startTime + i * 1000));

            snapshot.addStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor(), Long.valueOf(i));
            buffer.update(snapshot);
        }

        assertEquals(bufferCapacity, buffer.size());

        // Expire data ensure that the buffer shrinks
        final long lastTimestamp = startTime + 1440 * 1000;
        buffer.expireBefore(new Date(lastTimestamp - 144_001L));
        assertEquals(144, buffer.size());

        buffer.expireBefore(new Date(lastTimestamp - 16_001L));
        assertEquals(16, buffer.size());

        buffer.expireBefore(new Date(lastTimestamp));
        assertEquals(0, buffer.size());

        // Ensure that we can now properly add data again
        long insertStart = lastTimestamp + 10_000L;
        final List<Date> timestamps = new ArrayList<>();
        for (int i=0; i < 4; i++) {
            final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(PROCESSOR_METRICS);
            snapshot.setTimestamp(new Date(insertStart + i * 1000));
            timestamps.add(snapshot.getTimestamp());

            snapshot.addStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor(), Long.valueOf(i));
            buffer.update(snapshot);
        }

        assertEquals(4, buffer.size());
        final List<StatusSnapshot> snapshots = buffer.getSnapshots(timestamps, true, PROCESSOR_METRICS);
        assertEquals(4, snapshots.size());
        for (int i=0; i < 4; i++) {
            final StatusSnapshot snapshot = snapshots.get(i);
            assertEquals(Long.valueOf(i), snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor()));
        }
    }
}
