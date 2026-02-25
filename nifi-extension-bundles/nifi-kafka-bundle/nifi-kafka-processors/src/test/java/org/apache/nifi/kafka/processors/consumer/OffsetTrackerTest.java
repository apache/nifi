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
package org.apache.nifi.kafka.processors.consumer;

import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetTrackerTest {

    private static final String TOPIC_A = "topicA";
    private static final String TOPIC_B = "topicB";

    @Test
    void testGetTrackedPartitionsEmpty() {
        final OffsetTracker tracker = new OffsetTracker();

        assertTrue(tracker.getTrackedPartitions().isEmpty());
    }

    @Test
    void testGetTrackedPartitionsSingleRecord() {
        final OffsetTracker tracker = new OffsetTracker();
        tracker.update(byteRecord(TOPIC_A, 0, 10));

        final Set<TopicPartitionSummary> partitions = tracker.getTrackedPartitions();

        assertEquals(1, partitions.size());
        assertTrue(partitions.contains(new TopicPartitionSummary(TOPIC_A, 0)));
    }

    @Test
    void testGetTrackedPartitionsMultiplePartitions() {
        final OffsetTracker tracker = new OffsetTracker();
        tracker.update(byteRecord(TOPIC_A, 0, 1));
        tracker.update(byteRecord(TOPIC_A, 1, 5));
        tracker.update(byteRecord(TOPIC_B, 0, 3));

        final Set<TopicPartitionSummary> partitions = tracker.getTrackedPartitions();

        assertEquals(3, partitions.size());
        assertTrue(partitions.contains(new TopicPartitionSummary(TOPIC_A, 0)));
        assertTrue(partitions.contains(new TopicPartitionSummary(TOPIC_A, 1)));
        assertTrue(partitions.contains(new TopicPartitionSummary(TOPIC_B, 0)));
    }

    @Test
    void testGetTrackedPartitionsDeduplicated() {
        final OffsetTracker tracker = new OffsetTracker();
        tracker.update(byteRecord(TOPIC_A, 0, 1));
        tracker.update(byteRecord(TOPIC_A, 0, 2));
        tracker.update(byteRecord(TOPIC_A, 0, 3));

        final Set<TopicPartitionSummary> partitions = tracker.getTrackedPartitions();

        assertEquals(1, partitions.size());
        assertTrue(partitions.contains(new TopicPartitionSummary(TOPIC_A, 0)));
    }

    private static ByteRecord byteRecord(final String topic, final int partition, final long offset) {
        return new ByteRecord(topic, partition, offset, System.currentTimeMillis(),
                Collections.emptyList(), null, new byte[0], 1);
    }
}
