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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.kafka.pubsub.ConsumerLease;
import org.apache.nifi.processors.kafka.pubsub.ConsumerPool;
import org.apache.nifi.processors.kafka.pubsub.ConsumerPool.PoolStats;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerPoolTest {

    private Consumer<byte[], byte[]> consumer = null;
    private ProcessSession mockSession = null;
    private ProcessContext mockContext = Mockito.mock(ProcessContext.class);
    private ProvenanceReporter mockReporter = null;
    private ConsumerPool testPool = null;
    private ConsumerPool testDemarcatedPool = null;
    private ComponentLog logger = null;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        consumer = mock(Consumer.class);
        logger = mock(ComponentLog.class);
        mockSession = mock(ProcessSession.class);
        mockReporter = mock(ProvenanceReporter.class);
        when(mockSession.getProvenanceReporter()).thenReturn(mockReporter);
        testPool = new ConsumerPool(
                1,
                null,
                Collections.emptyMap(),
                Collections.singletonList("nifi"),
                100L,
                "utf-8",
                "ssl",
                "localhost",
                logger,
                true,
                StandardCharsets.UTF_8,
                null) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return consumer;
            }
        };
        testDemarcatedPool = new ConsumerPool(
                1,
                "--demarcator--".getBytes(StandardCharsets.UTF_8),
                Collections.emptyMap(),
                Collections.singletonList("nifi"),
                100L,
                "utf-8",
                "ssl",
                "localhost",
                logger,
                true,
                StandardCharsets.UTF_8,
                Pattern.compile(".*")) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return consumer;
            }
        };
    }

    @Test
    public void validatePoolSimpleCreateClose() throws Exception {

        when(consumer.poll(anyLong())).thenReturn(createConsumerRecords("nifi", 0, 0L, new byte[][]{}));
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
        }
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
        }
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
        }
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
        }
        testPool.close();
        verify(mockSession, times(0)).create();
        verify(mockSession, times(0)).commit();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(4, stats.leasesObtainedCount);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void validatePoolSimpleCreatePollClose() throws Exception {
        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> firstRecs = createConsumerRecords("foo", 1, 1L, firstPassValues);

        when(consumer.poll(anyLong())).thenReturn(firstRecs, createConsumerRecords("nifi", 0, 0L, new byte[][]{}));
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
            lease.commit();
        }
        testPool.close();
        verify(mockSession, times(3)).create();
        verify(mockSession, times(1)).commit();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(1, stats.leasesObtainedCount);
    }

    @Test
    public void validatePoolSimpleBatchCreateClose() throws Exception {
        when(consumer.poll(anyLong())).thenReturn(createConsumerRecords("nifi", 0, 0L, new byte[][]{}));
        for (int i = 0; i < 100; i++) {
            try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
                for (int j = 0; j < 100; j++) {
                    lease.poll();
                }
            }
        }
        testPool.close();
        verify(mockSession, times(0)).create();
        verify(mockSession, times(0)).commit();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(100, stats.leasesObtainedCount);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void validatePoolBatchCreatePollClose() throws Exception {
        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> firstRecs = createConsumerRecords("foo", 1, 1L, firstPassValues);

        when(consumer.poll(anyLong())).thenReturn(firstRecs, createConsumerRecords("nifi", 0, 0L, new byte[][]{}));
        try (final ConsumerLease lease = testDemarcatedPool.obtainConsumer(mockSession, mockContext)) {
            lease.poll();
            lease.commit();
        }
        testDemarcatedPool.close();
        verify(mockSession, times(1)).create();
        verify(mockSession, times(1)).commit();
        final PoolStats stats = testDemarcatedPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(1, stats.leasesObtainedCount);
    }

    @Test
    public void validatePoolConsumerFails() throws Exception {

        when(consumer.poll(anyLong())).thenThrow(new KafkaException("oops"));
        try (final ConsumerLease lease = testPool.obtainConsumer(mockSession, mockContext)) {
            try {
                lease.poll();
                fail();
            } catch (final KafkaException ke) {

            }
        }
        testPool.close();
        verify(mockSession, times(0)).create();
        verify(mockSession, times(0)).commit();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(1, stats.leasesObtainedCount);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static ConsumerRecords<byte[], byte[]> createConsumerRecords(final String topic, final int partition, final long startingOffset, final byte[][] rawRecords) {
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        final TopicPartition tPart = new TopicPartition(topic, partition);
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        long offset = startingOffset;
        for (final byte[] rawRecord : rawRecords) {
            final ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord(topic, partition, offset++, UUID.randomUUID().toString().getBytes(), rawRecord);
            records.add(rec);
        }
        map.put(tPart, records);
        return new ConsumerRecords(map);
    }

}
