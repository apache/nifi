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

import java.util.Collections;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.kafka.pubsub.ConsumerPool.PoolStats;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerPoolTest {

    Consumer<byte[], byte[]> consumer = null;
    ComponentLog logger = null;

    @Before
    public void setup() {
        consumer = mock(Consumer.class);
        logger = mock(ComponentLog.class);
    }

    @Test
    public void validatePoolSimpleCreateClose() throws Exception {

        final ConsumerPool testPool = new ConsumerPool(1, Collections.singletonList("nifi"), Collections.emptyMap(), logger) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return consumer;
            }
        };

        when(consumer.poll(anyInt())).thenReturn(ConsumerRecords.empty());

        try (final ConsumerLease lease = testPool.obtainConsumer()) {
            lease.poll();
            lease.commitOffsets(Collections.emptyMap());
        }
        testPool.close();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(1, stats.leasesObtainedCount);
        assertEquals(1, stats.unproductivePollCount);
        assertEquals(0, stats.productivePollCount);
    }

    @Test
    public void validatePoolSimpleBatchCreateClose() throws Exception {

        final ConsumerPool testPool = new ConsumerPool(5, Collections.singletonList("nifi"), Collections.emptyMap(), logger) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return consumer;
            }
        };

        when(consumer.poll(anyInt())).thenReturn(ConsumerRecords.empty());

        for (int i = 0; i < 100; i++) {
            try (final ConsumerLease lease = testPool.obtainConsumer()) {
                for (int j = 0; j < 100; j++) {
                    lease.poll();
                }
                lease.commitOffsets(Collections.emptyMap());
            }
        }
        testPool.close();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(100, stats.leasesObtainedCount);
        assertEquals(10000, stats.unproductivePollCount);
        assertEquals(0, stats.productivePollCount);
    }

    @Test
    public void validatePoolConsumerFails() throws Exception {

        final ConsumerPool testPool = new ConsumerPool(1, Collections.singletonList("nifi"), Collections.emptyMap(), logger) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return consumer;
            }
        };

        when(consumer.poll(anyInt())).thenThrow(new KafkaException());

        try (final ConsumerLease lease = testPool.obtainConsumer()) {
            lease.poll();
            fail();
        } catch (final KafkaException ke) {

        }
        testPool.close();
        final PoolStats stats = testPool.getPoolStats();
        assertEquals(1, stats.consumerCreatedCount);
        assertEquals(1, stats.consumerClosedCount);
        assertEquals(1, stats.leasesObtainedCount);
        assertEquals(0, stats.unproductivePollCount);
        assertEquals(0, stats.productivePollCount);
    }
}
