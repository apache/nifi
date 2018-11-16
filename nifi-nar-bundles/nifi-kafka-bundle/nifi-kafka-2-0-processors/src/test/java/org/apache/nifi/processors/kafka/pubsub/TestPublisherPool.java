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

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestPublisherPool {

    @Test
    public void testLeaseCloseReturnsToPool() {
        final Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put("bootstrap.servers", "localhost:1111");
        kafkaProperties.put("key.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.put("value.serializer", ByteArraySerializer.class.getName());

        final PublisherPool pool = new PublisherPool(kafkaProperties, Mockito.mock(ComponentLog.class), 1024 * 1024, 1000L, false, null, StandardCharsets.UTF_8);
        assertEquals(0, pool.available());

        final PublisherLease lease = pool.obtainPublisher();
        assertEquals(0, pool.available());

        lease.close();
        assertEquals(1, pool.available());
    }

    @Test
    public void testPoisonedLeaseNotReturnedToPool() {
        final Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put("bootstrap.servers", "localhost:1111");
        kafkaProperties.put("key.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.put("value.serializer", ByteArraySerializer.class.getName());

        final PublisherPool pool = new PublisherPool(kafkaProperties, Mockito.mock(ComponentLog.class), 1024 * 1024, 1000L, false, null, StandardCharsets.UTF_8);
        assertEquals(0, pool.available());

        final PublisherLease lease = pool.obtainPublisher();
        assertEquals(0, pool.available());

        lease.poison();
        lease.close();
        assertEquals(0, pool.available());
    }

}
