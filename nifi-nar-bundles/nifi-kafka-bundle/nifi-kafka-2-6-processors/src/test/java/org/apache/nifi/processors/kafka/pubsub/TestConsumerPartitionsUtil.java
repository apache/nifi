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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestConsumerPartitionsUtil {
    private final ComponentLog logger = new MockComponentLogger();
    private String hostname;

    @Before
    public void setup() throws UnknownHostException {
        hostname = InetAddress.getLocalHost().getHostName();;
    }

    @Test
    public void testNoPartitionAssignments() throws UnknownHostException {
        final Map<String, String> properties = Collections.singletonMap("key", "value");
        final int[] partitions = ConsumerPartitionsUtil.getPartitionsForHost(properties, logger);
        assertNull(partitions);
    }

    @Test
    public void testAllPartitionsAssignedToOneHost() throws UnknownHostException {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        properties.put("partitions." + hostname, "0, 1, 2, 3");
        final int[] partitions = ConsumerPartitionsUtil.getPartitionsForHost(properties, logger);
        assertNotNull(partitions);

        assertArrayEquals(new int[] {0, 1, 2, 3}, partitions);
    }

    @Test
    public void testSomePartitionsSkipped() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        properties.put("partitions." + hostname, "0, 1, 2, 3, 5");
        final ValidationResult invalidResult = ConsumerPartitionsUtil.validateConsumePartitions(properties);
        assertNotNull(invalidResult);
        assertFalse(invalidResult.isValid());

        properties.put("partitions." + hostname, "0, 1,2,3,4, 5");
        final ValidationResult validResult = ConsumerPartitionsUtil.validateConsumePartitions(properties);
        assertNotNull(validResult);
        assertTrue(validResult.isValid());
    }

    @Test
    public void testCurrentNodeNotSpecified() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        properties.put("partitions.other-host", "0, 1, 2, 3");

        final ValidationResult invalidResult = ConsumerPartitionsUtil.validateConsumePartitions(properties);
        assertNotNull(invalidResult);
        assertFalse(invalidResult.isValid());
    }

    @Test
    public void testPartitionListedTwice() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        properties.put("partitions." + hostname, "2");
        properties.put("partitions.other-host", "0, 1, 2, 3");

        final ValidationResult invalidResult = ConsumerPartitionsUtil.validateConsumePartitions(properties);
        assertNotNull(invalidResult);
        assertFalse(invalidResult.isValid());
    }

    @Test
    public void testNodeWithNoAssignment() throws UnknownHostException {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        properties.put("partitions." + hostname, "");
        properties.put("partitions.other-host", "0, 1, 2, 3");

        final ValidationResult invalidResult = ConsumerPartitionsUtil.validateConsumePartitions(properties);
        assertNotNull(invalidResult);
        assertTrue(invalidResult.isValid());

        final int[] partitions = ConsumerPartitionsUtil.getPartitionsForHost(properties, logger);
        assertNotNull(partitions);
        assertEquals(0, partitions.length);
    }

}
