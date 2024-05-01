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
package org.apache.nifi.kafka.service.api.producer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PublishContextTest {

    @Test
    void testCtorAndAccessors() {
        final String topic = "topic";
        final int partition = 0;
        final long timestamp = System.currentTimeMillis();
        final PublishContext publishContext = new PublishContext(topic, partition, timestamp, null);
        assertEquals(topic, publishContext.getTopic());
        assertEquals(partition, publishContext.getPartition());
        assertEquals(timestamp, publishContext.getTimestamp());

        assertNull(publishContext.getException());
        final IOException e = new IOException();
        publishContext.setException(e);
        assertEquals(e, publishContext.getException());
    }
}
