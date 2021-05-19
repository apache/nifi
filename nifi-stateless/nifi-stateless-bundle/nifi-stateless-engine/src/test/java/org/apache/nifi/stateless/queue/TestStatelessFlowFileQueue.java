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

package org.apache.nifi.stateless.queue;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestStatelessFlowFileQueue {

    @Test
    public void testAcknowledgment() {
        final StatelessFlowFileQueue queue = new StatelessFlowFileQueue("id");
        assertQueueSize(0, 0, queue.size());

        queue.put(new MockFlowFile(0));
        assertQueueSize(1, 0, queue.size());

        final FlowFileRecord pulled = queue.poll(Collections.emptySet());
        assertNotNull(pulled);
        assertQueueSize(1, 0, queue.size());
        assertFalse(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());

        queue.acknowledge(pulled);
        assertQueueSize(0, 0, queue.size());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPollingForBatch() {
        final StatelessFlowFileQueue queue = new StatelessFlowFileQueue("id");

        for (int i=0; i < 100; i++) {
            queue.put(new MockFlowFile(i));
        }

        assertQueueSize(100, 0, queue.size());

        for (int i=0; i < 10; i++) {
            final List<FlowFileRecord> flowFiles = queue.poll(10, Collections.emptySet());
            assertEquals(10, flowFiles.size());
            assertQueueSize(100 - 10 * i, 0, queue.size());

            queue.acknowledge(flowFiles);
            assertQueueSize(100 - 10 * (i + 1), 0, queue.size());

            int batchIndex = 0;
            for (final FlowFileRecord flowFile : flowFiles) {
                assertEquals(i * 10 + batchIndex, flowFile.getId());
                batchIndex++;
            }
        }

        assertTrue(queue.isEmpty());
        assertNull(queue.poll(Collections.emptySet()));
    }

    @Test
    public void testPollWithFilter() {
        final StatelessFlowFileQueue queue = new StatelessFlowFileQueue("id");

        for (int i=0; i < 100; i++) {
            queue.put(new MockFlowFile(i));
        }

        assertQueueSize(100, 0, queue.size());

        // Poll for a single FlowFile in the middle.
        final List<FlowFileRecord> matching = queue.poll(ff -> ff.getId() == 50 ? FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE : FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE,
            Collections.emptySet());

        assertEquals(1, matching.size());
        assertEquals(50, matching.get(0).getId());
        queue.acknowledge(matching.get(0));
        assertQueueSize(99, 0, queue.size());

        final List<FlowFileRecord> allBut40 = queue.poll(ff -> ff.getId() == 40 ? FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE : FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE,
            Collections.emptySet());
        assertEquals(98, allBut40.size());
        queue.acknowledge(allBut40);
        assertQueueSize(1, 0, queue.size());

        for (final FlowFileRecord flowFile : allBut40) {
            assertNotEquals(40, flowFile.getId());
        }
    }

    private void assertQueueSize(final int flowFileCount, final long byteCount, final QueueSize queueSize) {
        assertEquals(flowFileCount, queueSize.getObjectCount());
        assertEquals(byteCount, queueSize.getByteCount());
    }
}
