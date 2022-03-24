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
package org.apache.nifi.integration.lifecycle;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.Bulletin;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertNotNull;

public class FlowFileRepositoryLifecycleIT extends FrameworkIntegrationTest {

    @Test
    public void testFlowFilesReloadedIntoQueuesUponRestart() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode procNode = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                flowFile = session.create();
            }

            flowFile = session.putAttribute(flowFile,"creator", "unit-test");
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(procNode, getNopProcessor(), REL_SUCCESS);
        start(procNode);
        final FlowFileQueue queue = getDestinationQueue(procNode, REL_SUCCESS);

        while (queue.isEmpty()) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
            }
        }

        stop(procNode).get();

        final int queueSize = queue.size().getObjectCount();
        assertTrue(queueSize > 0);
        assertEquals(0L, queue.size().getByteCount());

        assertProvenanceEventCount(ProvenanceEventType.CREATE, queueSize);

        shutdown();

        final FlowFileQueue restoredQueue = createFlowFileQueue(queue.getIdentifier(), getRootGroup());
        initialize();
        getFlowController().initializeFlow(() -> Collections.singleton(restoredQueue));

        for (int i=0; i < queueSize; i++) {
            final FlowFileRecord flowFileRecord = restoredQueue.poll(Collections.emptySet());
            assertNotNull(flowFileRecord);
            assertEquals("unit-test", flowFileRecord.getAttribute("creator"));
        }

        assertFalse(restoredQueue.isEmpty());
        assertTrue(restoredQueue.isActiveQueueEmpty());
        assertNull(restoredQueue.poll(Collections.emptySet()));
    }


    @Test
    public void testMissingSwapFileOnSwapIn() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createProcessorNode((context, session) -> {
            for (int i=0; i < 30_000; i++) {
                session.transfer(session.create(), REL_SUCCESS);
            }
        }, REL_SUCCESS);

        connect(createProcessor, getTerminateAllProcessor(), REL_SUCCESS);
        triggerOnce(createProcessor);

        // Verify queue sizes
        final FlowFileQueue flowFileQueue = getDestinationQueue(createProcessor, REL_SUCCESS);
        assertEquals(30_000, flowFileQueue.size().getObjectCount());
        assertEquals(10_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapQueueSize().getObjectCount());
        assertEquals(1, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapFileCount());
        assertEquals(20_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getActiveQueueSize().getObjectCount());

        // Delete all swap files
        final FlowFileSwapManager swapManager = getFlowController().createSwapManager();
        swapManager.purge();

        // Verify queue sizes haven't changed
        assertEquals(30_000, flowFileQueue.size().getObjectCount());
        assertEquals(10_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapQueueSize().getObjectCount());
        assertEquals(1, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapFileCount());
        assertEquals(20_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getActiveQueueSize().getObjectCount());

        for (int i=0; i < 5; i++) {
            triggerOnce(getTerminateAllProcessor());

            // Verify new queue sizes
            assertEquals(10_000, flowFileQueue.size().getObjectCount());
            assertEquals(10_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapQueueSize().getObjectCount());
            assertEquals(1, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapFileCount());
            assertEquals(0, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getActiveQueueSize().getObjectCount());

            final List<Bulletin> bulletins = getFlowController().getBulletinRepository().findBulletinsForController();
            assertEquals(1, bulletins.size());
        }

        assertProvenanceEventCount(ProvenanceEventType.CREATE, 30_000);
        assertProvenanceEventCount(ProvenanceEventType.DROP, 20_000);

        final DropFlowFileStatus status = flowFileQueue.dropFlowFiles("unit-test-id-1", "unit-test");
        while (status.getState() != DropFlowFileState.COMPLETE) {
            Thread.sleep(10L);
        }

        assertEquals(0, status.getDroppedSize().getObjectCount());
        assertEquals(10_000, flowFileQueue.size().getObjectCount());
        assertEquals(10_000, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapQueueSize().getObjectCount());
        assertEquals(1, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getSwapFileCount());
        assertEquals(0, flowFileQueue.getQueueDiagnostics().getLocalQueuePartitionDiagnostics().getActiveQueueSize().getObjectCount());
    }
}
