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
package org.apache.nifi.integration.swap;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.integration.processors.GenerateProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class StandaloneSwapFileIT extends FrameworkIntegrationTest {
    @Test(timeout=60_000)
    public void testSwapOnRestart() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode generator = createProcessorNode(GenerateProcessor.class);
        generator.setProperties(Collections.singletonMap(GenerateProcessor.COUNT.getName(), "60000"));

        Connection connection = connect(generator, getTerminateAllProcessor(), REL_SUCCESS);
        triggerOnce(generator);

        FlowFileQueue queue = connection.getFlowFileQueue();
        QueueDiagnostics diagnostics = queue.getQueueDiagnostics();
        LocalQueuePartitionDiagnostics localPartitionDiagnostics = diagnostics.getLocalQueuePartitionDiagnostics();

        assertEquals(20_000, localPartitionDiagnostics.getActiveQueueSize().getObjectCount());
        assertEquals(4, localPartitionDiagnostics.getSwapFileCount());
        assertEquals(40_000, localPartitionDiagnostics.getSwapQueueSize().getObjectCount());
        assertEquals(60_000, queue.size().getObjectCount());

        // restart nifi
        restart();

        // get the new Connection with the same ID
        connection = getRootGroup().getConnection(connection.getIdentifier());
        queue = connection.getFlowFileQueue();
        diagnostics = queue.getQueueDiagnostics();
        localPartitionDiagnostics = diagnostics.getLocalQueuePartitionDiagnostics();

        // Ensure we have the correct queue sizes
        assertEquals(20_000, localPartitionDiagnostics.getActiveQueueSize().getObjectCount());
        assertEquals(4, localPartitionDiagnostics.getSwapFileCount());
        assertEquals(40_000, localPartitionDiagnostics.getSwapQueueSize().getObjectCount());
        assertEquals(60_000, queue.size().getObjectCount());

        // Consume all the data
        for (int i=0; i < 60_000; i++) {
            final FlowFileRecord flowFile = queue.poll(Collections.emptySet());
            assertNotNull(flowFile);

            queue.acknowledge(flowFile);
        }

        assertNull(queue.poll(Collections.emptySet()));

        // Check queue sizes again
        diagnostics = queue.getQueueDiagnostics();
        localPartitionDiagnostics = diagnostics.getLocalQueuePartitionDiagnostics();

        assertEquals(0, localPartitionDiagnostics.getActiveQueueSize().getObjectCount());
        assertEquals(0, localPartitionDiagnostics.getSwapFileCount());
        assertEquals(0, localPartitionDiagnostics.getSwapQueueSize().getObjectCount());
        assertEquals(0, queue.size().getObjectCount());
    }


}
