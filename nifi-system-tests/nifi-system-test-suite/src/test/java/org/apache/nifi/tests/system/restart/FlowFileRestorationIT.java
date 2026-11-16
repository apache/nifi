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

package org.apache.nifi.tests.system.restart;

import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowFileRestorationIT extends NiFiSystemIT {
    private static final int SWAP_THRESHOLD = 20;

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of("nifi.queue.swap.threshold", String.valueOf(SWAP_THRESHOLD));
    }

    @Test
    public void testDataInMissingQueueRestoredWhenQueueRestored() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generator, Collections.singletonMap("File Size", "1 KB"));
        getClientUtil().updateProcessorSchedulingPeriod(generator, "100 min");

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generator, terminate, "success");

        getClientUtil().startProcessor(generator);
        waitForQueueCount(connection.getId(), 1);
        getNifiClient().getProcessorClient().stopProcessor(generator);

        final byte[] flowFileContents = getFlowFileContents(connection.getId(), 0);

        assertEquals(1024, flowFileContents.length);

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();

        final File nifiHome = nifiInstance.getInstanceDirectory();
        final File confDir = new File(nifiHome, "conf");

        final File flowJsonGz = new File(confDir, "flow.json.gz");
        final byte[] flowJsonGzBytes = Files.readAllBytes(flowJsonGz.toPath());
        assertTrue(flowJsonGz.delete());

        nifiInstance.start();

        assertThrows(NiFiClientException.class, () -> getNifiClient().getConnectionClient().getConnection(connection.getId()));

        // Stop the instance, restore the flow.xml.gz, and restart
        nifiInstance.stop();
        Files.write(flowJsonGz.toPath(), flowJsonGzBytes, StandardOpenOption.CREATE);
        nifiInstance.start();

        // Ensure that there's a FlowFile queued up and that its contents are still accessible and have not changed.
        final ConnectionEntity retrievedConnection = getNifiClient().getConnectionClient().getConnection(connection.getId());
        assertNotNull(retrievedConnection);
        waitForQueueCount(connection.getId(), 1);
        final byte[] contentsAfterRestart = getFlowFileContents(connection.getId(), 0);

        assertArrayEquals(flowFileContents, contentsAfterRestart);
    }

    @Test
    public void testSwappedFlowFileOrderPreservedAfterRestart() throws NiFiClientException, IOException, InterruptedException {
        final int flowFileCount = 100;
        final int newerFlowFileCount = 2;
        ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        generate = getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", String.valueOf(flowFileCount), "sequence", "${nextInt()}"));

        ProcessorEntity verifyOrder = getClientUtil().createProcessor("VerifySequentialOrder");
        verifyOrder = getClientUtil().updateProcessorProperties(verifyOrder, Map.of("Value to Consider", "${sequence}"));
        final ConnectionEntity connection = getClientUtil().updateConnectionPrioritizer(getClientUtil().createConnection(generate, verifyOrder, SUCCESS), "FirstInFirstOutPrioritizer");

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        final ConnectionEntity sequentialConnection = getClientUtil().createConnection(verifyOrder, terminate, "sequential");
        final ConnectionEntity failureConnection = getClientUtil().createConnection(verifyOrder, terminate, "failure");

        generate = getClientUtil().runProcessorOnce(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());
        waitForQueueCount(connection.getId(), flowFileCount);

        // The oldest FlowFiles are held in memory and the rest are written to swap files. Process as many of the oldest FlowFiles as the number
        // of newer FlowFiles and then queue the newer FlowFiles, which wait behind the swapped FlowFiles. All FlowFiles held in memory then fit
        // within the swap threshold when they are restored.
        for (int i = 0; i < newerFlowFileCount; i++) {
            verifyOrder = getClientUtil().runProcessorOnce(verifyOrder);
            getClientUtil().waitForStoppedProcessor(verifyOrder.getId());
        }

        generate = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        generate = getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", String.valueOf(newerFlowFileCount)));
        generate = getClientUtil().runProcessorOnce(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());
        waitForQueueCount(connection.getId(), flowFileCount);
        assertEquals(SWAP_THRESHOLD - newerFlowFileCount, getClientUtil().performQueueListing(connection.getId()).getListingRequest().getFlowFileSummaries().size());

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();
        nifiInstance.start();
        waitForQueueCount(connection.getId(), flowFileCount);

        // Process all FlowFiles, including the swapped FlowFiles. Each FlowFile must be processed directly after the FlowFile created before it.
        verifyOrder = getNifiClient().getProcessorClient().getProcessor(verifyOrder.getId());
        getClientUtil().startProcessor(verifyOrder);
        waitFor(() -> getConnectionQueueSize(sequentialConnection.getId()) + getConnectionQueueSize(failureConnection.getId()) == flowFileCount + newerFlowFileCount);
        assertEquals(0, getConnectionQueueSize(failureConnection.getId()));
    }

    private byte[] getFlowFileContents(final String connectionId, final int flowFileIndex) throws IOException, NiFiClientException {
        try (final InputStream in = getClientUtil().getFlowFileContent(connectionId, flowFileIndex);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            in.transferTo(baos);
            return baos.toByteArray();
        }
    }
}
