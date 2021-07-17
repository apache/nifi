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

import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FlowFileRestorationIT extends NiFiSystemIT {

    @Test
    public void testDataInMissingQueueRestoredWhenQueueRestored() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generator, Collections.singletonMap("File Size", "1 KB"));
        getClientUtil().updateProcessorSchedulingPeriod(generator, "100 min");

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generator, terminate, "success");

        getNifiClient().getProcessorClient().startProcessor(generator);
        waitForQueueCount(connection.getId(), 1);
        getNifiClient().getProcessorClient().stopProcessor(generator);

        final byte[] flowFileContents = getFlowFileContents(connection.getId(), 0);

        assertEquals(1024, flowFileContents.length);

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();

        final File nifiHome = nifiInstance.getInstanceDirectory();
        final File confDir = new File(nifiHome, "conf");
        final File flowXmlGz = new File(confDir, "flow.xml.gz");
        final byte[] flowXmlGzBytes = Files.readAllBytes(flowXmlGz.toPath());
        assertTrue(flowXmlGz.delete());

        nifiInstance.start();

        try {
            getNifiClient().getConnectionClient().getConnection(connection.getId());
            Assert.fail("Didn't expect to retrieve a connection");
        } catch (final NiFiClientException nfce) {
            // Expected because the connection no longer exists.
        }

        // Stop the instance, restore the flow.xml.gz, and restart
        nifiInstance.stop();
        Files.write(flowXmlGz.toPath(), flowXmlGzBytes, StandardOpenOption.CREATE);
        nifiInstance.start();

        // Ensure that there's a FlowFile queued up and that its contents are still accessible and have not changed.
        final ConnectionEntity retrievedConnection = getNifiClient().getConnectionClient().getConnection(connection.getId());
        assertNotNull(retrievedConnection);
        waitForQueueCount(connection.getId(), 1);
        final byte[] contentsAfterRestart = getFlowFileContents(connection.getId(), 0);

        assertArrayEquals(flowFileContents, contentsAfterRestart);
    }

    private byte[] getFlowFileContents(final String connectionId, final int flowFileIndex) throws IOException, NiFiClientException {
        final byte[] flowFileContents;
        try (final InputStream in = getClientUtil().getFlowFileContent(connectionId, flowFileIndex);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            return baos.toByteArray();
        }
    }
}
