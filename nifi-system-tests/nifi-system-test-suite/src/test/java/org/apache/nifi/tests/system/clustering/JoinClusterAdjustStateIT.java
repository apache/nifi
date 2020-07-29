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
package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class JoinClusterAdjustStateIT extends NiFiSystemIT {
    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                .instanceDirectory("target/node1")
                .build(),
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                .instanceDirectory("target/node2")
                .autoStart(false)
                .build()
        );
    }

    @Test
    public void testProcessorsStartWhenAble() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity countProcessor = getClientUtil().createProcessor(TEST_PROCESSORS_PACKAGE + ".CountEvents", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
        final ProcessorEntity fileProcessor = getClientUtil().createProcessor(TEST_PROCESSORS_PACKAGE + ".ValidateFileExists", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());

        // Set scheduling Period to 1 hour so that we know when the Processors actually get triggered to run.
        getClientUtil().updateProcessorSchedulingPeriod(countProcessor, "1 hour");
        getClientUtil().updateProcessorSchedulingPeriod(fileProcessor, "1 hour");

        final File firstNodeInstanceDir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File firstNodeMonitoredDir = new File(firstNodeInstanceDir, "monitored");
        assertTrue(firstNodeMonitoredDir.mkdirs());

        getClientUtil().updateProcessorProperties(fileProcessor, Collections.singletonMap("Filename", "./monitored"));
        getClientUtil().setAutoTerminatedRelationships(countProcessor, Collections.singleton("success"));
        getClientUtil().setAutoTerminatedRelationships(fileProcessor, Collections.singleton("success"));

        getClientUtil().waitForValidProcessor(countProcessor.getId());
        getClientUtil().waitForValidProcessor(fileProcessor.getId());

        // Start the Processor that requires a file named "monitored" to exist. When we join Node 2 to the cluster, this directory will not exist.
        // We want to ensure that the Processor does in fact start on its own when the directory is created.
        getNifiClient().getProcessorClient().startProcessor(fileProcessor.getId(), fileProcessor.getRevision().getClientId(), 1);
        getClientUtil().waitForRunningProcessor(fileProcessor.getId());

        // Create a new NiFi instance
        final NiFiInstance node2Instance = getNiFiInstance().getNodeInstance(2);

        // Copy the flow from Node 1 to Node 2.
        final File node1Flow = new File(firstNodeInstanceDir, "conf/flow.xml.gz");
        final File node2Flow = new File(node2Instance.getInstanceDirectory(), "conf/flow.xml.gz");
        Thread.sleep(2000L); // Wait a bit before copying it, since the flow is written out in the background, and we want to ensure that the flow is up-to-date.
        Files.copy(node1Flow.toPath(), node2Flow.toPath());

        // Start the Count Processor on Node 1. When Node 2 joins the cluster, we know that its flow will indicate that the Processor is stopped.
        // But because the cluster indicates that the Processor is running, the second node should inherit this value and immediately start the Processor also.
        getNifiClient().getProcessorClient().startProcessor(countProcessor.getId(), countProcessor.getRevision().getClientId(), 1);

        node2Instance.start();

        // Wait for the second node to join the cluster.
        waitForAllNodesConnected(2);

        // Wait for the Count Processor to be running
        getClientUtil().waitForRunningProcessor(countProcessor.getId());
        getClientUtil().waitForCounter(countProcessor.getId(), "Triggered", 2);

        // Create the "monitored" directory for node 2.
        final File secondNodeMonitoredDir = new File(node2Instance.getInstanceDirectory(), "monitored");
        assertTrue(secondNodeMonitoredDir.mkdirs());

        getClientUtil().waitForCounter(fileProcessor.getId(), "Triggered", 2);
    }

}
