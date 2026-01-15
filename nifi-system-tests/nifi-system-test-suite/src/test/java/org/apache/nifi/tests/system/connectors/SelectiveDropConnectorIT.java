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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedStandaloneNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System test for the SelectiveDropConnector which tests the ability to selectively drop FlowFiles
 * based on a predicate when a Connector is stopped.
 */
public class SelectiveDropConnectorIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(SelectiveDropConnectorIT.class);

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        // Ensure that each FlowFile is written to its own content claim by setting max appendable size to 1 byte
        // and set the swap threshold to 2,000 FlowFiles to allow for multiple swap files to be created during the test
        final Map<String, String> nifiPropertiesOverrides = Map.of(
            "nifi.content.claim.max.appendable.size", "1 B",
            "nifi.queue.swap.threshold", "2000"
        );

        return new SpawnedStandaloneNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/default/bootstrap.conf")
                .instanceDirectory("target/standalone-instance")
                .overrideNifiProperties(nifiPropertiesOverrides)
                .build());
    }

    // Ensure environment is destroyed after each test to reset nifi.properties overrides
    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    // Do not allow factory reuse as the test modifies nifi.properties
    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testSelectiveDropOnConnectorStop() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("SelectiveDropConnector");
        assertNotNull(connector);

        getNifiClient().getConnectorClient().startConnector(connector);
        logger.info("Started SelectiveDropConnector, waiting for 20,000 FlowFiles to be queued");

        waitForConnectorQueueSize(connector.getId(), 20000);
        logger.info("Queue has reached 20,000 FlowFiles");

        // We expect 9 swap files because it should be 2,000 FlowFiles in active queue and 18,000 FlowFiles swapped out.
        // With 2,000 per swap file, that results in 9 swap files.
        final File instanceDir = getNiFiInstance().getInstanceDirectory();
        waitForSwapFileCount(9);

        getClientUtil().stopConnector(connector);
        logger.info("Stopped SelectiveDropConnector, which should have triggered selective drop of even-indexed FlowFiles");

        final int queuedFlowFiles = getConnectorQueueSize(connector.getId());
        assertEquals(10000, queuedFlowFiles);

        final int swapFileCountAfterPurge = countSwapFiles(instanceDir);
        assertEquals(9, swapFileCountAfterPurge);

        logger.info("Restarting NiFi instance to verify state persists across restart");
        getNiFiInstance().stop();
        getNiFiInstance().start();

        final int queuedFlowFilesAfterRestart = getConnectorQueueSize(connector.getId());
        assertEquals(10000, queuedFlowFilesAfterRestart);

        final int contentFileCountWithArchive = countContentRepositoryFiles(instanceDir, true);
        final int contentFileCountWithoutArchive = countContentRepositoryFiles(instanceDir, false);
        assertEquals(10000, contentFileCountWithoutArchive);
        assertEquals(20000, contentFileCountWithArchive);

        final int swapFileCountAfterRestart = countSwapFiles(instanceDir);
        assertEquals(9, swapFileCountAfterRestart);
    }

    private void waitForSwapFileCount(final int expectedCount) throws InterruptedException {
        waitFor(() -> {
            final File instanceDir = getNiFiInstance().getInstanceDirectory();
            final int swapFileCount = countSwapFiles(instanceDir);
            return swapFileCount == expectedCount;
        });
    }

    private void waitForConnectorQueueSize(final String connectorId, final int expectedSize) throws InterruptedException {
        waitFor(() -> getConnectorQueueSize(connectorId) >= expectedSize);
    }

    private int getConnectorQueueSize(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorEntity connector = getNifiClient().getConnectorClient().getConnector(connectorId);
        final String managedProcessGroupId = connector.getComponent().getManagedProcessGroupId();
        if (managedProcessGroupId == null) {
            return 0;
        }

        final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connectorId, managedProcessGroupId);
        final ProcessGroupFlowDTO flowDto = flowEntity.getProcessGroupFlow();
        if (flowDto == null || flowDto.getFlow() == null) {
            return 0;
        }

        final Set<ConnectionEntity> connections = flowDto.getFlow().getConnections();
        int totalQueueSize = 0;
        for (final ConnectionEntity connection : connections) {
            if (connection.getStatus() != null && connection.getStatus().getAggregateSnapshot() != null) {
                totalQueueSize += connection.getStatus().getAggregateSnapshot().getFlowFilesQueued();
            }
        }
        return totalQueueSize;
    }

    private int countContentRepositoryFiles(final File instanceDir, final boolean includeArchive) {
        final File contentRepo = new File(instanceDir, "content_repository");
        final Predicate<File> filter = includeArchive ? file -> true : file -> !file.getName().equals("archive");
        return countFilesRecursively(contentRepo, filter);
    }

    private int countSwapFiles(final File instanceDir) {
        final File flowFileRepo = new File(instanceDir, "flowfile_repository");
        final File swapDir = new File(flowFileRepo, "swap");
        final File[] swapFiles = swapDir.listFiles();
        return swapFiles == null ? 0 : swapFiles.length;
    }

    private int countFilesRecursively(final File directory, final Predicate<File> filter) {
        if (!directory.exists() || !directory.isDirectory()) {
            return 0;
        }

        int count = 0;
        final File[] files = directory.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.isDirectory()) {
                    if (filter.test(file)) {
                        count += countFilesRecursively(file, filter);
                    }
                } else if (filter.test(file)) {
                    count++;
                }
            }
        }

        return count;
    }
}
