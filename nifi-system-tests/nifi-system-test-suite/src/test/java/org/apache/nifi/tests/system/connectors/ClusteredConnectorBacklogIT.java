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

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BacklogDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cluster-specific verification that Connector backlog aggregates correctly across nodes and that
 * the queue-snapshot path works against both {@code StandardFlowFileQueue} (normal connections)
 * and {@code SocketLoadBalancedFlowFileQueue} (load-balanced connections).
 */
public class ClusteredConnectorBacklogIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredConnectorBacklogIT.class);

    private static final int CLUSTER_SIZE = 2;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testClusteredDelegateBacklogAggregatesAcrossNodes() throws NiFiClientException, IOException, InterruptedException {
        final long perNodeFlowFiles = 42L;
        final long perNodeBytes = 1024L;
        final long perNodeRecords = 7L;

        final ConnectorEntity connector = ConnectorBacklogIT.configureBacklogConnector(getClientUtil(), Map.of(
                "Delegation Mode", "DELEGATE_TO_PROCESSOR",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", String.valueOf(perNodeFlowFiles),
                "Byte Backlog", String.valueOf(perNodeBytes),
                "Record Backlog", String.valueOf(perNodeRecords),
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());

        assertNotNull(backlog);
        assertEquals(Long.valueOf(perNodeFlowFiles * CLUSTER_SIZE), backlog.getFlowFileCount());
        assertEquals(Long.valueOf(perNodeBytes * CLUSTER_SIZE), backlog.getByteCount());
        assertEquals(Long.valueOf(perNodeRecords * CLUSTER_SIZE), backlog.getRecordCount());
    }

    @Test
    public void testClusteredReadQueueAttributesNormalConnection() throws NiFiClientException, IOException, InterruptedException {
        runQueueAttributeAggregationScenario("DO_NOT_LOAD_BALANCE");
    }

    @Test
    public void testClusteredReadQueueAttributesLoadBalancedConnection() throws NiFiClientException, IOException, InterruptedException {
        runQueueAttributeAggregationScenario("ROUND_ROBIN");
    }

    private void runQueueAttributeAggregationScenario(final String loadBalanceStrategy) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = ConnectorBacklogIT.configureBacklogConnector(getClientUtil(), Map.of(
                "Delegation Mode", "READ_QUEUE_ATTRIBUTES",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Load Balance Strategy", loadBalanceStrategy));

        getClientUtil().startConnector(connector.getId());
        waitForConnectorMinQueueCount(connector.getId(), 40);
        getClientUtil().stopConnector(connector.getId());

        final int totalQueued = getConnectorQueuedFlowFileCount(connector.getId());
        logger.info("Load Balance Strategy {}: cluster has {} queued FlowFiles before reading backlog", loadBalanceStrategy, totalQueued);
        assertTrue(totalQueued > 0);

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());
        assertNotNull(backlog);

        final long flowFiles = backlog.getFlowFileCount();
        assertTrue(flowFiles >= 0);
        assertTrue(flowFiles <= totalQueued,
                "Cluster aggregate FlowFile count from queue snapshot (" + flowFiles + ") must not exceed total queued count (" + totalQueued + ")");
        assertEquals(Long.valueOf(flowFiles), backlog.getRecordCount());
        assertTrue(backlog.getByteCount() >= 0L);
    }
}
