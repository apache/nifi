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

import org.apache.nifi.tests.system.NiFiClientUtil;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies Connector backlog reporting end to end in a single-node NiFi:
 * <ul>
 *     <li>Connector delegates {@code getBacklog} to an embedded {@code BacklogReportingTestProcessor}.</li>
 *     <li>Connector reads its own queue snapshot, accesses FlowFile attributes, and reports a Backlog from them.</li>
 *     <li>Errors thrown by the embedded Processor's {@code getBacklog} are surfaced as the backlog request's
 *         failure reason, both for {@link org.apache.nifi.components.BacklogReportingException} and for unexpected
 *         {@link RuntimeException}s.</li>
 *     <li>A Connector that opts out of backlog reporting returns a {@code null} backlog.</li>
 * </ul>
 */
public class ConnectorBacklogIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorBacklogIT.class);

    private static final String STEP_NAME = "Backlog Configuration";

    @Test
    public void testConnectorDelegatesBacklogToProcessor() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "DELEGATE_TO_PROCESSOR",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", "42",
                "Byte Backlog", "1024",
                "Record Backlog", "7",
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());

        assertNotNull(backlog);
        assertEquals(Long.valueOf(42L), backlog.getFlowFileCount());
        assertEquals(Long.valueOf(1024L), backlog.getByteCount());
        assertEquals(Long.valueOf(7L), backlog.getRecordCount());
        assertEquals("EXACT", backlog.getPrecision());
    }

    @Test
    public void testConnectorReadsFlowFileAttributesFromQueueSnapshot() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "READ_QUEUE_ATTRIBUTES",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        getClientUtil().startConnector(connector.getId());
        waitForConnectorMinQueueCount(connector.getId(), 20);
        getClientUtil().stopConnector(connector.getId());

        final int totalQueued = getConnectorQueuedFlowFileCount(connector.getId());
        logger.info("Total queued FlowFiles before reading backlog: {}", totalQueued);
        assertTrue(totalQueued > 0);

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());
        assertNotNull(backlog);

        final long flowFiles = backlog.getFlowFileCount();
        assertTrue(flowFiles >= 0);
        assertTrue(flowFiles <= totalQueued, "FlowFile count read from queue (" + flowFiles + ") must not exceed total queued count (" + totalQueued + ")");
        assertEquals(Long.valueOf(flowFiles), backlog.getRecordCount());
        assertTrue(backlog.getByteCount() >= 0L);
    }

    @Test
    public void testProcessorBacklogReportingExceptionReportedAsFailure() throws NiFiClientException, IOException, InterruptedException {
        final String message = "Simulated BacklogReportingException for ConnectorBacklogIT";
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "DELEGATE_TO_PROCESSOR",
                "Processor Backlog Mode", "THROW_BACKLOG_REPORTING_EXCEPTION",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Exception Message", message,
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final NiFiClientException thrown = assertThrows(NiFiClientException.class,
                () -> getClientUtil().getConnectorBacklog(connector.getId()));
        assertTrue(thrown.getMessage().contains(message),
                "Expected backlog request failure reason to contain configured exception message but was: " + thrown.getMessage());
    }

    @Test
    public void testProcessorRuntimeExceptionReportedAsFailure() throws NiFiClientException, IOException, InterruptedException {
        final String message = "Simulated RuntimeException for ConnectorBacklogIT";
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "DELEGATE_TO_PROCESSOR",
                "Processor Backlog Mode", "THROW_RUNTIME_EXCEPTION",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Exception Message", message,
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final NiFiClientException thrown = assertThrows(NiFiClientException.class,
                () -> getClientUtil().getConnectorBacklog(connector.getId()));
        assertTrue(thrown.getMessage().contains(message),
                "Expected backlog request failure reason to contain configured exception message but was: " + thrown.getMessage());
    }

    @Test
    public void testConnectorReturnsEmptyBacklog() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "RETURN_EMPTY",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());
        assertNull(backlog);
    }

    /**
     * A Connector that delegates to a Processor reporting a fully caught-up source returns a non-null
     * {@link BacklogDTO} carrying explicit zero counts. This "empty backlog with zero counts" result is
     * distinct from {@link #testConnectorReturnsEmptyBacklog()}, where a Connector that opts out of backlog
     * reporting returns a null {@code backlog}. A caller must be able to tell "the source has exactly zero
     * remaining work" (zero counts) apart from "no backlog information is available" (null backlog).
     */
    @Test
    public void testConnectorDelegatesZeroCountBacklog() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = configureBacklogConnector(Map.of(
                "Delegation Mode", "DELEGATE_TO_PROCESSOR",
                "Processor Backlog Mode", "NORMAL",
                "FlowFile Backlog", "0",
                "Byte Backlog", "0",
                "Record Backlog", "0",
                "Load Balance Strategy", "DO_NOT_LOAD_BALANCE"));

        final BacklogDTO backlog = getClientUtil().getConnectorBacklog(connector.getId());

        assertNotNull(backlog);
        assertEquals(Long.valueOf(0L), backlog.getFlowFileCount());
        assertEquals(Long.valueOf(0L), backlog.getByteCount());
        assertEquals(Long.valueOf(0L), backlog.getRecordCount());
        assertEquals("EXACT", backlog.getPrecision());
    }

    static ConnectorEntity configureBacklogConnector(final NiFiClientUtil clientUtil, final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = clientUtil.createConnector("BacklogReportingTestConnector");
        assertNotNull(connector);
        clientUtil.configureConnector(connector, STEP_NAME, properties);
        clientUtil.applyConnectorUpdate(connector);
        clientUtil.waitForValidConnector(connector.getId());
        return connector;
    }

    private ConnectorEntity configureBacklogConnector(final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        return configureBacklogConnector(getClientUtil(), properties);
    }
}
