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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStatusHistoryEndpointMerger {

    private static final String UUID = "12345678-1234-1234-1234-123456789012";

    @Test
    public void testCanHandleConnectorStatusHistory() {
        final StatusHistoryEndpointMerger merger = new StatusHistoryEndpointMerger(300000);

        assertTrue(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/processors/" + UUID + "/status/history"), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/connections/" + UUID + "/status/history"), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/process-groups/" + UUID + "/status/history"), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/remote-process-groups/" + UUID + "/status/history"), "GET"));

        // The top-level flow endpoints continue to be handled alongside the connector endpoints.
        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/processors/" + UUID + "/status/history"), "GET"));

        // Non-GET methods and unrelated connector paths are not handled.
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/processors/" + UUID + "/status/history"), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + UUID + "/status"), "GET"));
    }

    @Test
    public void testNormalizedStatusSnapshotDate() {
        final Date date1 = new Date(1388538000000L);
        final Date date2 = new Date(1388538299999L);
        final Date date3 = new Date(1388538300000L);
        final Date date4 = new Date(1388538300001L);

        final Date normalized1 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date1, 300000);
        assertEquals(date1, normalized1);

        final Date normalized2 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date2, 300000);
        assertEquals(date1, normalized2);

        final Date normalized3 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date3, 300000);
        assertEquals(date3, normalized3);

        final Date normalized4 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date4, 300000);
        assertEquals(date3, normalized4);
    }
}
