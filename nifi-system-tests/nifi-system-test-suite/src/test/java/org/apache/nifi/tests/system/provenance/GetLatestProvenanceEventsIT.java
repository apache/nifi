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

package org.apache.nifi.tests.system.provenance;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetLatestProvenanceEventsIT extends NiFiSystemIT {

    @Test
    public void testSingleEvent() throws NiFiClientException, IOException, InterruptedException {
        runTest(false);
    }

    @Test
    public void testMultipleEvents() throws NiFiClientException, IOException, InterruptedException {
        runTest(true);
    }

    private void runTest(final boolean autoTerminateReverse) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity reverse = getClientUtil().createProcessor("ReverseContents");

        if (autoTerminateReverse) {
            getClientUtil().setAutoTerminatedRelationships(reverse, Set.of("success", "failure"));
        } else {
            final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
            getClientUtil().createConnection(reverse, terminate, "success");
            getClientUtil().setAutoTerminatedRelationships(reverse, "failure");
        }

        getClientUtil().createConnection(generate, reverse, "success");
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", "Hello, World!"));

        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(reverse);

        final int expectedEventCount = getNumberOfNodes() * (autoTerminateReverse ? 2 : 1);
        waitFor(() -> {
            final LatestProvenanceEventsEntity entity = getNifiClient().getProvenanceClient().getLatestEvents(reverse.getId());
            final List<ProvenanceEventDTO> events = entity.getLatestProvenanceEvents().getProvenanceEvents();
            return events.size() == expectedEventCount;
        });

        final LatestProvenanceEventsEntity entity = getNifiClient().getProvenanceClient().getLatestEvents(reverse.getId());
        final List<ProvenanceEventDTO> events = entity.getLatestProvenanceEvents().getProvenanceEvents();
        final Map<String, Integer> countsByEventType = new HashMap<>();
        for (final ProvenanceEventDTO event : events) {
            assertEquals(reverse.getId(), event.getComponentId());
            final String eventType = event.getEventType();
            countsByEventType.put(eventType, countsByEventType.getOrDefault(eventType, 0) + 1);

            if (getNumberOfNodes() > 1) {
                assertNotNull(event.getClusterNodeId());
            }
        }

        if (autoTerminateReverse) {
            assertEquals(getNumberOfNodes(), countsByEventType.get("DROP").intValue());
        }
        assertEquals(getNumberOfNodes(), countsByEventType.get("CONTENT_MODIFIED").intValue());
    }
}
