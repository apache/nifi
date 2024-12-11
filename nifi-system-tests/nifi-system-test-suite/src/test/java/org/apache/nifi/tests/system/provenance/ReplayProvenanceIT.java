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
import org.apache.nifi.toolkit.client.ProvenanceClient.ReplayEventNodes;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplayProvenanceIT extends NiFiSystemIT {

    @ParameterizedTest
    @EnumSource(ReplayEventNodes.class)
    public void testReplayLastEvent(final ReplayEventNodes nodes) throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        // Run Generate once
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), getNumberOfNodes());
        getClientUtil().stopProcessor(generate);

        // Run terminate once
        getClientUtil().startProcessor(terminate);
        waitForQueueCount(connection.getId(), 0);
        getClientUtil().stopProcessor(terminate);

        // Replay last event for terminate and ensure that data is queued up.
        final ReplayLastEventResponseEntity replayResponse = getNifiClient().getProvenanceClient().replayLastEvent(terminate.getId(), nodes);
        assertNull(replayResponse.getAggregateSnapshot().getFailureExplanation());
        assertEquals(Boolean.TRUE, replayResponse.getAggregateSnapshot().getEventAvailable());
        final int expectedEventsPlayed = (nodes == ReplayEventNodes.PRIMARY) ? 1 : getNumberOfNodes();
        assertEquals(expectedEventsPlayed, replayResponse.getAggregateSnapshot().getEventsReplayed().size());

        waitForQueueCount(connection.getId(), expectedEventsPlayed);

        // Attempt to replay event for generate - it should provide an error because this is a source processor whose event cannot be replayed
        final ReplayLastEventResponseEntity generateReplayResponse = getNifiClient().getProvenanceClient().replayLastEvent(generate.getId(), nodes);
        final String failureExplanation = generateReplayResponse.getAggregateSnapshot().getFailureExplanation();
        assertNotNull(failureExplanation);

        // The failure text is not provided if multiple nodes failed, as it can get too unwieldy to understand.
        final String expectedFailureText = (nodes == ReplayEventNodes.ALL && getNumberOfNodes() > 1) ? "See logs for more details" : "Source FlowFile Queue";
        assertTrue(failureExplanation.contains(expectedFailureText), failureExplanation);
    }

}
