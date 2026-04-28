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

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OffloadIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(OffloadIT.class);

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    // Test to ensure that node can be offloaded, reconnected, offloaded several times. This test typically takes only about 1-2 minutes
    // but can occasionally take 5-6 minutes on Github Actions so we set the timeout to 10 minutes to allow for these occasions
    public void testOffload() throws InterruptedException, IOException, NiFiClientException {
        for (int i = 0; i < 5; i++) {
            logger.info("Running iteration {}", i);
            testIteration();
            logger.info("Node reconnected to cluster");
            destroyFlow();
        }
    }

    private void testIteration() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity sleep = getClientUtil().createProcessor("Sleep");
        ConnectionEntity connectionEntity = getClientUtil().createConnection(generate, sleep, "success");

        getClientUtil().setAutoTerminatedRelationships(sleep, "success");
        generate = getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("File Size", "1 KB"));
        final ProcessorConfigDTO configDto = generate.getComponent().getConfig();
        configDto.setSchedulingPeriod("0 sec");
        getClientUtil().updateProcessorConfig(generate, configDto);

        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "100 ms"));

        getClientUtil().startProcessGroupComponents("root");

        waitForQueueNotEmpty(connectionEntity.getId());

        final NodeDTO node2Dto = getNodeDtoByApiPort(5672);

        disconnectNode(node2Dto);
        waitForNodeStatus(node2Dto, "DISCONNECTED");

        final String nodeId = node2Dto.getNodeId();
        getClientUtil().offloadNode(nodeId);
        waitForNodeStatus(node2Dto, "OFFLOADED");

        getClientUtil().connectNode(nodeId);
        waitForAllNodesConnected();
    }

    /**
     * Verifies that a node can complete offload after a Processor that retains Sessions across onTrigger
     * invocations is stopped and terminated. The HoldInput Processor extends AbstractSessionFactoryProcessor
     * and stashes its ProcessSession (not the ProcessSessionFactory) in a member field. With the configured
     * Hold Time of one hour, HoldInput never voluntarily releases the FlowFiles it has accepted, so the
     * only way for those FlowFiles to be acknowledged on the source queue is through the framework's stop
     * and terminate path rolling back the stashed Session.
     */
    @Test
    @Timeout(120)
    public void testOffloadCompletesAfterStoppingAndTerminatingProcessorThatRetainsSessions() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity hold = getClientUtil().createProcessor("HoldInput");
        final ConnectionEntity generateToHold = getClientUtil().createConnection(generate, hold, "success");

        getClientUtil().setAutoTerminatedRelationships(hold, "success");

        generate = getClientUtil().updateProcessorProperties(generate, Map.of("File Size", "1 KB"));
        final ProcessorConfigDTO generateConfig = generate.getComponent().getConfig();
        generateConfig.setSchedulingPeriod("0 sec");
        getClientUtil().updateProcessorConfig(generate, generateConfig);

        // Configure HoldInput with a Hold Time that exceeds the test timeout so it pulls FlowFiles
        // into stashed Sessions and never voluntarily releases them.
        hold = getClientUtil().updateProcessorProperties(hold, Map.of("Hold Time", "1 hour"));

        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(hold);

        // Confirm GenerateFlowFile has produced FlowFiles into the connection. HoldInput pulls
        // 10,000 FlowFiles per onTrigger, so the queue may be drained back to zero quickly, but
        // first observing it as non-empty proves both Processors are running and that HoldInput
        // is being scheduled with FlowFiles available.
        waitForQueueNotEmpty(generateToHold.getId());

        // Allow time on each node for HoldInput to perform at least one onTrigger invocation that pulls
        // FlowFiles in and stashes the Session.
        Thread.sleep(2_000L);

        getClientUtil().stopProcessor(generate);
        getClientUtil().stopProcessor(hold);

        // With HoldInput stopped, terminate it so the framework's terminate path runs while the previously
        // created Sessions are still pinning FlowFiles as unacknowledged on the source queue.
        getNifiClient().getProcessorClient().terminateProcessor(hold.getId());

        final NodeDTO nodeTwoDto = getNodeDtoByApiPort(5672);
        disconnectNode(nodeTwoDto);
        waitForNodeStatus(nodeTwoDto, "DISCONNECTED");

        final String nodeId = nodeTwoDto.getNodeId();
        getClientUtil().offloadNode(nodeId);

        // Offload waits until the queued FlowFile count on every queue drops to zero. If the stop and
        // terminate sequence above failed to roll back the stashed Sessions, the unacknowledged FlowFiles
        // on the source queue prevent the count from reaching zero and this wait will not complete before
        // the test timeout.
        waitForNodeStatus(nodeTwoDto, "OFFLOADED");

        getClientUtil().connectNode(nodeId);
        waitForAllNodesConnected();
        destroyFlow();
    }

    private void disconnectNode(final NodeDTO nodeDto) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().disconnectNode(nodeDto.getNodeId());

        final Integer apiPort = nodeDto.getApiPort();
        waitFor(() -> {
            try {
                final NodeDTO dto = getNodeDtoByApiPort(apiPort);
                final String status = dto.getStatus();
                return "DISCONNECTED".equals(status);
            } catch (final Exception e) {
                logger.error("Failed to determine if node is disconnected", e);
            }

            return false;
        });
    }

}
