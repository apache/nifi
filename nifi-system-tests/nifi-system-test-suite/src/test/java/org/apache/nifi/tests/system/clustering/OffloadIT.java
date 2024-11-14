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
