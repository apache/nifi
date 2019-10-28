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
package org.apache.nifi.tests.system.rpg;

import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class RemoteProcessGroupIT extends NiFiSystemIT {

    @Test
    public void testRPGBackToSelfHttp() throws NiFiClientException, IOException, InterruptedException {
        testRPGBackToSelf(SiteToSiteTransportProtocol.HTTP, "HttpIn");
    }

    @Test
    public void testRPGBackToSelfRaw() throws NiFiClientException, IOException, InterruptedException {
        testRPGBackToSelf(SiteToSiteTransportProtocol.RAW, "RawIn");
    }


    protected void testRPGBackToSelf(final SiteToSiteTransportProtocol protocol, final String portName) throws NiFiClientException, IOException, InterruptedException {
        final NiFiClientUtil util = getClientUtil();

        // Create a flow that is InputPort -> CountEvents
        final PortEntity port = util.createRemoteInputPort("root", portName);
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");
        util.setAutoTerminatedRelationships(count, "success");

        // Create a flow that is GenerateFlowFile -> RPG, connected to the input port
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        RemoteProcessGroupEntity rpg = getClientUtil().createRPG("root", protocol);

        util.updateProcessorProperties(generateFlowFile, Collections.singletonMap("File Size", "1 KB"));
        util.updateProcessorProperties(generateFlowFile, Collections.singletonMap("Batch Size", "3"));
        util.updateProcessorSchedulingPeriod(generateFlowFile, "10 min");

        final String rpgId = rpg.getId();

        // Wait for the port to become available. We have to check for the specific port ID because otherwise,
        // the RPG may have an old Port ID cached, since we are running an HTTP-based test and a RAW-based test
        waitFor(() -> {
            try {
                final RemoteProcessGroupEntity entity = getNifiClient().getRemoteProcessGroupClient().getRemoteProcessGroup(rpgId);
                final Set<RemoteProcessGroupPortDTO> ports = entity.getComponent().getContents().getInputPorts();
                if (ports.isEmpty()) {
                    return false;
                }

                for (final RemoteProcessGroupPortDTO dto : ports) {
                    if (dto.getTargetId().equals(port.getId())) {
                        return true;
                    }
                }

                return false;
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Could not retrieve RPG with ID " + rpgId);
                return false;
            }
        });

        rpg = getNifiClient().getRemoteProcessGroupClient().getRemoteProcessGroup(rpg.getId());
        final String rpgPortId = rpg.getComponent().getContents().getInputPorts().stream()
            .filter(dto -> dto.getTargetId().equals(port.getId()))
            .findFirst() // find the port with the desired ID
            .get() // get the Port
            .getId(); // get the Port's ID

        final ConnectableDTO destination = new ConnectableDTO();
        destination.setId(rpgPortId);
        destination.setGroupId(rpg.getId());
        destination.setType("REMOTE_INPUT_PORT");

        final ConnectionEntity generateToRPG = getClientUtil().createConnection(util.createConnectableDTO(generateFlowFile), destination, "success");
        final ConnectionEntity portToCount = getClientUtil().createConnection(util.createConnectableDTO(port), util.createConnectableDTO(count), "");

        getNifiClient().getInputPortClient().startInputPort(port);
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);
        getNifiClient().getRemoteProcessGroupClient().startTransmitting(rpg);

        waitFor(() -> util.getQueueSize(generateToRPG.getId()).getObjectCount() == 0);
        waitFor(() -> util.getQueueSize(portToCount.getId()).getObjectCount() == 3 * getNumberOfNodes());
    }
}
