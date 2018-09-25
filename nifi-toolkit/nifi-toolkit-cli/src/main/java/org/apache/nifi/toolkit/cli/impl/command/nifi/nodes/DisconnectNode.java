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
package org.apache.nifi.toolkit.cli.impl.command.nifi.nodes;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.NodeResult;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.NodeEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for disconnecting a node from the NiFi cluster.
 */
public class DisconnectNode extends AbstractNiFiCommand<NodeResult> {

    public DisconnectNode() {
        super("disconnect-node", NodeResult.class);
    }

    @Override
    public String getDescription() {
        return "Disconnects a node from the NiFi cluster.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.NIFI_NODE_ID.createOption());
    }

    @Override
    public NodeResult doExecute(NiFiClient client, Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String nodeId = getRequiredArg(properties, CommandOption.NIFI_NODE_ID);
        final ControllerClient controllerClient = client.getControllerClient();

        NodeDTO nodeDto = new NodeDTO();
        nodeDto.setNodeId(nodeId);
        // TODO There are no constants for the DISCONNECT node status
        nodeDto.setStatus("DISCONNECTING");
        NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setNode(nodeDto);
        NodeEntity nodeEntityResult = controllerClient.disconnectNode(nodeId, nodeEntity);
        return new NodeResult(getResultType(properties), nodeEntityResult);
    }
}
