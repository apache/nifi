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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.client.ConnectionClient;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Command to stop the components of a process group.
 */
public class PGConnect extends AbstractNiFiCommand<StringResult> {

    public PGConnect() {
        super("pg-connect", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Connects the output port of the source process group to the input port of a destination process group.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.SOURCE_PG.createOption());
        addOption(CommandOption.SOURCE_OUTPUT_PORT.createOption());
        addOption(CommandOption.DESTINATION_PG.createOption());
        addOption(CommandOption.DESTINATION_INPUT_PORT.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String sourcePgId = getRequiredArg(properties, CommandOption.SOURCE_PG);
        final String sourceOutputPort = getRequiredArg(properties, CommandOption.SOURCE_OUTPUT_PORT);
        final String destinationPgId = getRequiredArg(properties, CommandOption.DESTINATION_PG);
        final String destinationInputPort = getRequiredArg(properties, CommandOption.DESTINATION_INPUT_PORT);

        PortEntity source = null;
        PortEntity destination = null;

        final FlowClient pgClient = client.getFlowClient();

        final ProcessGroupFlowEntity sourcePgEntity = pgClient.getProcessGroup(sourcePgId);
        final ProcessGroupFlowEntity destinationPgEntity = pgClient.getProcessGroup(destinationPgId);

        final String parentPgId = sourcePgEntity.getProcessGroupFlow().getParentGroupId();
        if (!parentPgId.equals(destinationPgEntity.getProcessGroupFlow().getParentGroupId())) {
            throw new IOException("The source process group and the destination process group are not at the same level");
        }

        // retrieving the ID of the output port based on its name in source process
        // group
        Set<PortEntity> outputPorts = sourcePgEntity.getProcessGroupFlow().getFlow().getOutputPorts();
        for (PortEntity outputPort : outputPorts) {
            if (outputPort.getComponent().getName().equals(sourceOutputPort)) {
                source = outputPort;
                break;
            }
        }
        if (source == null) {
            throw new IOException("Unable to find an output port with the name '" + sourceOutputPort + "' in the source process group");
        }

        // retrieving the ID of the output port based on its name in source process
        // group
        Set<PortEntity> inputPorts = destinationPgEntity.getProcessGroupFlow().getFlow().getInputPorts();
        for (PortEntity inputPort : inputPorts) {
            if (inputPort.getComponent().getName().equals(destinationInputPort)) {
                destination = inputPort;
                break;
            }
        }
        if (destination == null) {
            throw new IOException("Unable to find an input port with the name '" + destinationInputPort + "' in the destination process group");
        }

        final ConnectionEntity connectionEntity = new ConnectionEntity();

        connectionEntity.setDestinationGroupId(destinationPgId);
        connectionEntity.setDestinationId(destination.getId());
        connectionEntity.setDestinationType(destination.getPortType());

        connectionEntity.setSourceGroupId(sourcePgId);
        connectionEntity.setSourceId(source.getId());
        connectionEntity.setSourceType(source.getPortType());

        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        connectionEntity.setRevision(revisionDto);

        final ConnectionDTO connectionDto = new ConnectionDTO();
        connectionDto.setDestination(createConnectableDTO(destination));
        connectionDto.setSource(createConnectableDTO(source));
        connectionDto.setParentGroupId(parentPgId);
        connectionEntity.setComponent(connectionDto);

        final ConnectionClient connectionClient = client.getConnectionClient();
        final ConnectionEntity createdEntity = connectionClient.createConnection(parentPgId, connectionEntity);
        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }

    private ConnectableDTO createConnectableDTO(final PortEntity port) {
        final ConnectableDTO dto = new ConnectableDTO();
        dto.setGroupId(port.getComponent().getParentGroupId());
        dto.setId(port.getId());
        dto.setName(port.getComponent().getName());
        dto.setRunning("RUNNING".equalsIgnoreCase(port.getComponent().getState()));
        dto.setType(port.getPortType());
        return dto;
    }

}
