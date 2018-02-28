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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupBox;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Command for importing a flow to NiFi from NiFi Registry.
 */
public class PGImport extends AbstractNiFiCommand<StringResult> {

    public PGImport() {
        super("pg-import", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a new process group by importing a versioned flow from a registry. If no process group id is " +
                "specified, then the created process group will be placed in the root group. If only one registry client " +
                "exists in NiFi, then it does not need to be specified and will be automatically selected.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer flowVersion = getRequiredIntArg(properties, CommandOption.FLOW_VERSION);

        // if a registry client is specified use it, otherwise see if there is only one available and use that,
        // if more than one is available then throw an exception because we don't know which one to use
        String registryId = getArg(properties, CommandOption.REGISTRY_CLIENT_ID);
        if (StringUtils.isBlank(registryId)) {
            final RegistryClientsEntity registries = client.getControllerClient().getRegistryClients();

            final Set<RegistryClientEntity> entities = registries.getRegistries();
            if (entities == null || entities.isEmpty()) {
                throw new NiFiClientException("No registry clients available");
            }

            if (entities.size() == 1) {
                registryId = entities.stream().findFirst().get().getId();
            } else {
                throw new MissingOptionException(CommandOption.REGISTRY_CLIENT_ID.getLongName()
                        + " must be provided when there is more than one available");
            }
        }

        // get the optional id of the parent PG, otherwise fallback to the root group
        String parentPgId = getArg(properties, CommandOption.PG_ID);
        if (StringUtils.isBlank(parentPgId)) {
            final FlowClient flowClient = client.getFlowClient();
            parentPgId = flowClient.getRootGroupId();
        }

        final VersionControlInformationDTO versionControlInfo = new VersionControlInformationDTO();
        versionControlInfo.setRegistryId(registryId);
        versionControlInfo.setBucketId(bucketId);
        versionControlInfo.setFlowId(flowId);
        versionControlInfo.setVersion(flowVersion);

        final ProcessGroupBox pgBox = client.getFlowClient().getSuggestedProcessGroupCoordinates(parentPgId);

        final PositionDTO posDto = new PositionDTO();
        posDto.setX(Integer.valueOf(pgBox.getX()).doubleValue());
        posDto.setY(Integer.valueOf(pgBox.getY()).doubleValue());

        final ProcessGroupDTO pgDto = new ProcessGroupDTO();
        pgDto.setVersionControlInformation(versionControlInfo);
        pgDto.setPosition(posDto);

        final ProcessGroupEntity pgEntity = new ProcessGroupEntity();
        pgEntity.setComponent(pgDto);
        pgEntity.setRevision(getInitialRevisionDTO());

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final ProcessGroupEntity createdEntity = pgClient.createProcessGroup(parentPgId, pgEntity);
        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }

}
