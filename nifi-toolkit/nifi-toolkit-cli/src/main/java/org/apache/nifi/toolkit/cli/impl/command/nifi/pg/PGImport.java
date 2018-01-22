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
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for importing a flow to NiFi from NiFi Registry.
 */
public class PGImport extends AbstractNiFiCommand {

    public PGImport() {
        super("pg-import");
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.POS_X.createOption());
        addOption(CommandOption.POS_Y.createOption());
    }

    @Override
    protected void doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String registryId = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);
        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer flowVersion = getRequiredIntArg(properties, CommandOption.FLOW_VERSION);

        Integer posX = getIntArg(properties, CommandOption.POS_X);
        if (posX == null) {
            posX = new Integer(0);
        }

        Integer posY = getIntArg(properties, CommandOption.POS_Y);
        if (posY == null) {
            posY = new Integer(0);
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

        final PositionDTO posDto = new PositionDTO();
        posDto.setX(posX.doubleValue());
        posDto.setY(posY.doubleValue());

        final ProcessGroupDTO pgDto = new ProcessGroupDTO();
        pgDto.setVersionControlInformation(versionControlInfo);

        final ProcessGroupEntity pgEntity = new ProcessGroupEntity();
        pgEntity.setComponent(pgDto);
        pgEntity.setRevision(getInitialRevisionDTO());

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final ProcessGroupEntity createdEntity = pgClient.createProcessGroup(parentPgId, pgEntity);
        println(createdEntity.getId());
    }

}
