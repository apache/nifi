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
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.VersionsClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VersionedFlowSnapshotMetadataSetResult;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get all the available versions for a given process group that is under version control.
 */
public class PGGetAllVersions extends AbstractNiFiCommand<VersionedFlowSnapshotMetadataSetResult> {

    public PGGetAllVersions() {
        super("pg-get-all-versions", VersionedFlowSnapshotMetadataSetResult.class);
    }
    @Override
    public String getDescription() {
        return "Returns all of the available versions for a version controlled process group.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public VersionedFlowSnapshotMetadataSetResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final VersionsClient versionsClient = client.getVersionsClient();
        final VersionControlInformationEntity existingVersionControlInfo = versionsClient.getVersionControlInfo(pgId);
        final VersionControlInformationDTO existingVersionControlDTO = existingVersionControlInfo.getVersionControlInformation();

        if (existingVersionControlDTO == null) {
            throw new NiFiClientException("Process group is not under version control");
        }

        final String registryId = existingVersionControlDTO.getRegistryId();
        final String bucketId = existingVersionControlDTO.getBucketId();
        final String flowId = existingVersionControlDTO.getFlowId();

        final FlowClient flowClient = client.getFlowClient();
        final VersionedFlowSnapshotMetadataSetEntity versions = flowClient.getVersions(registryId, bucketId, flowId);

        if (versions.getVersionedFlowSnapshotMetadataSet() == null || versions.getVersionedFlowSnapshotMetadataSet().isEmpty()) {
            throw new NiFiClientException("No versions available");
        }

        return new VersionedFlowSnapshotMetadataSetResult(getResultType(properties), versions);
    }

}
