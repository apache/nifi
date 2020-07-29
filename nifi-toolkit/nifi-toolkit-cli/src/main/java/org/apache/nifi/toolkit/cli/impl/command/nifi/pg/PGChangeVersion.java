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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.VersionsClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to change the version of a version controlled process group.
 */
public class PGChangeVersion extends AbstractNiFiCommand<VoidResult> {

    public PGChangeVersion() {
        super("pg-change-version", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Changes the version for a version controlled process group. " +
                "This can be used to upgrade to a new version, or revert to a previous version. " +
                "If no version is specified, the latest version will be used.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final VersionsClient versionsClient = client.getVersionsClient();
        final VersionControlInformationEntity existingVersionControlInfo = versionsClient.getVersionControlInfo(pgId);
        final VersionControlInformationDTO existingVersionControlDTO = existingVersionControlInfo.getVersionControlInformation();

        if (existingVersionControlDTO == null) {
            throw new NiFiClientException("Process group is not under version control");
        }

        // start with the version specified in the arguments
        Integer newVersion = getIntArg(properties, CommandOption.FLOW_VERSION);

        // if no version was specified, automatically determine the latest and change to that
        if (newVersion == null) {
            newVersion = getLatestVersion(client, existingVersionControlDTO);

            if (newVersion.intValue() == existingVersionControlDTO.getVersion().intValue()) {
                throw new NiFiClientException("Process group already at latest version");
            }
        }

        // update the version in the existing DTO to the new version so we can submit it back
        existingVersionControlDTO.setVersion(newVersion);

        // initiate the version change which creates an update request that must be checked for completion
        final VersionedFlowUpdateRequestEntity initialUpdateRequest = versionsClient.updateVersionControlInfo(pgId, existingVersionControlInfo);

        // poll the update request for up to 30 seconds to see if it has completed
        // if it doesn't complete then an exception will be thrown, but in either case the request will be deleted
        final String updateRequestId = initialUpdateRequest.getRequest().getRequestId();
        try {
            boolean completed = false;
            for (int i = 0; i < 30; i++) {
                final VersionedFlowUpdateRequestEntity updateRequest = versionsClient.getUpdateRequest(updateRequestId);
                if (updateRequest != null && updateRequest.getRequest().isComplete()) {
                    completed = true;
                    break;
                } else {
                    try {
                        if (getContext().isInteractive()) {
                            println("Waiting for update request to complete...");
                        }
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (!completed) {
                throw new NiFiClientException("Unable to change version of process group, cancelling request");
            }

        } finally {
            versionsClient.deleteUpdateRequest(updateRequestId);
        }

        return VoidResult.getInstance();
    }

    private int getLatestVersion(final NiFiClient client, final VersionControlInformationDTO existingVersionControlDTO)
            throws NiFiClientException, IOException {
        final FlowClient flowClient = client.getFlowClient();

        final String registryId = existingVersionControlDTO.getRegistryId();
        final String bucketId = existingVersionControlDTO.getBucketId();
        final String flowId = existingVersionControlDTO.getFlowId();

        final VersionedFlowSnapshotMetadataSetEntity versions = flowClient.getVersions(registryId, bucketId, flowId);
        if (versions.getVersionedFlowSnapshotMetadataSet() == null || versions.getVersionedFlowSnapshotMetadataSet().isEmpty()) {
            throw new NiFiClientException("No versions available");
        }

        int latestVersion = 1;
        for (VersionedFlowSnapshotMetadataEntity version : versions.getVersionedFlowSnapshotMetadataSet()) {
            if (version.getVersionedFlowSnapshotMetadata().getVersion() > latestVersion) {
                latestVersion = version.getVersionedFlowSnapshotMetadata().getVersion();
            }
        }
        return latestVersion;
    }

}
