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
package org.apache.nifi.toolkit.cli.impl.command.nifi.registry;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.RegistryFlowVersionsResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Lists flow versions for a given flow in a given branch and bucket seen by a
 * given registry client
 */
public class ListFlowVersions extends AbstractNiFiCommand<RegistryFlowVersionsResult> {

    public ListFlowVersions() {
        super("list-flow-versions", RegistryFlowVersionsResult.class);
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.FLOW_BRANCH.createOption());
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
    }

    @Override
    public String getDescription() {
        return "Returns the list of flow versions for a given flow in a given branch and bucket seen by the specified registry client.";
    }

    @Override
    public RegistryFlowVersionsResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {
        final String regClientId = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);
        final String branchName = getRequiredArg(properties, CommandOption.FLOW_BRANCH);
        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final VersionedFlowSnapshotMetadataSetEntity flowVersions = client.getFlowClient().getVersions(regClientId, bucketId, flowId, branchName);
        return new RegistryFlowVersionsResult(getResultType(properties), flowVersions);
    }

}
