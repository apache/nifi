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
package org.apache.nifi.toolkit.cli.impl.command.registry.flow;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Deletes a flow from the given registry.
 */
public class DeleteFlow extends AbstractNiFiRegistryCommand {

    public DeleteFlow() {
        super("delete-flow");
    }

    @Override
    public String getDescription() {
        return "Deletes a flow from the registry. If the flow has one or more versions then the force option must be used.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FORCE.createOption());
    }

    @Override
    protected void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final boolean forceDelete = properties.containsKey(CommandOption.FORCE.getLongName());

        final String bucketId = getBucketId(client, flowId);

        final FlowSnapshotClient flowSnapshotClient = client.getFlowSnapshotClient();
        final List<VersionedFlowSnapshotMetadata> snapshotMetadata = flowSnapshotClient.getSnapshotMetadata(bucketId, flowId);

        if (snapshotMetadata != null && snapshotMetadata.size() > 0 && !forceDelete) {
            throw new NiFiRegistryException("Flow has versions, use --" + CommandOption.FORCE.getLongName() + " to delete");
        } else {
            final FlowClient flowClient = client.getFlowClient();
            flowClient.delete(bucketId, flowId);
        }
    }
}
