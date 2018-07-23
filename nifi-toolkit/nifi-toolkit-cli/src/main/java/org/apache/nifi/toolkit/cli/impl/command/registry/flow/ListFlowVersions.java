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
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.VersionedFlowSnapshotMetadataResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Lists the metadata for the versions of a specific flow in a specific bucket.
 */
public class ListFlowVersions extends AbstractNiFiRegistryCommand<VersionedFlowSnapshotMetadataResult> {

    public ListFlowVersions() {
        super("list-flow-versions", VersionedFlowSnapshotMetadataResult.class);
    }

    @Override
    public String getDescription() {
        return "Lists all of the flows for the given bucket.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
    }

    @Override
    public VersionedFlowSnapshotMetadataResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String flow = getRequiredArg(properties, CommandOption.FLOW_ID);

        final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();
        final List<VersionedFlowSnapshotMetadata> snapshotMetadata = snapshotClient.getSnapshotMetadata(flow);
        return new VersionedFlowSnapshotMetadataResult(getResultType(properties), snapshotMetadata);
    }

}
