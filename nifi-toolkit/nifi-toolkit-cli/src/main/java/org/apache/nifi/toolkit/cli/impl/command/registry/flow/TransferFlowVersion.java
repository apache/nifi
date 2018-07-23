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
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.OkResult;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class TransferFlowVersion extends AbstractNiFiRegistryCommand<StringResult> {

    public TransferFlowVersion() {
        super("transfer-flow-version", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Transfers a version of a flow directly from one flow to another, without needing to export/import. " +
                "If --" + CommandOption.SRC_PROPS.getLongName() + " is not specified, the source flow is " +
                "assumed to be in the same registry as the destination flow. " +
                "If --" + CommandOption.SRC_FLOW_VERSION.getLongName() + " is not specified, then the latest " +
                "version will be transferred.";
    }

    @Override
    protected void doInitialize(final Context context) {
        // source properties
        addOption(CommandOption.SRC_PROPS.createOption());

        // source flow id
        addOption(CommandOption.SRC_FLOW_ID.createOption());

        // optional version of source flow, otherwise latest
        addOption(CommandOption.SRC_FLOW_VERSION.createOption());

        // destination flow id
        addOption(CommandOption.FLOW_ID.createOption());

        // destination properties will come from standard -p or nifi.reg.props in session
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String srcPropsValue = getArg(properties, CommandOption.SRC_PROPS);
        final String srcFlowId  = getRequiredArg(properties, CommandOption.SRC_FLOW_ID);
        final Integer srcFlowVersion = getIntArg(properties, CommandOption.SRC_FLOW_VERSION);
        final String destFlowId = getRequiredArg(properties, CommandOption.FLOW_ID);

        final NiFiRegistryClient srcClient = getSourceClient(client, srcPropsValue);

        // get the snapshot of the source flow, either the version specified or the latest
        final VersionedFlowSnapshot srcSnapshot;
        if (srcFlowVersion == null) {
            srcSnapshot = srcClient.getFlowSnapshotClient().getLatest(srcFlowId);
        } else {
            srcSnapshot = srcClient.getFlowSnapshotClient().get(srcFlowId, srcFlowVersion);
        }

        final Integer srcSnapshotFlowVersion = srcSnapshot.getSnapshotMetadata().getVersion();

        // get the destination flow
        final VersionedFlow destFlow;
        try {
            destFlow = client.getFlowClient().get(destFlowId);
        } catch (Exception e) {
            throw new NiFiRegistryException("Error retrieving destination flow : " + e.getMessage(), e);
        }

        // determine the next version number for the destination flow
        final List<Integer> destVersions = getVersions(client, destFlow.getIdentifier());
        final Integer destFlowVersion = destVersions.isEmpty() ? 1 : destVersions.get(0) + 1;

        // create the new metadata for the destination snapshot
        final VersionedFlowSnapshotMetadata destMetadata = new VersionedFlowSnapshotMetadata();
        destMetadata.setBucketIdentifier(destFlow.getBucketIdentifier());
        destMetadata.setFlowIdentifier(destFlowId);
        destMetadata.setVersion(destFlowVersion);
        destMetadata.setComments(srcSnapshot.getSnapshotMetadata().getComments());

        // update the source snapshot with the destination metadata
        srcSnapshot.setFlow(null);
        srcSnapshot.setBucket(null);
        srcSnapshot.setSnapshotMetadata(destMetadata);

        // create the destination snapshot
        client.getFlowSnapshotClient().create(srcSnapshot);

        if (getContext().isInteractive()) {
            println();
            println("Transferred version " + srcSnapshotFlowVersion
                    + " of source flow to version " + destFlowVersion + " of destination flow");
        }

        return new OkResult(getContext().isInteractive());
    }


}
