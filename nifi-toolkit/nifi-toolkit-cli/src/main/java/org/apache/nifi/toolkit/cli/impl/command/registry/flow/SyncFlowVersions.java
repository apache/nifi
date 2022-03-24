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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SyncFlowVersions extends AbstractNiFiRegistryCommand<StringResult> {

    public SyncFlowVersions() {
        super("sync-flow-versions", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Syncs the versions of a flow to another flow, which could be in a different bucket or registry. " +
                "This command assumes the intent is to maintain the exact version history across the two flows. " +
                "The list of versions from the source flow will be compared to the destination flow, and any " +
                "versions not present will be added. If --" + CommandOption.SRC_PROPS.getLongName() + " is not " +
                "provided then the source registry will be assumed to be the same as the destination registry.";
    }

    @Override
    protected void doInitialize(final Context context) {
        // source properties
        addOption(CommandOption.SRC_PROPS.createOption());

        // source flow id
        addOption(CommandOption.SRC_FLOW_ID.createOption());

        // destination flow id
        addOption(CommandOption.FLOW_ID.createOption());

        // destination properties will come from standard -p or nifi.reg.props in session
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String srcPropsValue = getArg(properties, CommandOption.SRC_PROPS);
        final String srcFlowId  = getRequiredArg(properties, CommandOption.SRC_FLOW_ID);
        final String destFlowId = getRequiredArg(properties, CommandOption.FLOW_ID);

        final NiFiRegistryClient srcClient = getSourceClient(client, srcPropsValue);

        // ensure source flow exists

        final VersionedFlow srcFlow;
        try {
            srcFlow = srcClient.getFlowClient().get(srcFlowId);
        } catch (Exception e) {
            throw new NiFiRegistryException("Error retrieving source flow : " + e.getMessage(), e);
        }

        // ensure destination flow exists

        final VersionedFlow destFlow;
        try {
            destFlow = client.getFlowClient().get(destFlowId);
        } catch (Exception e) {
            throw new NiFiRegistryException("Error retrieving destination flow : " + e.getMessage(), e);
        }

        // get version list for source and dest

        final List<Integer> srcVersions = getVersions(srcClient, srcFlow.getIdentifier());
        final List<Integer> destVersions = getVersions(client, destFlow.getIdentifier());

        if (destVersions.size() > srcVersions.size()) {
            throw new NiFiRegistryException("Destination flow has more versions than source flow");
        }

        srcVersions.removeAll(destVersions);

        if (srcVersions.isEmpty()) {
            if (getContext().isInteractive()) {
                println();
                println("Source and destination already in sync");
            }
            return new OkResult(getContext().isInteractive());
        }

        // the REST API returns versions in decreasing order, but we want them in increasing order
        Collections.sort(srcVersions);

        for (final Integer srcVersion : srcVersions) {
            final VersionedFlowSnapshot srcFlowSnapshot = srcClient.getFlowSnapshotClient().get(srcFlowId, srcVersion);
            srcFlowSnapshot.setFlow(null);
            srcFlowSnapshot.setBucket(null);

            final VersionedFlowSnapshotMetadata destMetadata = new VersionedFlowSnapshotMetadata();
            destMetadata.setBucketIdentifier(destFlow.getBucketIdentifier());
            destMetadata.setFlowIdentifier(destFlowId);
            destMetadata.setVersion(srcVersion);
            destMetadata.setComments(srcFlowSnapshot.getSnapshotMetadata().getComments());

            srcFlowSnapshot.setSnapshotMetadata(destMetadata);
            client.getFlowSnapshotClient().create(srcFlowSnapshot);

            if (getContext().isInteractive()) {
                println();
                println("Synced version " + srcVersion);
            }
        }

        return new OkResult(getContext().isInteractive());
    }

}
