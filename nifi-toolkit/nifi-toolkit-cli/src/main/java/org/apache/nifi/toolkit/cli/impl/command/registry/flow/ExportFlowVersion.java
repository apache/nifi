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
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ExportFlowVersion extends AbstractNiFiRegistryCommand {

    public ExportFlowVersion() {
        super("export-flow-version");
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
    }

    @Override
    public void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version = getIntArg(properties, CommandOption.FLOW_VERSION);

        // determine the bucket for the provided flow id
        final String bucketId = getBucketId(client, flowId);

        // if no version was provided then export the latest, otherwise use specific version
        final VersionedFlowSnapshot versionedFlowSnapshot;
        if (version == null) {
            versionedFlowSnapshot = client.getFlowSnapshotClient().getLatest(bucketId, flowId);
        } else {
            versionedFlowSnapshot = client.getFlowSnapshotClient().get(bucketId, flowId, version);
        }

        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setFlowIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setLink(null);

        writeResult(properties, versionedFlowSnapshot);
    }

    /*
     * NOTE: This will bring back every item in the registry. We should create an end-point on the registry side
     * to retrieve a flow by id and remove this later.
     */
    private String getBucketId(final NiFiRegistryClient client, final String flowId) throws IOException, NiFiRegistryException {
        final List<BucketItem> items = client.getItemsClient().getAll();

        final Optional<BucketItem> matchingItem = items.stream()
                .filter(i ->  i.getIdentifier().equals(flowId))
                .findFirst();

        if (!matchingItem.isPresent()) {
            throw new NiFiRegistryException("Versioned flow does not exist with id " + flowId);
        }

        return matchingItem.get().getBucketIdentifier();
    }

}
