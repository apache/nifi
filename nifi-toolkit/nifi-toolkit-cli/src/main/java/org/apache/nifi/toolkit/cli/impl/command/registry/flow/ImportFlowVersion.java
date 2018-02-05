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
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Imports a version of a flow to specific bucket and flow in a given registry.
 */
public class ImportFlowVersion extends AbstractNiFiRegistryCommand {

    public ImportFlowVersion() {
        super("import-flow-version");
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.INPUT_FILE.createOption());
    }

    @Override
    protected void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_FILE);

        try (final FileInputStream in = new FileInputStream(inputFile)) {
            final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();

            final VersionedFlowSnapshot deserializedSnapshot = MAPPER.readValue(in, VersionedFlowSnapshot.class);
            if (deserializedSnapshot == null) {
                throw new IOException("Unable to deserialize flow version from " + inputFile);
            }

            // determine the bucket for the provided flow id
            final String bucketId = getBucketId(client, flowId);

            // determine the latest existing version in the destination system
            Integer version;
            try {
                final VersionedFlowSnapshotMetadata latestMetadata = snapshotClient.getLatestMetadata(bucketId, flowId);
                version = latestMetadata.getVersion() + 1;
            } catch (NiFiRegistryException e) {
                // when there are no versions it produces a 404 not found
                version = new Integer(1);
            }

            // create new metadata using the passed in bucket and flow in the target registry, keep comments
            final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
            metadata.setBucketIdentifier(bucketId);
            metadata.setFlowIdentifier(flowId);
            metadata.setVersion(version);
            metadata.setComments(deserializedSnapshot.getSnapshotMetadata().getComments());

            // create a new snapshot using the new metadata and the contents from the deserialized snapshot
            final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
            snapshot.setSnapshotMetadata(metadata);
            snapshot.setFlowContents(deserializedSnapshot.getFlowContents());


            final VersionedFlowSnapshot createdSnapshot = snapshotClient.create(snapshot);
            final VersionedFlowSnapshotMetadata createdMetadata = createdSnapshot.getSnapshotMetadata();

            println(String.valueOf(createdMetadata.getVersion()));
        }
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
