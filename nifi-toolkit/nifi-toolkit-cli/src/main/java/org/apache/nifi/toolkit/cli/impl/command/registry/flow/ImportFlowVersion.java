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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Imports a version of a flow to specific bucket and flow in a given registry.
 */
public class ImportFlowVersion extends AbstractNiFiRegistryCommand<StringResult> {

    public ImportFlowVersion() {
        super("import-flow-version", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Imports a version of a flow from a local file, or a public URL. " +
                "The imported version automatically becomes the next version of the given flow.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_SOURCE);

        String contents;
        try {
            // try a public resource URL
            URL url = new URL(inputFile);
            contents = IOUtils.toString(url, StandardCharsets.UTF_8);
        } catch (MalformedURLException e) {
            // assume a local file then
            URI uri = Paths.get(inputFile).toAbsolutePath().toUri();
            contents = IOUtils.toString(uri, StandardCharsets.UTF_8);
        }

        final FlowClient flowClient = client.getFlowClient();
        final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();

        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final VersionedFlowSnapshot deserializedSnapshot = objectMapper.readValue(contents, VersionedFlowSnapshot.class);
        if (deserializedSnapshot == null) {
            throw new IOException("Unable to deserialize flow version from " + inputFile);
        }

        final VersionedFlow versionedFlow = flowClient.get(flowId);

        // determine the latest existing version in the destination system
        Integer version;
        try {
            final VersionedFlowSnapshotMetadata latestMetadata = snapshotClient.getLatestMetadata(flowId);
            version = latestMetadata.getVersion() + 1;
        } catch (NiFiRegistryException e) {
            // when there are no versions it produces a 404 not found
            version = 1;
        }

        // create new metadata using the passed in bucket and flow in the target registry, keep comments
        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setBucketIdentifier(versionedFlow.getBucketIdentifier());
        metadata.setFlowIdentifier(flowId);
        metadata.setVersion(version);
        metadata.setComments(deserializedSnapshot.getSnapshotMetadata().getComments());

        // create a new snapshot using the new metadata and the contents from the deserialized snapshot
        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setSnapshotMetadata(metadata);
        snapshot.setFlowContents(deserializedSnapshot.getFlowContents());


        final VersionedFlowSnapshot createdSnapshot = snapshotClient.create(snapshot);
        final VersionedFlowSnapshotMetadata createdMetadata = createdSnapshot.getSnapshotMetadata();

        return new StringResult(String.valueOf(createdMetadata.getVersion()), getContext().isInteractive());
    }

}
