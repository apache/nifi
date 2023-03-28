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
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.ListBuckets;
import org.apache.nifi.toolkit.cli.impl.result.registry.VersionedFlowSnapshotsResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExportAllFlows extends AbstractNiFiRegistryCommand<VersionedFlowSnapshotsResult> {
    private static final String ALL_BUCKETS_COLLECTED = "All buckets collected...";
    private static final String ALL_FLOWS_COLLECTED = "All flows collected...";
    private static final String ALL_FLOW_VERSIONS_COLLECTED = "All flow versions collected...";
    private static final String EXPORTING_FLOW_VERSIONS = "Exporting flow versions...";
    private final ListBuckets listBuckets;
    private final ListFlows listFlows;
    private final ListFlowVersions listFlowVersions;
    private final ExportFlowVersion exportFlowVersion;

    public ExportAllFlows() {
        super("export-all-flows", VersionedFlowSnapshotsResult.class);
        this.listBuckets = new ListBuckets();
        this.listFlows = new ListFlows();
        this.listFlowVersions = new ListFlowVersions();
        this.exportFlowVersion = new ExportFlowVersion();
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.OUTPUT_DIR.createOption());

        listBuckets.initialize(context);
        listFlows.initialize(context);
        listFlowVersions.initialize(context);
        exportFlowVersion.initialize(context);
    }

    @Override
    public String getDescription() {
        return "List all the buckets, for each bucket, list all the flows, for each flow, list all versions and export each version." +
                "Versions will be saved in the provided target directory.";
    }

    @Override
    public VersionedFlowSnapshotsResult doExecute(NiFiRegistryClient client, Properties properties) throws IOException, NiFiRegistryException, ParseException, CommandException {
        final String outputDirectory = getRequiredArg(properties, CommandOption.OUTPUT_DIR);
        final boolean isInteractive = getContext().isInteractive();

        // Gather all buckets and create a map for quick access by bucket id
        final Map<String, Bucket> bucketMap = getBucketMap(client, isInteractive);

        // Gather all flows and create a map for quick access by flow id
        final Map<String, VersionedFlow> flowMap = getFlowMap(client, bucketMap, isInteractive);

        // Gather all versions for all the flows
        final List<VersionedFlowSnapshotMetadata> versionedFlowSnapshotMetadataList = getVersionedFlowSnapshotMetadataList(client, flowMap, isInteractive);

        // Prepare flow version exports
        final List<VersionedFlowSnapshot> versionedFlowSnapshotList = getVersionedFlowSnapshotResults(client, outputDirectory, bucketMap, flowMap, versionedFlowSnapshotMetadataList, isInteractive);

        // Export all flow versions
        return new VersionedFlowSnapshotsResult(versionedFlowSnapshotList, outputDirectory);
    }

    private Map<String, Bucket> getBucketMap(final NiFiRegistryClient client, final boolean isInteractive) throws IOException, NiFiRegistryException {
        printMessage(isInteractive, ALL_BUCKETS_COLLECTED);

        return listBuckets.doExecute(client, new Properties())
                .getResult()
                .stream()
                .collect(Collectors.toMap(Bucket::getIdentifier, Function.identity()));
    }

    private Map<String, VersionedFlow> getFlowMap(final NiFiRegistryClient client, final Map<String, Bucket> bucketMap,
                                                  final boolean isInteractive) throws IOException, NiFiRegistryException, ParseException {
        printMessage(isInteractive, ALL_FLOWS_COLLECTED);

        return getFlows(client, bucketMap)
                .stream()
                .collect(Collectors.toMap(VersionedFlow::getIdentifier, Function.identity()));
    }

    private List<VersionedFlow> getFlows(final NiFiRegistryClient client, final Map<String, Bucket> bucketMap) throws ParseException, IOException, NiFiRegistryException {
        final List<VersionedFlow> versionedFlowList = new ArrayList<>();

        for (final String bucketId : bucketMap.keySet()) {
            final Properties listFlowProperties = new Properties();
            listFlowProperties.setProperty(CommandOption.BUCKET_ID.getLongName(), bucketId);

            versionedFlowList.addAll(listFlows.doExecute(client, listFlowProperties).getResult());
        }
        return versionedFlowList;
    }

    private List<VersionedFlowSnapshotMetadata> getVersionedFlowSnapshotMetadataList(final NiFiRegistryClient client, final Map<String, VersionedFlow> flowMap,
                                                                                     final boolean isInteractive) throws ParseException, IOException, NiFiRegistryException {
        final List<VersionedFlowSnapshotMetadata> versionedFlowSnapshotMetadataList =  new ArrayList<>();

        for (final String flowId : flowMap.keySet()) {
            final Properties listFlowVersionsProperties = new Properties();
            listFlowVersionsProperties.setProperty(CommandOption.FLOW_ID.getLongName(), flowId);

            versionedFlowSnapshotMetadataList.addAll(listFlowVersions.doExecute(client, listFlowVersionsProperties).getResult());
        }

        printMessage(isInteractive, ALL_FLOW_VERSIONS_COLLECTED);

        return versionedFlowSnapshotMetadataList;
    }

    private List<VersionedFlowSnapshot> getVersionedFlowSnapshotResults(final NiFiRegistryClient client, final String outputDirectory, final Map<String, Bucket> bucketMap,
                                        final Map<String, VersionedFlow> flowMap, final List<VersionedFlowSnapshotMetadata> versionedFlowSnapshotMetadataList,
                                                                        final boolean isInteractive) throws ParseException, IOException, NiFiRegistryException {
        final List<VersionedFlowSnapshot> versionedFlowSnapshotList =  new ArrayList<>();

        for (final VersionedFlowSnapshotMetadata metaData : versionedFlowSnapshotMetadataList) {
            final Properties exportFlowVersionProperties = new Properties();
            exportFlowVersionProperties.setProperty(CommandOption.FLOW_ID.getLongName(), metaData.getFlowIdentifier());
            exportFlowVersionProperties.setProperty(CommandOption.FLOW_VERSION.getLongName(), Integer.toString(metaData.getVersion()));
            exportFlowVersionProperties.setProperty(CommandOption.OUTPUT_FILE.getLongName(), outputDirectory);

            final VersionedFlowSnapshot exportedSnapshot = exportFlowVersion.doExecute(client, exportFlowVersionProperties).getResult();

            final VersionedFlow flow = new VersionedFlow();
            flow.setIdentifier(metaData.getFlowIdentifier());
            flow.setName(flowMap.get(metaData.getFlowIdentifier()).getName());
            flow.setDescription(flowMap.get(metaData.getFlowIdentifier()).getDescription());
            exportedSnapshot.setFlow(flow);

            final Bucket bucket = new Bucket();
            bucket.setIdentifier(metaData.getBucketIdentifier());
            bucket.setName(bucketMap.get(metaData.getBucketIdentifier()).getName());
            bucket.setDescription(bucketMap.get(metaData.getBucketIdentifier()).getDescription());
            exportedSnapshot.setBucket(bucket);

            versionedFlowSnapshotList.add(exportedSnapshot);
        }

        printMessage(isInteractive, EXPORTING_FLOW_VERSIONS);

        return versionedFlowSnapshotList;
    }

    private void printMessage(final boolean isInteractive, final String message) {
        if (isInteractive) {
            println();
            println(message);
            println();
        }
    }
}
