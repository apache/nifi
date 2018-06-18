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
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.VersionedFlowSnapshotResult;

import java.io.IOException;
import java.util.Properties;

public class ExportFlowVersion extends AbstractNiFiRegistryCommand<VersionedFlowSnapshotResult> {

    public ExportFlowVersion() {
        super("export-flow-version", VersionedFlowSnapshotResult.class);
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
    }

    @Override
    public String getDescription() {
        return "Exports a specific version of a flow. The --" + CommandOption.OUTPUT_FILE.getLongName()
                + " can be used to export to a file, otherwise the content will be written to terminal or standard out.";
    }

    @Override
    public VersionedFlowSnapshotResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version = getIntArg(properties, CommandOption.FLOW_VERSION);

        // if no version was provided then export the latest, otherwise use specific version
        final VersionedFlowSnapshot versionedFlowSnapshot;
        if (version == null) {
            versionedFlowSnapshot = client.getFlowSnapshotClient().getLatest(flowId);
        } else {
            versionedFlowSnapshot = client.getFlowSnapshotClient().get(flowId, version);
        }

        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setFlowIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setLink(null);

        // currently export doesn't use the ResultWriter concept, it always writes JSON
        // destination will be a file if outputFile is specified, otherwise it will be the output stream of the CLI
        final String outputFile;
        if (properties.containsKey(CommandOption.OUTPUT_FILE.getLongName())) {
            outputFile = properties.getProperty(CommandOption.OUTPUT_FILE.getLongName());
        } else {
            outputFile = null;
        }

        return new VersionedFlowSnapshotResult(versionedFlowSnapshot, outputFile);
    }

}
