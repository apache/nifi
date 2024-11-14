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
package org.apache.nifi.toolkit.cli.impl.command.nifi.flow;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.VersionedReportingTaskSnapshotResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;

import java.io.IOException;
import java.util.Properties;

public class ExportReportingTask extends AbstractNiFiCommand<VersionedReportingTaskSnapshotResult> {

    public ExportReportingTask() {
        super("export-reporting-task", VersionedReportingTaskSnapshotResult.class);
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.RT_ID.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
    }

    @Override
    public String getDescription() {
        return "Exports a snapshot of the specified reporting task and any management controller services used by the reporting task. " +
                " The --" + CommandOption.OUTPUT_FILE.getLongName() + " can be used to export to a file, " +
                "otherwise the content will be written to terminal or standard out.";
    }

    @Override
    public VersionedReportingTaskSnapshotResult doExecute(final NiFiClient client, final Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String reportingTaskId = getRequiredArg(properties, CommandOption.RT_ID);
        final VersionedReportingTaskSnapshot snapshot = client.getFlowClient().getReportingTaskSnapshot(reportingTaskId);

        // currently export doesn't use the ResultWriter concept, it always writes JSON
        // destination will be a file if outputFile is specified, otherwise it will be
        // the output stream of the CLI
        final String outputFile;
        if (properties.containsKey(CommandOption.OUTPUT_FILE.getLongName())) {
            outputFile = properties.getProperty(CommandOption.OUTPUT_FILE.getLongName());
        } else {
            outputFile = null;
        }

        return new VersionedReportingTaskSnapshotResult(snapshot, outputFile);
    }
}
