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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ImportReportingTasksResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;

import java.io.IOException;
import java.util.Properties;

public class ImportReportingTasks extends AbstractNiFiCommand<ImportReportingTasksResult> {

    public ImportReportingTasks() {
        super("import-reporting-tasks", ImportReportingTasksResult.class);
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public String getDescription() {
        return "Imports the contents of a reporting task snapshot produced from export-reporting-tasks or export-reporting-task.";
    }

    @Override
    public ImportReportingTasksResult doExecute(final NiFiClient client, final Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_SOURCE);
        final String contents = getInputSourceContent(inputFile);

        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final VersionedReportingTaskSnapshot reportingTaskSnapshot = objectMapper.readValue(contents, VersionedReportingTaskSnapshot.class);
        if (reportingTaskSnapshot == null) {
            throw new IOException("Unable to deserialize reporting task snapshot from " + inputFile);
        }

        final VersionedReportingTaskImportRequestEntity importRequestEntity = new VersionedReportingTaskImportRequestEntity();
        importRequestEntity.setReportingTaskSnapshot(reportingTaskSnapshot);

        final VersionedReportingTaskImportResponseEntity importResponseEntity = client.getControllerClient().importReportingTasks(importRequestEntity);
        return new ImportReportingTasksResult(getResultType(properties), importResponseEntity);
    }
}
