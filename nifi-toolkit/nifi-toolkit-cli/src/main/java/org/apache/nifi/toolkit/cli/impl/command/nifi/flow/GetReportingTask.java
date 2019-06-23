/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ReportingTasksClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ReportingTaskResult;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for retrieving the status of a reporting task.
 */
public class GetReportingTask extends AbstractNiFiCommand<ReportingTaskResult> {

    public GetReportingTask() {
        super("get-reporting-task", ReportingTaskResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the status for a reporting task.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.RT_ID.createOption());
    }

    @Override
    public ReportingTaskResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String rtId = getRequiredArg(properties, CommandOption.RT_ID);
        final ReportingTasksClient rtClient = client.getReportingTasksClient();

        final ReportingTaskEntity rtEntity = rtClient.getReportingTask(rtId);
        return new ReportingTaskResult(getResultType(properties), rtEntity);
    }
}
