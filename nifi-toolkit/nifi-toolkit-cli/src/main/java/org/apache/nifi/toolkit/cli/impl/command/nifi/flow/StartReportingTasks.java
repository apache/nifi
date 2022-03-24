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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiActivateCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command to start the reporting tasks.
 */
public class StartReportingTasks extends AbstractNiFiActivateCommand<ReportingTaskEntity,
        ReportingTaskRunStatusEntity> {

    public StartReportingTasks() {
        super("start-reporting-tasks");
    }

    @Override
    public String getDescription() {
        return "Starts any enabled and valid reporting tasks.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.RT_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String rtId = getArg(properties, CommandOption.RT_ID);
        final Set<ReportingTaskEntity> reportingTaskEntities = new HashSet<>();

        if (StringUtils.isBlank(rtId)) {
            final ReportingTasksEntity reportingTasksEntity = client.getFlowClient().getReportingTasks();
            reportingTaskEntities.addAll(reportingTasksEntity.getReportingTasks());
        } else {
            reportingTaskEntities.add(client.getReportingTasksClient().getReportingTask(rtId));
        }

        activate(client, properties, reportingTaskEntities, "RUNNING");

        return VoidResult.getInstance();
    }

    @Override
    public ReportingTaskRunStatusEntity getRunStatusEntity() {
        return new ReportingTaskRunStatusEntity();
    }

    @Override
    public ReportingTaskEntity activateComponent(final NiFiClient client, final ReportingTaskEntity reportingTaskEntity,
            final ReportingTaskRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        return client.getReportingTasksClient().activateReportingTask(reportingTaskEntity.getId(), runStatusEntity);
    }

    @Override
    public String getDispName(final ReportingTaskEntity reportingTaskEntity) {
        return "Reporting task \"" + reportingTaskEntity.getComponent().getName() + "\" " +
                "(id: " + reportingTaskEntity.getId() + ")";
    }
}
