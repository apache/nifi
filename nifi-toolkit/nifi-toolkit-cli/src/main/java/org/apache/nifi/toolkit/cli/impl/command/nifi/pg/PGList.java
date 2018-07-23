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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.ProcessGroupsResult;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Command to list process-groups for a given parent process group.
 */
public class PGList extends AbstractNiFiCommand<ProcessGroupsResult> {

    public PGList() {
        super("pg-list", ProcessGroupsResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns the process groups contained in the specified process group. If no process group is specified, " +
                "then the root group will be used.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public ProcessGroupsResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException {

        final FlowClient flowClient = client.getFlowClient();

        // get the optional id of the parent PG, otherwise fallback to the root group
        String parentPgId = getArg(properties, CommandOption.PG_ID);
        if (StringUtils.isBlank(parentPgId)) {
            parentPgId = flowClient.getRootGroupId();
        }

        final ProcessGroupFlowEntity processGroupFlowEntity = flowClient.getProcessGroup(parentPgId);
        final ProcessGroupFlowDTO processGroupFlowDTO = processGroupFlowEntity.getProcessGroupFlow();
        final FlowDTO flowDTO = processGroupFlowDTO.getFlow();

        final List<ProcessGroupDTO> processGroups = new ArrayList<>();
        if (flowDTO.getProcessGroups() != null) {
            flowDTO.getProcessGroups().stream().map(pge -> pge.getComponent()).forEach(dto -> processGroups.add(dto));
        }

        return new ProcessGroupsResult(getResultType(properties), processGroups);
    }

}
