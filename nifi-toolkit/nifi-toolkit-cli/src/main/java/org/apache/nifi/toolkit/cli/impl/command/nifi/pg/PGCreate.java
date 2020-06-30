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

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to create process-groups childs of the root process group.
 */
public class PGCreate extends AbstractNiFiCommand<StringResult> {

    public PGCreate() {
        super("pg-create", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a process group child of the root group.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_NAME.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String processGroupName = getRequiredArg(properties, CommandOption.PG_NAME);

        final FlowClient flowClient = client.getFlowClient();
        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final String rootPgId = flowClient.getRootGroupId();

        final ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setParentGroupId(rootPgId);
        processGroupDTO.setName(processGroupName);

        final ProcessGroupEntity pgEntity = new ProcessGroupEntity();
        pgEntity.setComponent(processGroupDTO);
        pgEntity.setRevision(getInitialRevisionDTO());

        final ProcessGroupEntity createdEntity = pgClient.createProcessGroup(
                rootPgId, pgEntity);

        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }

}
