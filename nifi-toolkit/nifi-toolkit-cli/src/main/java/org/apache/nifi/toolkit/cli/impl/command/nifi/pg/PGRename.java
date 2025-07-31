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
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessGroupClient;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to rename a process group.
 */
public class PGRename extends AbstractNiFiCommand<StringResult> {

    public PGRename() {
        super("pg-rename", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Renames the given process group.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.PG_NAME.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);
        final String pgName = getRequiredArg(properties, CommandOption.PG_NAME);

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final ProcessGroupEntity pgEntity = pgClient.getProcessGroup(pgId);
        if (pgEntity == null) {
            throw new NiFiClientException("Process group with id " + pgId + " not found.");
        }

        rename(pgName, pgClient, pgEntity);

        return new StringResult(pgId, isInteractive());
    }

    protected void rename(final String pgName, final ProcessGroupClient pgClient, final ProcessGroupEntity pgEntity) throws NiFiClientException, IOException {
        pgEntity.setProcessGroupUpdateStrategy(ProcessGroupRecursivity.DIRECT_CHILDREN.name());
        pgEntity.getComponent().setContents(null);
        pgEntity.getComponent().setVersionControlInformation(null);
        pgEntity.getComponent().setName(pgName);

        pgClient.updateProcessGroup(pgEntity);
    }
}