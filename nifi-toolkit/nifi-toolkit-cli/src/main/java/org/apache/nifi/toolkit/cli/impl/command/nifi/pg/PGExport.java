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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessGroupClient;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Command to export a given process-group to a json representation.
 */
public class PGExport extends AbstractNiFiCommand<VoidResult> {
    public PGExport() {
        super("pg-export", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Exports a given process group to a json representation, with or without the referenced services from outside the target group.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
        addOption(CommandOption.INCLUDE_REFERENCED_SERVICES.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String id = getRequiredArg(properties, CommandOption.PG_ID);
        final boolean includeReferencedServices = getArg(properties, CommandOption.INCLUDE_REFERENCED_SERVICES) == null ? Boolean.FALSE : Boolean.TRUE;
        final File outputFile = new File(getRequiredArg(properties, CommandOption.OUTPUT_FILE));

        final ProcessGroupClient processGroupClient = client.getProcessGroupClient();
        processGroupClient.exportProcessGroup(id, includeReferencedServices, outputFile);
        return VoidResult.getInstance();
    }
}
