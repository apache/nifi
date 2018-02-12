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
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.VariableDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VariableRegistryUpdateRequestEntity;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Command to set the value of a variable in a process group.
 */
public class PGSetVar extends AbstractNiFiCommand<VoidResult> {

    public PGSetVar() {
        super("pg-set-var", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Sets the value of a variable in the variable registry for the specified process group.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.PG_VAR_NAME.createOption());
        addOption(CommandOption.PG_VAR_VALUE.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);
        final String varName = getRequiredArg(properties, CommandOption.PG_VAR_NAME);
        final String varVal = getRequiredArg(properties, CommandOption.PG_VAR_VALUE);

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final VariableRegistryEntity variableRegistry = pgClient.getVariables(pgId);
        final VariableRegistryDTO variableRegistryDTO = variableRegistry.getVariableRegistry();

        final VariableDTO variableDTO = new VariableDTO();
        variableDTO.setName(varName);
        variableDTO.setValue(varVal);

        final VariableEntity variableEntity = new VariableEntity();
        variableEntity.setVariable(variableDTO);

        // take the existing DTO and set only the requested variable for this command
        variableRegistryDTO.setVariables(Collections.singleton(variableEntity));

        // initiate the update request by posting the updated variable registry
        final VariableRegistryUpdateRequestEntity createdUpdateRequest = pgClient.updateVariableRegistry(pgId, variableRegistry);

        // poll the update request for up to 30 seconds to see if it has completed
        // if it doesn't complete then an exception will be thrown, but in either case the request will be deleted
        final String updateRequestId = createdUpdateRequest.getRequest().getRequestId();
        try {
            boolean completed = false;
            for (int i = 0; i < 30; i++) {
                final VariableRegistryUpdateRequestEntity updateRequest =
                        pgClient.getVariableRegistryUpdateRequest(pgId, updateRequestId);

                if (updateRequest != null && updateRequest.getRequest().isComplete()) {
                    completed = true;
                    break;
                } else {
                    try {
                        if (getContext().isInteractive()) {
                            println("Waiting for update request to complete...");
                        }
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (!completed) {
                throw new NiFiClientException("Unable to update variables of process group, cancelling request");
            }

        } finally {
            pgClient.deleteVariableRegistryUpdateRequest(pgId, updateRequestId);
        }

        return VoidResult.getInstance();
    }
}
