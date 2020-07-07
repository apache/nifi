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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to replace the content of an existing process group with the content from a versioned flow snapshot.
 */
public class PGReplace extends AbstractNiFiCommand<VoidResult> {

    public PGReplace() {
        super("pg-replace", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Replaces the content of a process group with the content from the specified versioned flow snapshot.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_SOURCE);
        final String contents = getInputSourceContent(inputFile);

        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final VersionedFlowSnapshot deserializedSnapshot = objectMapper.readValue(contents, VersionedFlowSnapshot.class);
        if (deserializedSnapshot == null) {
            throw new IOException("Unable to deserialize flow version from " + inputFile);
        }

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final ProcessGroupEntity existingProcessGroup = pgClient.getProcessGroup(pgId);

        final ProcessGroupImportEntity importEntity = new ProcessGroupImportEntity();
        importEntity.setVersionedFlowSnapshot(deserializedSnapshot);
        importEntity.setProcessGroupRevision(existingProcessGroup.getRevision());

        final ProcessGroupReplaceRequestEntity createdReplaceRequestEntity = pgClient.replaceProcessGroup(pgId, importEntity);
        final String requestId = createdReplaceRequestEntity.getRequest().getRequestId();
        try {
            boolean completed = false;
            for (int i = 0; i < 30; i++) {
                final ProcessGroupReplaceRequestEntity replaceRequest =
                        pgClient.getProcessGroupReplaceRequest(pgId, requestId);

                if (replaceRequest != null && replaceRequest.getRequest().isComplete()) {
                    completed = true;
                    break;
                } else {
                    try {
                        if (getContext().isInteractive()) {
                            println("Waiting for replacement request to complete...");
                        }
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (!completed) {
                throw new NiFiClientException("Unable to replace process group, cancelling request");
            }

        } finally {
            pgClient.deleteProcessGroupReplaceRequest(pgId, requestId);
        }

        return VoidResult.getInstance();
    }
}
