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
package org.apache.nifi.toolkit.cli.impl.command.nifi.nodes;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.OkResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for deleting a node from the NiFi cluster.
 */
public class DeleteNode extends AbstractNiFiCommand<OkResult> {

    public DeleteNode() {
        super("delete-node", OkResult.class);
    }

    @Override
    public String getDescription() {
        return "Deletes a node from the NiFi cluster.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.NIFI_NODE_ID.createOption());
    }

    @Override
    public OkResult doExecute(NiFiClient client, Properties properties) throws NiFiClientException, IOException, MissingOptionException {
        final String nodeId = getRequiredArg(properties, CommandOption.NIFI_NODE_ID);
        final ControllerClient controllerClient = client.getControllerClient();

        controllerClient.deleteNode(nodeId);
        return new OkResult(getContext().isInteractive());
    }
}
