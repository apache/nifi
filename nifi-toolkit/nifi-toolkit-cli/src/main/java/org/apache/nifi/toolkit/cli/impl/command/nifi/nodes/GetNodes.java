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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.NodesResult;
import org.apache.nifi.web.api.entity.ClusterEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for retrieving the status of the nodes from the NiFi cluster.
 */
public class GetNodes extends AbstractNiFiCommand<NodesResult> {

    public GetNodes() {
        super("get-nodes", NodesResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves statuses for the nodes of the NiFi cluster.";
    }

    @Override
    public NodesResult doExecute(NiFiClient client, Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final ControllerClient controllerClient = client.getControllerClient();

        ClusterEntity clusterEntityResult = controllerClient.getNodes();
        return new NodesResult(getResultType(properties), clusterEntityResult);
    }
}
