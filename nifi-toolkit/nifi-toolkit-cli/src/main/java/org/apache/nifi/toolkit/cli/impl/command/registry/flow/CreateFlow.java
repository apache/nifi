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
package org.apache.nifi.toolkit.cli.impl.command.registry.flow;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a flow in the registry
 */
public class CreateFlow extends AbstractNiFiRegistryCommand<StringResult> {

    public CreateFlow() {
        super("create-flow", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a flow in the given bucket with the given name and description.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_NAME.createOption());
        addOption(CommandOption.FLOW_DESC.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flowName = getRequiredArg(properties, CommandOption.FLOW_NAME);
        final String flowDesc = getArg(properties, CommandOption.FLOW_DESC);

        final VersionedFlow flow = new VersionedFlow();
        flow.setName(flowName);
        flow.setDescription(flowDesc);
        flow.setBucketIdentifier(bucketId);

        final FlowClient flowClient = client.getFlowClient();
        final VersionedFlow createdFlow = flowClient.create(flow);
        return new StringResult(createdFlow.getIdentifier(), getContext().isInteractive());
    }
}
