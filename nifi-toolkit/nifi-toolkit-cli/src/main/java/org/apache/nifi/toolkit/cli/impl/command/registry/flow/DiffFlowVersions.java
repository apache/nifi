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
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.VersionedFlowDifferenceResult;

import java.io.IOException;
import java.util.Properties;

public class DiffFlowVersions extends AbstractNiFiRegistryCommand<VersionedFlowDifferenceResult> {

    public DiffFlowVersions() {
        super("diff-flow-versions", VersionedFlowDifferenceResult.class);
    }

    @Override
    public String getDescription() {
        return "Shows the differences between two versions of a flow.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION_1.createOption());
        addOption(CommandOption.FLOW_VERSION_2.createOption());
    }

    @Override
    public VersionedFlowDifferenceResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version1 = getRequiredIntArg(properties, CommandOption.FLOW_VERSION_1);
        final Integer version2 = getRequiredIntArg(properties, CommandOption.FLOW_VERSION_2);

        final FlowClient flowClient = client.getFlowClient();
        final VersionedFlow flow = flowClient.get(flowId);

        final VersionedFlowDifference flowDifference = flowClient.diff(flow.getBucketIdentifier(), flowId, version1, version2);
        return new VersionedFlowDifferenceResult(getResultType(properties), flowDifference);
    }
}
