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
package org.apache.nifi.toolkit.cli.impl.command.nifi.registry;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.RegistryBranchesResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.FlowRegistryBranchesEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Lists the branches seen by the specified registry client.
 */
public class ListBranches extends AbstractNiFiCommand<RegistryBranchesResult> {

    public ListBranches() {
        super("list-branches", RegistryBranchesResult.class);
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
    }

    @Override
    public String getDescription() {
        return "Returns the list of branches seen by the specified registry client.";
    }

    @Override
    public RegistryBranchesResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {
        final String regClientId = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);
        final FlowRegistryBranchesEntity branches = client.getFlowClient().getFlowRegistryBranches(regClientId);
        return new RegistryBranchesResult(getResultType(properties), branches);
    }

}
