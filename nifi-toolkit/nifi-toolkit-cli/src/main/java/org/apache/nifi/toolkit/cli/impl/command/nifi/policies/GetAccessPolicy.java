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
package org.apache.nifi.toolkit.cli.impl.command.nifi.policies;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.AccessPolicyAction;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.AccessPolicyResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the configuration of an access policy.
 */
public class GetAccessPolicy extends AbstractNiFiCommand<AccessPolicyResult> {

    public GetAccessPolicy() {
        super("get-policy", AccessPolicyResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the configuration for an access policy.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.POLICY_RESOURCE.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());
    }

    @Override
    public AccessPolicyResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, CommandException, MissingOptionException {
        final PoliciesClient policyClient = client.getPoliciesClient();

        final String resource = getRequiredArg(properties, CommandOption.POLICY_RESOURCE);
        final AccessPolicyAction actionType = AccessPolicyAction.valueOf(
                getRequiredArg(properties, CommandOption.POLICY_ACTION).toUpperCase().trim());

        return new AccessPolicyResult(getResultType(properties), policyClient.getAccessPolicy(
                resource, actionType.toString().toLowerCase()));
    }

}
