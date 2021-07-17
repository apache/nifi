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
package org.apache.nifi.toolkit.cli.impl.command.registry.policy;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.AccessPolicyResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the configuration of an access policy.
 */
public class GetAccessPolicy extends AbstractNiFiRegistryCommand<AccessPolicyResult> {
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
    public AccessPolicyResult doExecute(final NiFiRegistryClient client, final Properties properties) throws IOException, NiFiRegistryException, ParseException {
        final PoliciesClient policiesClient = client.getPoliciesClient();

        final String action = getRequiredArg(properties, CommandOption.POLICY_ACTION);
        final String resource = getRequiredArg(properties, CommandOption.POLICY_RESOURCE);

        final AccessPolicy accessPolicy = policiesClient.getAccessPolicy(action, resource);

        return new AccessPolicyResult(getResultType(properties), accessPolicy);
    }

}
