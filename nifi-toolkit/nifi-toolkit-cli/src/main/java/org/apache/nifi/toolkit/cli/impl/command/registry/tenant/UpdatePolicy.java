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
package org.apache.nifi.toolkit.cli.impl.command.registry.tenant;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.ExtendedNiFiRegistryClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Command for update an existing policy.
 */
public class UpdatePolicy extends AbstractNiFiRegistryCommand<StringResult> {

    public UpdatePolicy() {
        super("update-policy", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates an existing access policy.";
    }

    @Override
    protected void doInitialize(final Context context) {
        // Required
        addOption(CommandOption.POLICY_ID.createOption());

        // Optional
        addOption(CommandOption.POLICY_RESOURCE.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());
        addOption(CommandOption.USER_NAME_LIST.createOption());
        addOption(CommandOption.USER_ID_LIST.createOption());
        addOption(CommandOption.GROUP_NAME_LIST.createOption());
        addOption(CommandOption.GROUP_ID_LIST.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        if (!(client instanceof ExtendedNiFiRegistryClient)) {
            throw new IllegalArgumentException("This command needs extended registry client!");
        }

        final ExtendedNiFiRegistryClient extendedClient = (ExtendedNiFiRegistryClient) client;
        final PoliciesClient policiesClient = extendedClient.getPoliciesClient();
        final TenantsClient tenantsClient = extendedClient.getTenantsClient();

        final String policyId = getRequiredArg(properties, CommandOption.POLICY_ID);
        final String resource = getArg(properties, CommandOption.POLICY_RESOURCE);
        final String action = getArg(properties, CommandOption.POLICY_ACTION);

        final AccessPolicy existingPolicy = policiesClient.getPolicy(policyId);

        if (StringUtils.isNotBlank(resource)) {
            existingPolicy.setResource(resource);
        }

        if (StringUtils.isNotBlank(action)) {
            existingPolicy.setAction(action);
        }

        final Set<Tenant> users = TenantHelper.getExistingUsers(
                tenantsClient,
                getArg(properties, CommandOption.USER_NAME_LIST),
                getArg(properties, CommandOption.USER_ID_LIST));

        if (StringUtils.isNotBlank(getArg(properties, CommandOption.USER_NAME_LIST)) || StringUtils.isNotBlank(getArg(properties, CommandOption.USER_ID_LIST))) {
            existingPolicy.setUsers(users);
        }

        final Set<Tenant> userGroups = TenantHelper.getExistingGroups(
                tenantsClient,
                getArg(properties, CommandOption.GROUP_NAME_LIST),
                getArg(properties, CommandOption.GROUP_ID_LIST));

        if (StringUtils.isNotBlank(getArg(properties, CommandOption.GROUP_NAME_LIST)) || StringUtils.isNotBlank(getArg(properties, CommandOption.GROUP_ID_LIST))) {
            existingPolicy.setUserGroups(userGroups);
        }

        final AccessPolicy updatedPolicy = policiesClient.updatePolicy(existingPolicy);
        return new StringResult(updatedPolicy.getIdentifier(), getContext().isInteractive());
    }
}
