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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.TenantHelper;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Command for creating a new or updating an existing policy.
 */
public class CreateOrUpdateAccessPolicy extends AbstractNiFiRegistryCommand<VoidResult> {

    public CreateOrUpdateAccessPolicy() {
        super("update-policy", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates the access policy for the given resource and action, or creates the policy " +
            "if it doesn't exist. In stand-alone mode this command will not produce all of " +
            "the output seen in interactive mode unless the --verbose argument is specified.";
    }

    @Override
    protected void doInitialize(final Context context) {
        // Required
        addOption(CommandOption.POLICY_RESOURCE.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());

        // Optional
        addOption(CommandOption.USER_NAME_LIST.createOption());
        addOption(CommandOption.USER_ID_LIST.createOption());
        addOption(CommandOption.GROUP_NAME_LIST.createOption());
        addOption(CommandOption.GROUP_ID_LIST.createOption());
        addOption(CommandOption.OVERWRITE_POLICY.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiRegistryClient client, final Properties properties) throws IOException, NiFiRegistryException, ParseException {
        final PoliciesClient policiesClient = client.getPoliciesClient();
        final TenantsClient tenantsClient = client.getTenantsClient();

        final String action = getRequiredArg(properties, CommandOption.POLICY_ACTION);
        final String resource = getRequiredArg(properties, CommandOption.POLICY_RESOURCE);

        AccessPolicy currentPolicy;
        try {
            currentPolicy = policiesClient.getAccessPolicy(action, resource);
        } catch (NiFiRegistryException e) {
            currentPolicy = null;
        }

        if (currentPolicy == null) {
            currentPolicy = new AccessPolicy();

            currentPolicy.setAction(action);
            currentPolicy.setResource(resource);

            setUsers(currentPolicy, properties, tenantsClient);
            setGroups(currentPolicy, properties, tenantsClient);

            policiesClient.createAccessPolicy(currentPolicy);
        } else {
            setUsers(currentPolicy, properties, tenantsClient);
            setGroups(currentPolicy, properties, tenantsClient);

            policiesClient.updateAccessPolicy(currentPolicy);
        }

        return VoidResult.getInstance();
    }

    private void setUsers(AccessPolicy accessPolicy, Properties properties, TenantsClient tenantsClient) throws IOException, NiFiRegistryException {
        String userNames = getArg(properties, CommandOption.USER_NAME_LIST);
        String userIds = getArg(properties, CommandOption.USER_ID_LIST);

        if (StringUtils.isNotBlank(userNames) || StringUtils.isNotBlank(userIds)) {
            Set<Tenant> existingUsers = TenantHelper.selectExistingTenants(userNames, userIds, tenantsClient.getUsers());

            accessPolicy.setUsers(existingUsers);
        }
    }

    private void setGroups(AccessPolicy accessPolicy, Properties properties, TenantsClient tenantsClient) throws IOException, NiFiRegistryException {
        String groupNames = getArg(properties, CommandOption.GROUP_NAME_LIST);
        String groupIds = getArg(properties, CommandOption.GROUP_ID_LIST);

        if (StringUtils.isNotBlank(groupNames) || StringUtils.isNotBlank(groupIds)) {
            Set<Tenant> existingGroups = TenantHelper.selectExistingTenants(groupNames, groupIds, tenantsClient.getUserGroups());

            accessPolicy.setUserGroups(existingGroups);
        }
    }
}
