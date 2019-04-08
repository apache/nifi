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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.AccessPolicyAction;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command for updating an access policy.
 */
public class UpdateAccessPolicy extends AbstractNiFiCommand<VoidResult> {

    public UpdateAccessPolicy() {
        super("update-policy", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates the access policy for the given resource and action, or creates the policy " +
            "if it doesn't not exist. In stand-alone mode this command will not produce all of " +
            "the output seen in interactive mode unless the --verbose argument is specified.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.POLICY_RESOURCE.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());
        addOption(CommandOption.USER_NAME_LIST.createOption());
        addOption(CommandOption.USER_ID_LIST.createOption());
        addOption(CommandOption.GROUP_NAME_LIST.createOption());
        addOption(CommandOption.GROUP_ID_LIST.createOption());
        addOption(CommandOption.OVERWRITE_POLICY.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final PoliciesClient policiesClient = client.getPoliciesClient();
        final TenantsClient tenantsClient = client.getTenantsClient();

        final String resource = "/" + StringUtils.removeStart(getRequiredArg(properties, CommandOption.POLICY_RESOURCE), "/");
        final AccessPolicyAction actionType = AccessPolicyAction.valueOf(
                getRequiredArg(properties, CommandOption.POLICY_ACTION).toUpperCase().trim());

        final String users = getArg(properties, CommandOption.USER_NAME_LIST);
        final String userIds = getArg(properties, CommandOption.USER_ID_LIST);
        final String groups = getArg(properties, CommandOption.GROUP_NAME_LIST);
        final String groupIds = getArg(properties, CommandOption.GROUP_ID_LIST);

        final Set<TenantEntity> userEntities = new LinkedHashSet<>();

        if (StringUtils.isNotBlank(users)) {
            userEntities.addAll(generateTenantEntities(users, tenantsClient.getUsers()));
        }

        if (StringUtils.isNotBlank(userIds)) {
            userEntities.addAll(generateTenantEntities(userIds));
        }

        final Set<TenantEntity> groupEntites = new LinkedHashSet<>();

        if (StringUtils.isNotBlank(groups)) {
            groupEntites.addAll(generateTenantEntities(groups, tenantsClient.getUserGroups()));
        }

        if (StringUtils.isNotBlank(groupIds)) {
            groupEntites.addAll(generateTenantEntities(groupIds));
        }

        if (userEntities.isEmpty() && groupEntites.isEmpty()) {
            throw new CommandException("Users and groups were blank, nothing to update");
        }

        final boolean overwrite = properties.containsKey(CommandOption.OVERWRITE_POLICY.getLongName());

        AccessPolicyEntity policyEntity;
        try {
            policyEntity = policiesClient.getAccessPolicy(resource, actionType.toString().toLowerCase());
        } catch (NiFiClientException e) {
            policyEntity = null;
        }

        if (policyEntity == null) {
            if (shouldPrint(properties)) {
                println("Access policy not found" +
                    " for action " + actionType.toString().toLowerCase() +
                    " on resource /" + StringUtils.removeStart(resource, "/"));
            }

            final AccessPolicyDTO policyDTO = new AccessPolicyDTO();
            policyDTO.setResource(resource);
            policyDTO.setAction(actionType.toString().toLowerCase());
            policyDTO.setUsers(new LinkedHashSet<>());
            policyDTO.setUserGroups(new LinkedHashSet<>());

            policyEntity = new AccessPolicyEntity();
            policyEntity.setComponent(policyDTO);
            policyEntity.setRevision(getInitialRevisionDTO());
            setTenant(policyEntity, userEntities, groupEntites, overwrite, properties);

            final AccessPolicyEntity createdEntity = policiesClient.createAccessPolicy(policyEntity);

            if (shouldPrint(properties)) {
                println("New access policy was created");
                println("id: " + createdEntity.getId());
            }
        } else if (!resource.equals(policyEntity.getComponent().getResource())) {
            if (shouldPrint(properties)) {
                println("Override the policy inherited from "
                    + policyEntity.getComponent().getResource());
            }

            final AccessPolicyDTO policyDTO = new AccessPolicyDTO();
            policyDTO.setResource(resource);
            policyDTO.setAction(actionType.toString().toLowerCase());
            policyDTO.setUsers(policyEntity.getComponent().getUsers());
            policyDTO.setUserGroups(policyEntity.getComponent().getUserGroups());

            policyEntity = new AccessPolicyEntity();
            policyEntity.setComponent(policyDTO);
            policyEntity.setRevision(getInitialRevisionDTO());
            setTenant(policyEntity, userEntities, groupEntites, overwrite, properties);

            final AccessPolicyEntity createdEntity = policiesClient.createAccessPolicy(policyEntity);

            if (shouldPrint(properties)) {
                println("Override access policy was created");
                println("id: " + createdEntity.getId());
            }
        } else {
            final String clientId = getContext().getSession().getNiFiClientID();
            policyEntity.getRevision().setClientId(clientId);
            setTenant(policyEntity, userEntities, groupEntites, overwrite, properties);

            policiesClient.updateAccessPolicy(policyEntity);

            if (shouldPrint(properties)) {
                println("Access policy was updated");
                println("id: " + policyEntity.getId());
            }
        }

        return VoidResult.getInstance();
    }

    private void setTenant(final AccessPolicyEntity policyEntity, final Set<TenantEntity> userEntities,
            final Set<TenantEntity> groupEntities, final boolean overwrite, final Properties properties) {
        if (overwrite) {
            policyEntity.getComponent().setUsers(new LinkedHashSet<>());
            policyEntity.getComponent().setUserGroups(new LinkedHashSet<>());
        }

        final Set<TenantEntity> userSet = policyEntity.getComponent().getUsers();
        userEntities.forEach(entity -> addTenant(userSet, entity, "User", properties));

        final Set<TenantEntity> groupSet = policyEntity.getComponent().getUserGroups();
        groupEntities.forEach(entity -> addTenant(groupSet, entity, "User group", properties));
    }

    private void addTenant(final Set<TenantEntity> tenantSet, final TenantEntity additionalTenant,
            final String tenantType, final Properties properties) {
        final String dispTenantName = additionalTenant.getComponent() != null && StringUtils.isNotBlank(additionalTenant.getComponent().getIdentity())
            ? tenantType + " \"" + additionalTenant.getComponent().getIdentity() + "\""
            : tenantType + " (id: " + additionalTenant.getId() + ")";

        if (tenantSet.contains(additionalTenant)) {
            if (shouldPrint(properties)) {
                println(dispTenantName + " already included");
            }
        } else {
            if (shouldPrint(properties)) {
                println(dispTenantName + " added");
            }
            tenantSet.add(additionalTenant);
        }
    }

    private boolean shouldPrint(final Properties properties) {
        return isInteractive() || isVerbose(properties);
    }
}
