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
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Command for updating an access policy.
 */
public class UpdateAccessPolicy extends AbstractNiFiCommand<VoidResult> {

    public UpdateAccessPolicy() {
        super("update-policy", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates an access policy.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.POLICY_RESOURCE.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());
        addOption(CommandOption.USER_LIST.createOption());
        addOption(CommandOption.GROUP_LIST.createOption());
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

        final String users = getArg(properties, CommandOption.USER_LIST);
        final String groups = getArg(properties, CommandOption.GROUP_LIST);

        if (StringUtils.isBlank(users) && StringUtils.isBlank(groups)) {
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
            println("Access policy not found" +
                    " for action " + actionType.toString().toLowerCase() +
                    " on resource /" + StringUtils.removeStart(resource, "/"));

            final AccessPolicyDTO policyDTO = new AccessPolicyDTO();
            policyDTO.setResource(resource);
            policyDTO.setAction(actionType.toString().toLowerCase());
            policyDTO.setUsers(new HashSet<>());
            policyDTO.setUserGroups(new HashSet<>());

            policyEntity = new AccessPolicyEntity();
            policyEntity.setComponent(policyDTO);
            policyEntity.setRevision(getInitialRevisionDTO());
            setTenant(policyEntity, users, groups, overwrite, tenantsClient);

            final AccessPolicyEntity createdEntity = policiesClient.createAccessPolicy(policyEntity);
            println("New access policy was created");
            println("id: " + createdEntity.getId());
        } else if (!resource.equals(policyEntity.getComponent().getResource())) {
            println("Override the policy inherited from "
                    + policyEntity.getComponent().getResource());

            final AccessPolicyDTO policyDTO = new AccessPolicyDTO();
            policyDTO.setResource(resource);
            policyDTO.setAction(actionType.toString().toLowerCase());
            policyDTO.setUsers(policyEntity.getComponent().getUsers());
            policyDTO.setUserGroups(policyEntity.getComponent().getUserGroups());

            policyEntity = new AccessPolicyEntity();
            policyEntity.setComponent(policyDTO);
            policyEntity.setRevision(getInitialRevisionDTO());
            setTenant(policyEntity, users, groups, overwrite, tenantsClient);

            final AccessPolicyEntity createdEntity = policiesClient.createAccessPolicy(policyEntity);
            println("Override access policy was created");
            println("id: " + createdEntity.getId());
        } else {
            final String clientId = getContext().getSession().getNiFiClientID();
            policyEntity.getRevision().setClientId(clientId);
            setTenant(policyEntity, users, groups, overwrite, tenantsClient);

            policiesClient.updateAccessPolicy(policyEntity);
            println("Access policy was updated");
            println("id: " + policyEntity.getId());
        }

        return VoidResult.getInstance();
    }

    private void setTenant(final AccessPolicyEntity policyEntity, final String users, final String groups,
                           final boolean overwrite, final TenantsClient tenantsClient)
            throws NiFiClientException, IOException {
        if (overwrite) {
            policyEntity.getComponent().setUsers(new HashSet<>());
            policyEntity.getComponent().setUserGroups(new HashSet<>());
        }

        if (StringUtils.isNotBlank(users)) {
            final Set<TenantEntity> userSet = policyEntity.getComponent().getUsers();
            final UsersEntity existingUsers = tenantsClient.getUsers();

            generateTenantEntities(users).forEach(entity -> {
                final Optional<UserEntity> existingUser = existingUsers.getUsers().stream()
                        .filter(userEntity -> entity.getId().equals(userEntity.getId())).findAny();

                if (userSet.contains(entity)) {
                    println("User id " + entity.getId() + " already included");
                } else if (!existingUser.isPresent()) {
                    println("User with id " + entity.getId() + " not found. Skipped.");
                } else {
                    println("User \"" + existingUser.get().getComponent().getIdentity() + "\""
                            + " (id " + existingUser.get().getId() + ") added");
                    userSet.add(entity);
                }
            });
        }

        if (StringUtils.isNotBlank(groups)) {
            final Set<TenantEntity> groupSet = policyEntity.getComponent().getUserGroups();
            final UserGroupsEntity existingGroups = tenantsClient.getUserGroups();

            generateTenantEntities(groups).forEach(entity -> {
                final Optional<UserGroupEntity> existingGroup = existingGroups.getUserGroups().stream()
                        .filter(groupEntity -> entity.getId().equals(groupEntity.getId())).findAny();
                if (groupSet.contains(entity)) {
                    println("User group id " + entity.getId() + " already included");
                } else if (!existingGroup.isPresent()) {
                    println("User group with id " + entity.getId() + " not found. Skipped.");
                } else {
                    println("User group \"" + existingGroup.get().getComponent().getIdentity() + "\""
                            + " (id " + existingGroup.get().getId() + ") added");
                    groupSet.add(entity);
                }
            });
        }
    }
}
