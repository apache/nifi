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
package org.apache.nifi.toolkit.cli.impl.command.nifi.tenants;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Command for updating a user group.
 */
public class UpdateUserGroup extends AbstractNiFiCommand<VoidResult> {

    public UpdateUserGroup() {
        super("update-user-group", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates a user group.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.UG_NAME.createOption());
        addOption(CommandOption.UG_ID.createOption());
        addOption(CommandOption.USER_NAME_LIST.createOption());
        addOption(CommandOption.USER_ID_LIST.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final TenantsClient tenantsClient = client.getTenantsClient();

        final String group = getArg(properties, CommandOption.UG_NAME);
        final String groupId = getArg(properties, CommandOption.UG_ID);

        if ((StringUtils.isBlank(group) && StringUtils.isBlank(groupId))
            || (StringUtils.isNotBlank(group) && StringUtils.isNotBlank(groupId))) {
            throw new CommandException("Specify either \"" + CommandOption.UG_NAME.getLongName()
                + "\" or \"" + CommandOption.UG_ID.getLongName() + "\" (not both)");
        }

        UserGroupEntity existingGroup;

        if (StringUtils.isNotBlank(group)) {
            final Optional<UserGroupEntity> existingGroupEntity = tenantsClient.getUserGroups().getUserGroups().stream()
                .filter(userGroupEntity -> group.equals(userGroupEntity.getComponent().getIdentity()))
                .findAny();

            if (!existingGroupEntity.isPresent()) {
                throw new CommandException("User group does not exist for identity \"" + group + "\"");
            }

            existingGroup = existingGroupEntity.get();
        } else {
            existingGroup = tenantsClient.getUserGroup(groupId);
        }

        final String users = getArg(properties, CommandOption.USER_NAME_LIST);
        final String userIds = getArg(properties, CommandOption.USER_ID_LIST);

        final Set<TenantEntity> tenantEntities = new HashSet<>();

        if (StringUtils.isNotBlank(users)) {
            tenantEntities.addAll(
                generateTenantEntities(users, client.getTenantsClient().getUsers()));
        }

        if (StringUtils.isNotBlank(userIds)) {
            tenantEntities.addAll(generateTenantEntities(userIds));
        }

        existingGroup.getComponent().setUsers(tenantEntities);

        final String clientId = getContext().getSession().getNiFiClientID();
        existingGroup.getRevision().setClientId(clientId);

        tenantsClient.updateUserGroup(existingGroup);
        return VoidResult.getInstance();
    }
}
