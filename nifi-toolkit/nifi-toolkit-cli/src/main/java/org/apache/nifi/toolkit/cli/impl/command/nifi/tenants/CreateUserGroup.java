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
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.UserGroupEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for creating a user group.
 */
public class CreateUserGroup extends AbstractNiFiCommand<StringResult> {

    public CreateUserGroup() {
        super("create-user-group", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates new user group.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.UG_NAME.createOption());
        addOption(CommandOption.USER_LIST.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String userGroupId = getRequiredArg(properties, CommandOption.UG_NAME);
        final String users = getArg(properties, CommandOption.USER_LIST);

        final UserGroupDTO userGroupDTO = new UserGroupDTO();
        userGroupDTO.setIdentity(userGroupId);
        if (StringUtils.isNotBlank(users)) {
            userGroupDTO.setUsers(generateTenantEntities(users));
        }

        final UserGroupEntity userGroupEntity = new UserGroupEntity();
        userGroupEntity.setComponent(userGroupDTO);
        userGroupEntity.setRevision(getInitialRevisionDTO());

        final UserGroupEntity createdEntity = client.getTenantsClient().createUserGroup(userGroupEntity);
        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }
}
