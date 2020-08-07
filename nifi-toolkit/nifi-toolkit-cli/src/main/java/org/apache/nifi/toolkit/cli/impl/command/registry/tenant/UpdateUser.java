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
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for updating an existing user.
 */
public class UpdateUser extends AbstractNiFiRegistryCommand<VoidResult> {
    public UpdateUser() {
        super("update-user", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates an existing user.";
    }

    @Override
    protected void doInitialize(final Context context) {
        // Required
        addOption(CommandOption.USER_ID.createOption());

        // Optional
        addOption(CommandOption.USER_NAME.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiRegistryClient client, final Properties properties)
        throws IOException, NiFiRegistryException, ParseException {

        final TenantsClient tenantsClient = client.getTenantsClient();
        final String userId = getRequiredArg(properties, CommandOption.USER_ID);
        final User existingUser = tenantsClient.getUser(userId);

        final String userName = getArg(properties, CommandOption.USER_NAME);

        if (StringUtils.isNotBlank(userName)) {
            existingUser.setIdentity(userName);
        }

        tenantsClient.updateUser(existingUser);

        return VoidResult.getInstance();
    }
}
