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
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for creating a user.
 */
public class CreateUser extends AbstractNiFiRegistryCommand<StringResult> {

    public CreateUser() {
        super("create-user", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a new user.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.USER_NAME.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final TenantsClient tenantsClient = client.getTenantsClient();

        final String userName = getRequiredArg(properties, CommandOption.USER_NAME);
        final User user = new User(null, userName);
        final User createdUser = tenantsClient.createUser(user);

        return new StringResult(createdUser.getIdentifier(), getContext().isInteractive());
    }
}
