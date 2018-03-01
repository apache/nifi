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
package org.apache.nifi.toolkit.cli.impl.command.registry.user;

import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.CurrentUserResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get info about the current user access NiFi Registry.
 */
public class CurrentUser extends AbstractNiFiRegistryCommand<CurrentUserResult> {

    public CurrentUser() {
        super("current-user", CurrentUserResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns information about the user accessing NiFi Registry. " +
                "This provides a way to test if the CLI is accessing NiFi Registry as the expected user.";
    }

    @Override
    public CurrentUserResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException {
        final UserClient userClient = client.getUserClient();
        final org.apache.nifi.registry.authorization.CurrentUser currentUser = userClient.getAccessStatus();
        return new CurrentUserResult(getResultType(properties), currentUser);
    }
}
