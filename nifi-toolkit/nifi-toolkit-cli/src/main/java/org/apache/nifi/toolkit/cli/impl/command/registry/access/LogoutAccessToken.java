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
package org.apache.nifi.toolkit.cli.impl.command.registry.access;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;

import java.io.IOException;
import java.util.Properties;

public class LogoutAccessToken extends AbstractNiFiRegistryCommand<VoidResult> {

    public LogoutAccessToken() {
        super("logout-access-token", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Performs a logout for the given access token";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BEARER_TOKEN.createOption());
    }

    @Override
    public VoidResult doExecute(NiFiRegistryClient client, Properties properties)
            throws IOException, NiFiRegistryException, ParseException, CommandException {
        final String bearerToken = getRequiredArg(properties, CommandOption.BEARER_TOKEN);
        client.getAccessClient().logout(bearerToken);
        return VoidResult.getInstance();
    }

}
