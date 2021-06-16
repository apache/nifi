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
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Properties;

public class GetAccessToken extends AbstractNiFiRegistryCommand<StringResult> {

    public GetAccessToken() {
        super("get-access-token", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Authenticates to NiFi Registry with the given username and password and returns an access token for use " +
                "on future requests as the value of the " + CommandOption.BEARER_TOKEN.getLongName() + " argument";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.USERNAME.createOption());
        addOption(CommandOption.PASSWORD.createOption());
    }

    @Override
    public StringResult doExecute(NiFiRegistryClient client, Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String username = getRequiredArg(properties, CommandOption.USERNAME);
        final String password = getRequiredArg(properties, CommandOption.PASSWORD);

        final String token = client.getAccessClient().getToken(username, password);
        return new StringResult(token, getContext().isInteractive());
    }
}
