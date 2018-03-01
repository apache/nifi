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
package org.apache.nifi.toolkit.cli.impl.command.nifi.registry;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.RegistryClientIDResult;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the id of a registry client by name or url.
 */
public class GetRegistryClientId extends AbstractNiFiCommand<RegistryClientIDResult> {

    public GetRegistryClientId() {
        super("get-reg-client-id", RegistryClientIDResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns the id of the first registry client found with the given name or url. " +
                "Only one of name or url can be specified.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.REGISTRY_CLIENT_NAME.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_URL.createOption());
    }

    @Override
    public RegistryClientIDResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, CommandException {
        final String regClientName = getArg(properties, CommandOption.REGISTRY_CLIENT_NAME);
        final String regClientUrl = getArg(properties, CommandOption.REGISTRY_CLIENT_URL);

        if (!StringUtils.isBlank(regClientName) && !StringUtils.isBlank(regClientUrl)) {
            throw new CommandException("Name and URL cannot be specified at the same time");
        }

        if (StringUtils.isBlank(regClientName) && StringUtils.isBlank(regClientUrl)) {
            throw new CommandException("Name or URL must be specified");
        }

        final RegistryClientsEntity registries = client.getControllerClient().getRegistryClients();

        RegistryDTO registry;

        if (!StringUtils.isBlank(regClientName)) {
            registry = registries.getRegistries().stream()
                    .map(r -> r.getComponent())
                    .filter(r -> r.getName().equalsIgnoreCase(regClientName.trim()))
                    .findFirst()
                    .orElse(null);
        } else {
            registry = registries.getRegistries().stream()
                    .map(r -> r.getComponent())
                    .filter(r -> r.getUri().equalsIgnoreCase(regClientUrl.trim()))
                    .findFirst()
                    .orElse(null);
        }

        if (registry == null) {
            throw new NiFiClientException("No registry client exists with the name '" + regClientName + "'");
        } else {
            return new RegistryClientIDResult(getResultType(properties), registry);
        }
    }

}
