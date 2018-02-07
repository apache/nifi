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

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the id of a registry client by name.
 */
public class GetRegistryClientId extends AbstractNiFiCommand {

    public GetRegistryClientId() {
        super("get-reg-client-id");
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.REGISTRY_CLIENT_NAME.createOption());
    }

    @Override
    protected void doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {
        final String regClientName = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_NAME);

        final RegistryClientsEntity registries = client.getControllerClient().getRegistryClients();

        final RegistryDTO registry = registries.getRegistries().stream()
                .map(r -> r.getComponent())
                .filter(r -> r.getName().equalsIgnoreCase(regClientName))
                .findFirst()
                .orElse(null);

        if (registry == null) {
            throw new NiFiClientException("No registry client exists with the name '" + regClientName + "'");
        } else {
            println(registry.getId());
        }
    }
}
