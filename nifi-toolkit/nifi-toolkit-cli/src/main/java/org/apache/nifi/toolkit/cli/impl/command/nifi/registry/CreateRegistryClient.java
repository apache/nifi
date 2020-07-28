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
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for creating a registry client in NiFi.
 */
public class CreateRegistryClient extends AbstractNiFiCommand<StringResult> {

    public CreateRegistryClient() {
        super("create-reg-client", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a registry client using the provided information.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.REGISTRY_CLIENT_NAME.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_URL.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_DESC.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String name = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_NAME);
        final String url = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_URL);
        final String desc = getArg(properties, CommandOption.REGISTRY_CLIENT_DESC);

        final RegistryDTO registryDTO = new RegistryDTO();
        registryDTO.setName(name);
        registryDTO.setUri(url);
        registryDTO.setDescription(desc);

        final RegistryClientEntity clientEntity = new RegistryClientEntity();
        clientEntity.setComponent(registryDTO);
        clientEntity.setRevision(getInitialRevisionDTO());

        final RegistryClientEntity createdEntity = client.getControllerClient().createRegistryClient(clientEntity);
        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }
}
