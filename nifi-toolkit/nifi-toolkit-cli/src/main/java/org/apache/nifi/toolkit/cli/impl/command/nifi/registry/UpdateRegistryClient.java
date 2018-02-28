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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.RegistryClientEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to update a registry client in NiFi.
 */
public class UpdateRegistryClient extends AbstractNiFiCommand<VoidResult> {

    public UpdateRegistryClient() {
        super("update-reg-client", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates the given registry client with a new name, url, or description.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_NAME.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_URL.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_DESC.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final ControllerClient controllerClient = client.getControllerClient();

        final String id = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);

        final RegistryClientEntity existingRegClient = controllerClient.getRegistryClient(id);
        if (existingRegClient == null) {
            throw new CommandException("Registry client does not exist for id " + id);
        }

        final String name = getArg(properties, CommandOption.REGISTRY_CLIENT_NAME);
        final String url = getArg(properties, CommandOption.REGISTRY_CLIENT_URL);
        final String desc = getArg(properties, CommandOption.REGISTRY_CLIENT_DESC);

        if (StringUtils.isBlank(name) && StringUtils.isBlank(url) && StringUtils.isBlank(desc)) {
            throw new CommandException("Name, url, and desc were all blank, nothing to update");
        }

        if (StringUtils.isNotBlank(name)) {
            existingRegClient.getComponent().setName(name);
        }

        if (StringUtils.isNotBlank(url)) {
            existingRegClient.getComponent().setUri(url);
        }

        if (StringUtils.isNotBlank(desc)) {
            existingRegClient.getComponent().setDescription(desc);
        }

        final String clientId = getContext().getSession().getNiFiClientID();
        existingRegClient.getRevision().setClientId(clientId);

        controllerClient.updateRegistryClient(existingRegClient);
        return VoidResult.getInstance();
    }
}
