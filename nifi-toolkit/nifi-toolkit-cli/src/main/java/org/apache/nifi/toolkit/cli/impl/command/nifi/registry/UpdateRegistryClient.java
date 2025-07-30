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
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
        return "Updates the given registry client with a new name, description, or URL.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_NAME.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_DESC.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_URL.createOption());
        addOption(CommandOption.SSL_CONTEXT_SERVICE_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final ControllerClient controllerClient = client.getControllerClient();

        final String id = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);

        final FlowRegistryClientEntity existingRegClient = controllerClient.getRegistryClient(id);
        if (existingRegClient == null) {
            throw new CommandException("Registry client does not exist for id " + id);
        }

        final String name = getArg(properties, CommandOption.REGISTRY_CLIENT_NAME);
        final String desc = getArg(properties, CommandOption.REGISTRY_CLIENT_DESC);
        final String url = getArg(properties, CommandOption.REGISTRY_CLIENT_URL);
        final String sslServiceId = getArg(properties, CommandOption.SSL_CONTEXT_SERVICE_ID);

        if (StringUtils.isBlank(name) && StringUtils.isBlank(desc) && StringUtils.isBlank(url)) {
            throw new CommandException("Name, description, and URL were all blank, nothing to update");
        }

        if (StringUtils.isNotBlank(name)) {
            existingRegClient.getComponent().setName(name);
        }

        if (StringUtils.isNotBlank(desc)) {
            existingRegClient.getComponent().setDescription(desc);
        }

        if (StringUtils.isNotBlank(url)) {
            Map<String, String> clientProperties = existingRegClient.getComponent().getProperties();
            if (clientProperties == null) {
                clientProperties = new HashMap<>();
            } else {
                // Create a new map to avoid modifying the original
                clientProperties = new HashMap<>(clientProperties);
            }
            clientProperties.put("url", url);
            existingRegClient.getComponent().setProperties(clientProperties);
        }

        if (StringUtils.isNotBlank(sslServiceId)) {
            final Map<String, String> updatedProperties = new HashMap<>();
            if (existingRegClient.getComponent().getProperties() != null) {
                updatedProperties.putAll(existingRegClient.getComponent().getProperties());
            }

            updatedProperties.put("ssl-context-service", sslServiceId);
            existingRegClient.getComponent().setProperties(updatedProperties);
        }
        
        final String clientId = getContext().getSession().getNiFiClientID();
        existingRegClient.getRevision().setClientId(clientId);

        controllerClient.updateRegistryClient(existingRegClient);
        return VoidResult.getInstance();
    }
}
