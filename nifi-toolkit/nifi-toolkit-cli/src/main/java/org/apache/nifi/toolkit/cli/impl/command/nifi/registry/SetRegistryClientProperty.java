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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SetRegistryClientProperty extends AbstractNiFiCommand<StringResult> {

    public SetRegistryClientProperty() {
        super("set-reg-client-property", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Sets a property on a registry client.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.PROPERTY_NAME.createOption());
        addOption(CommandOption.PROPERTY_VALUE.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String regClientId = getRequiredArg(properties, CommandOption.REGISTRY_CLIENT_ID);
        final String propertyName = getRequiredArg(properties, CommandOption.PROPERTY_NAME);
        final String propertyValue = getRequiredArg(properties, CommandOption.PROPERTY_VALUE);

        final FlowRegistryClientEntity existingRegClient = client.getControllerClient().getRegistryClient(regClientId);
        if (existingRegClient == null) {
            throw new CommandException("Registry client does not exist for id " + regClientId);
        }

        final FlowRegistryClientDTO existingRegClientDTO = existingRegClient.getComponent();
        final FlowRegistryClientDTO newRegClientDTO = new FlowRegistryClientDTO();

        newRegClientDTO.setId(regClientId);
        newRegClientDTO.setName(existingRegClientDTO.getName());
        newRegClientDTO.setType(existingRegClientDTO.getType());
        newRegClientDTO.setDescription(existingRegClientDTO.getDescription());
        newRegClientDTO.setBundle(existingRegClientDTO.getBundle());

        final Map<String, String> updatedProperties = new HashMap<>();
        if (existingRegClientDTO.getProperties() != null) {
            updatedProperties.putAll(existingRegClientDTO.getProperties());
        }

        updatedProperties.put(propertyName, propertyValue);
        newRegClientDTO.setProperties(updatedProperties);

        final FlowRegistryClientEntity newRegClient = new FlowRegistryClientEntity();
        newRegClient.setComponent(newRegClientDTO);
        newRegClient.setId(regClientId);
        newRegClient.setRevision(existingRegClient.getRevision());

        final FlowRegistryClientEntity updatedRegistryClient = client.getControllerClient().updateRegistryClient(newRegClient);
        return new StringResult(updatedRegistryClient.getId(), isInteractive());
    }
}
