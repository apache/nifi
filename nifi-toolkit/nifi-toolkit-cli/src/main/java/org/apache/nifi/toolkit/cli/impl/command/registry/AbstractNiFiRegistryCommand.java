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
package org.apache.nifi.toolkit.cli.impl.command.registry;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractPropertyCommand;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariables;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Base class for all NiFi Reg commands.
 */
public abstract class AbstractNiFiRegistryCommand extends AbstractPropertyCommand {

    public AbstractNiFiRegistryCommand(final String name) {
        super(name);
    }

    @Override
    protected SessionVariables getPropertiesSessionVariable() {
        return SessionVariables.NIFI_REGISTRY_CLIENT_PROPS;
    }

    @Override
    protected void doExecute(final Properties properties) throws CommandException {
        final ClientFactory<NiFiRegistryClient> clientFactory = getContext().getNiFiRegistryClientFactory();
        try (final NiFiRegistryClient client = clientFactory.createClient(properties)) {
            doExecute(client, properties);
        } catch (Exception e) {
            throw new CommandException("Error executing command '" + getName() + "' : " + e.getMessage(), e);
        }
    }

    /**
     * Sub-classes implement specific functionality using the provided client.
     *
     * @param client the NiFiRegistryClient to use for performing the action
     * @param properties the properties for the command
     */
    protected abstract void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException;

    /*
     * NOTE: This will bring back every item in the registry. We should create an end-point on the registry side
     * to retrieve a flow by id and remove this later.
     */
    protected String getBucketId(final NiFiRegistryClient client, final String flowId) throws IOException, NiFiRegistryException {
        final List<BucketItem> items = client.getItemsClient().getAll();

        final Optional<BucketItem> matchingItem = items.stream()
                .filter(i ->  i.getIdentifier().equals(flowId))
                .findFirst();

        if (!matchingItem.isPresent()) {
            throw new NiFiRegistryException("Versioned flow does not exist with id " + flowId);
        }

        return matchingItem.get().getBucketIdentifier();
    }

}
