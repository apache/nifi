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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.command.AbstractPropertyCommand;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Base class for all NiFi Reg commands.
 */
public abstract class AbstractNiFiRegistryCommand<R extends Result> extends AbstractPropertyCommand<R> {

    public AbstractNiFiRegistryCommand(final String name, final Class<R> resultClass) {
        super(name, resultClass);
    }

    @Override
    protected SessionVariable getPropertiesSessionVariable() {
        return SessionVariable.NIFI_REGISTRY_CLIENT_PROPS;
    }

    @Override
    public final R doExecute(final Properties properties) throws CommandException {
        final ClientFactory<NiFiRegistryClient> clientFactory = getContext().getNiFiRegistryClientFactory();
        try (final NiFiRegistryClient client = clientFactory.createClient(properties)) {
            return doExecute(client, properties);
        } catch (Exception e) {
            throw new CommandException("Error executing command '" + getName() + "' : " + e.getMessage(), e);
        }
    }

    /**
     * Sub-classes implement specific functionality using the provided client.
     *
     * @param client the NiFiRegistryClient to use for performing the action
     * @param properties the properties for the command
     * @return the Result of executing the command
     */
    public abstract R doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException;

    protected List<Integer> getVersions(final NiFiRegistryClient client, final String flowId)
            throws NiFiRegistryException, IOException {
        final FlowSnapshotClient srcSnapshotClient = client.getFlowSnapshotClient();
        final List<VersionedFlowSnapshotMetadata> srcVersionMetadata = srcSnapshotClient.getSnapshotMetadata(flowId);
        return srcVersionMetadata.stream().map(s -> s.getVersion()).collect(Collectors.toList());
    }

    /*
     * If srcProps was specified then load the properties and create a new client for the source,
     * but if it wasn't then assume the source is the same registry we already know about
     */
    protected NiFiRegistryClient getSourceClient(final NiFiRegistryClient client, final String srcPropsValue)
            throws IOException, org.apache.commons.cli.MissingOptionException {
        final NiFiRegistryClient srcClient;
        if (!StringUtils.isBlank(srcPropsValue)) {
            final Properties srcProps = new Properties();
            try (final InputStream in = new FileInputStream(srcPropsValue)) {
                srcProps.load(in);
            }

            final ClientFactory<NiFiRegistryClient> clientFactory = getContext().getNiFiRegistryClientFactory();
            srcClient = clientFactory.createClient(srcProps);
        } else {
            srcClient = client;
        }
        return srcClient;
    }

}
