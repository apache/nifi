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
package org.apache.nifi.toolkit.cli.impl.command.composite;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.api.SessionException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommand;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base class for higher-level marco commands that interact with NiFi & Registry to perform a series of actions.
 */
public abstract class AbstractCompositeCommand<R extends Result> extends AbstractCommand<R> {

    public AbstractCompositeCommand(final String name, final Class<R> resultClass) {
        super(name, resultClass);
    }

    @Override
    protected final Options createBaseOptions() {
        final Options options = new Options();
        options.addOption(CommandOption.NIFI_PROPS.createOption());
        options.addOption(CommandOption.NIFI_REG_PROPS.createOption());
        options.addOption(CommandOption.VERBOSE.createOption());
        return options;
    }

    @Override
    public final R execute(final CommandLine cli) throws CommandException {
        try {
            final Properties nifiProperties = createProperties(cli, CommandOption.NIFI_PROPS, SessionVariable.NIFI_CLIENT_PROPS);
            if (nifiProperties == null) {
                throw new CommandException("Unable to find NiFi config, must specify --"
                        + CommandOption.NIFI_PROPS.getLongName() + ", or setup session config");
            }

            final ClientFactory<NiFiClient> nifiClientFactory = getContext().getNiFiClientFactory();
            final NiFiClient nifiClient = nifiClientFactory.createClient(nifiProperties);

            final Properties registryProperties = createProperties(cli, CommandOption.NIFI_REG_PROPS, SessionVariable.NIFI_REGISTRY_CLIENT_PROPS);
            if (registryProperties == null) {
                throw new CommandException("Unable to find NiFi Registry config, must specify --"
                        + CommandOption.NIFI_REG_PROPS.getLongName() + ", or setup session config");
            }

            final ClientFactory<NiFiRegistryClient> registryClientFactory = getContext().getNiFiRegistryClientFactory();
            final NiFiRegistryClient  registryClient = registryClientFactory.createClient(registryProperties);

            return doExecute(cli, nifiClient, nifiProperties, registryClient, registryProperties);
        } catch (CommandException ce) {
            throw ce;
        } catch (Exception e) {
            throw new CommandException("Error executing command '" + getName() + "' : " + e.getMessage(), e);
        }
    }

    /**
     * Creates a Properties instance by looking at the propertOption and falling back to the session.
     *
     * @param commandLine the current command line
     * @param propertyOption the options specifying a properties to load
     * @param sessionVariable the session variable specifying a properties file
     * @return a Properties instance or null if the option wasn't specified and nothing is in the session
     */
    private Properties createProperties(final CommandLine commandLine, final CommandOption propertyOption, final SessionVariable sessionVariable)
            throws IOException, SessionException {

        // use the properties file specified by the properyOption if it exists
        if (commandLine.hasOption(propertyOption.getLongName())) {
            final String propertiesFile = commandLine.getOptionValue(propertyOption.getLongName());
            if (!StringUtils.isBlank(propertiesFile)) {
                try (final InputStream in = new FileInputStream(propertiesFile)) {
                    final Properties properties = new Properties();
                    properties.load(in);
                    return properties;
                }
            }
        } else {
            // no properties file was specified so see if there is anything in the session
            if (sessionVariable != null) {
                final Session session = getContext().getSession();
                final String sessionPropsFiles = session.get(sessionVariable.getVariableName());
                if (!StringUtils.isBlank(sessionPropsFiles)) {
                    try (final InputStream in = new FileInputStream(sessionPropsFiles)) {
                        final Properties properties = new Properties();
                        properties.load(in);
                        return properties;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Sub-classes implement specific logic using both clients.
     */
    public abstract R doExecute(final CommandLine commandLine,
                                final NiFiClient nifiClient,
                                final Properties nifiProperties,
                                final NiFiRegistryClient nifiRegistryClient,
                                final Properties nifiRegistryProperties)
            throws CommandException, IOException, NiFiRegistryException, ParseException, NiFiClientException;

}
