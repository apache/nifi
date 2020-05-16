/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.ignite;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.spring.IgniteSpringHelperImpl;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import java.net.URL;
import java.util.List;

/**
 * Base class for Ignite processors.
 */
public abstract class AbstractIgniteProcessor extends AbstractProcessor {

    /**
     * Ignite Spring configuration file.
     */
    public static final PropertyDescriptor IGNITE_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
            .displayName("Ignite Spring Properties XML File")
            .name("ignite-spring-properties-xml-file")
            .description("Ignite Spring configuration file, <path>/<ignite-configuration>.xml. If the configuration " +
                    "file is not provided, default Ignite configuration is used which binds to 127.0.0.1:47500..47509 " +
                    "in the case of thick client type or 127.0.0.1:10800 in the case of thin client type.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Success relationship.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are written to Ignite Cache are routed to this relationship.")
            .build();

    /**
     * Failure relationship.
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to Ignite Cache are routed to this relationship.")
            .build();

    /**
     * Allowable value for Thick Ignite client.
     */
    private static final AllowableValue THICK = new AllowableValue(ClientType.THICK.name(), ClientType.THICK.getDisplayName(), "Make use of thick Ignite client.");

    /**
     * Allowable value for Thin Ignite client.
     */
    private static final AllowableValue THIN = new AllowableValue(ClientType.THIN.name(), ClientType.THIN.getDisplayName(), "Make use of thin Ignite client.");

    /**
     * Whether to use the thick Ignite client or the thin Ignite client.
     */
    public static final PropertyDescriptor IGNITE_CLIENT_TYPE = new PropertyDescriptor.Builder()
            .displayName("Ignite Client Type")
            .name("ignite-client-type")
            .description("Whether to use the thick Ignite client or the thin Ignite client. Defaults to the thick Ignite client.")
            .required(true)
            .defaultValue(THICK.getValue())
            .allowableValues(THICK, THIN)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    /**
     * The thick Ignite client instance.
     */
    private transient Ignite thickIgniteClient;

    /**
     * The thin Ignite client instance.
     */
    private transient IgniteClient thinIgniteClient;

    /**
     * Get the thick Ignite client instance.
     *
     * @return Thick Ignite client instance.
     */
    protected Ignite getThickIgniteClient() {
        return thickIgniteClient;
    }

    /**
     * Get the thin Ignite client instance.
     *
     * @return Thin Ignite client instance.
     */
    public IgniteClient getThinIgniteClient() {
        return thinIgniteClient;
    }

    /**
     * Initialize the Ignite client instance with respect to the IGNITE_CLIENT_TYPE value, ensuring that only one
     * client type is active at a given point in time.
     *
     * @param context Process context.
     */
    protected void initializeIgniteClient(final ProcessContext context) {
        final ClientType clientType = ClientType.valueOf(context.getProperty(IGNITE_CLIENT_TYPE).getValue());
        if (clientType.equals(ClientType.THICK)) {
            if (thickIgniteClient != null) {
                getLogger().info("Thick Ignite client already initialized.");
                return;
            }

            synchronized (Ignition.class) {
                final List<Ignite> grids = Ignition.allGrids();

                if (grids.size() == 1) {
                    getLogger().info("Ignite grid already available.");
                    thickIgniteClient = grids.get(0);
                    return;
                }
                Ignition.setClientMode(true);

                final String configuration = context.getProperty(IGNITE_CONFIGURATION_FILE).getValue();
                getLogger().info("Initializing thick Ignite client with configuration {}.", new Object[]{configuration});
                if (StringUtils.isEmpty(configuration)) {
                    thickIgniteClient = Ignition.start();
                } else {
                    thickIgniteClient = Ignition.start(configuration);
                }
            }
        } else if (clientType.equals(ClientType.THIN)) {
            if (thinIgniteClient != null) {
                getLogger().info("Thin Ignite client already initialized.");
                return;
            }

            synchronized (Ignition.class) {
                final String configuration = context.getProperty(IGNITE_CONFIGURATION_FILE).getValue();
                getLogger().info("Initializing thin Ignite client with configuration {}.", new Object[]{configuration});
                if (StringUtils.isEmpty(configuration)) {
                    final ClientConfiguration clientConfiguration = getDefaultClientConfiguration();
                    thinIgniteClient = Ignition.startClient(clientConfiguration);
                } else {
                    final ClientConfiguration clientConfiguration = getClientConfiguration(configuration);
                    thinIgniteClient = Ignition.startClient(clientConfiguration);
                }
            }
        }
    }

    /**
     * Close the Ignite client instance.
     */
    protected void closeIgniteClient() {
        closeThickIgniteClient();
        closeThinIgniteClient();
    }

    /**
     * Get default thin Ignite client configuration.
     *
     * @return Default thin Ignite client configuration.
     */
    private ClientConfiguration getDefaultClientConfiguration() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setAddresses("127.0.0.1:10800");

        return clientConfiguration;
    }

    /**
     * Get thin Ignite client configuration from configuration path.
     *
     * @param configurationPath Configuration path.
     * @return Thin Ignite client configuration.
     */
    private ClientConfiguration getClientConfiguration(final String configurationPath) {
        final ApplicationContext springContext;
        try {
            final URL configurationUrl = IgniteUtils.resolveSpringUrl(configurationPath);
            springContext = IgniteSpringHelperImpl.applicationContext(configurationUrl);
        } catch (final IgniteCheckedException exception) {
            throw IgniteUtils.convertException(exception);
        }

        try {
            return springContext.getBean(ClientConfiguration.class);
        } catch (final BeansException exception) {
            throw IgniteUtils.convertException(new IgniteCheckedException("Failed to instantiate bean [type=" + ClientConfiguration.class + ", err=" + exception.getMessage() + "].", exception));
        }
    }

    /**
     * Close the thick Ignite client instance.
     */
    private void closeThickIgniteClient() {
        if (thickIgniteClient != null) {
            getLogger().info("Closing thick Ignite client instance.");
            thickIgniteClient.close();
            thickIgniteClient = null;
        } else {
            getLogger().info("Thick Ignite client instance already closed.");
        }
    }

    /**
     * Close the thin Ignite client instance.
     */
    private void closeThinIgniteClient() {
        if (thinIgniteClient != null) {
            getLogger().info("Closing thin Ignite client instance.");
            try {
                thinIgniteClient.close();
            } catch (final Exception exception) {
                getLogger().error("Error occurred when closing thin Ignite client instance.", exception);
            }
            thinIgniteClient = null;
        } else {
            getLogger().info("Thin Ignite client instance already closed.");
        }
    }
}
