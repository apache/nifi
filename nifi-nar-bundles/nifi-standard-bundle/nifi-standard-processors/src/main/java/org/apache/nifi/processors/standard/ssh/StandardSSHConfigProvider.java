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
package org.apache.nifi.processors.standard.ssh;

import net.schmizz.keepalive.KeepAlive;
import net.schmizz.keepalive.KeepAliveProvider;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.connection.ConnectionImpl;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.CIPHERS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT;

/**
 * Standard implementation of SSH Configuration Provider
 */
public class StandardSSHConfigProvider implements SSHConfigProvider {
    private static final String COMMA_SEPARATOR = ",";

    private static final int KEEP_ALIVE_INTERVAL_SECONDS = 5;

    private static final KeepAliveProvider DISABLED_KEEP_ALIVE_PROVIDER = new DisabledKeepAliveProvider();

    /**
     * Get SSH configuration based on configured properties
     *
     * @param identifier SSH Client identifier for runtime tracking
     * @param context Property Context
     * @return SSH Configuration
     */
    @Override
    public Config getConfig(final String identifier, final PropertyContext context) {
        final DefaultConfig config = new DefaultConfig();
        final KeepAliveProvider keepAliveProvider = getKeepAliveProvider(identifier, context);
        config.setKeepAliveProvider(keepAliveProvider);

        getOptionalProperty(context, CIPHERS_ALLOWED).ifPresent(property -> config.setCipherFactories(getFilteredValues(property, config.getCipherFactories())));
        getOptionalProperty(context, KEY_ALGORITHMS_ALLOWED).ifPresent(property -> config.setKeyAlgorithms(getFilteredValues(property, config.getKeyAlgorithms())));
        getOptionalProperty(context, KEY_EXCHANGE_ALGORITHMS_ALLOWED).ifPresent(property -> config.setKeyExchangeFactories(getFilteredValues(property, config.getKeyExchangeFactories())));
        getOptionalProperty(context, MESSAGE_AUTHENTICATION_CODES_ALLOWED).ifPresent(property -> config.setMACFactories(getFilteredValues(property, config.getMACFactories())));

        final String keyAlgorithmsAllowed = context.getProperty(KEY_ALGORITHMS_ALLOWED).evaluateAttributeExpressions().getValue();
        if (keyAlgorithmsAllowed == null) {
            // Prioritize ssh-rsa when Key Algorithms Allowed is not specified
            config.prioritizeSshRsaKeyAlgorithm();
        }

        return config;
    }

    private Optional<String> getOptionalProperty(final PropertyContext context, final PropertyDescriptor propertyDescriptor) {
        final PropertyValue propertyValue = context.getProperty(propertyDescriptor);
        return propertyValue.isSet() ? Optional.of(propertyValue.evaluateAttributeExpressions().getValue()) : Optional.empty();
    }

    private <T> List<Factory.Named<T>> getFilteredValues(final String propertyValue, final List<Factory.Named<T>> supportedValues) {
        final Set<String> configuredValues = getCommaSeparatedValues(propertyValue);
        return supportedValues.stream().filter(named -> configuredValues.contains(named.getName())).collect(Collectors.toList());
    }

    private Set<String> getCommaSeparatedValues(final String propertyValue) {
        final String[] values = propertyValue.split(COMMA_SEPARATOR);
        return Arrays.stream(values).map(String::trim).collect(Collectors.toSet());
    }

    private KeepAliveProvider getKeepAliveProvider(final String identifier, final PropertyContext context) {
        final boolean keepAliveEnabled = context.getProperty(USE_KEEPALIVE_ON_TIMEOUT).asBoolean();
        return keepAliveEnabled ? new EnabledKeepAliveProvider(identifier) : DISABLED_KEEP_ALIVE_PROVIDER;
    }

    private static class EnabledKeepAliveProvider extends KeepAliveProvider {
        private final String identifier;

        private EnabledKeepAliveProvider(final String identifier) {
            this.identifier = identifier;
        }

        @Override
        public KeepAlive provide(final ConnectionImpl connection) {
            final KeepAlive keepAlive = KeepAliveProvider.KEEP_ALIVE.provide(connection);
            keepAlive.setName(String.format("SSH-keep-alive-%s", identifier));
            keepAlive.setKeepAliveInterval(KEEP_ALIVE_INTERVAL_SECONDS);
            return keepAlive;
        }
    }

    private static class DisabledKeepAliveProvider extends KeepAliveProvider {

        @Override
        public KeepAlive provide(final ConnectionImpl connection) {
            return new DisabledKeepAliveThread(connection);
        }
    }

    private static class DisabledKeepAliveThread extends KeepAlive {

        private DisabledKeepAliveThread(final ConnectionImpl connection) {
            super(connection, "keep-alive-disabled");
        }

        @Override
        public void run() {

        }

        @Override
        protected void doKeepAlive() {

        }
    }
}
