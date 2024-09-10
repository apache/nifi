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
package org.apache.nifi.processors.standard.ftp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processors.standard.socket.ClientAuthenticationException;
import org.apache.nifi.processors.standard.socket.ClientConfigurationException;
import org.apache.nifi.processors.standard.socket.ClientConnectException;
import org.apache.nifi.processors.standard.socket.SocketFactoryProvider;
import org.apache.nifi.processors.standard.socket.StandardSocketFactoryProvider;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nifi.processors.standard.util.FTPTransfer.BUFFER_SIZE;
import static org.apache.nifi.processors.standard.util.FTPTransfer.CONNECTION_MODE;
import static org.apache.nifi.processors.standard.util.FTPTransfer.CONNECTION_MODE_ACTIVE;
import static org.apache.nifi.processors.standard.util.FTPTransfer.CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.standard.util.FTPTransfer.DATA_TIMEOUT;
import static org.apache.nifi.processors.standard.util.FTPTransfer.HOSTNAME;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PASSWORD;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PORT;
import static org.apache.nifi.processors.standard.util.FTPTransfer.TRANSFER_MODE;
import static org.apache.nifi.processors.standard.util.FTPTransfer.TRANSFER_MODE_ASCII;
import static org.apache.nifi.processors.standard.util.FTPTransfer.USERNAME;
import static org.apache.nifi.processors.standard.util.FTPTransfer.UTF8_ENCODING;

/**
 * Standard implementation of FTP Client Provider
 */
public class StandardFTPClientProvider implements FTPClientProvider {
    private static final SocketFactoryProvider SOCKET_FACTORY_PROVIDER = new StandardSocketFactoryProvider();

    private static final String ADDRESS_FORMAT = "%s:%d";

    private static final Logger logger = LoggerFactory.getLogger(StandardFTPClientProvider.class);

    /**
     * Get configured FTP Client using context properties and attributes
     *
     * @param context Property Context
     * @param attributes FlowFile attributes for property expression evaluation
     * @return Configured FTP Client
     */
    @Override
    public FTPClient getClient(final PropertyContext context, final Map<String, String> attributes) {
        Objects.requireNonNull(context, "Property Context required");
        Objects.requireNonNull(attributes, "Attributes required");

        final FTPClient client = createClient(context);
        setClientProperties(client, context);

        final boolean attributesEmpty = attributes.isEmpty();

        // Evaluate Hostname and Port properties based on the presence of attributes because ListFTP does not support FlowFile attributes
        final PropertyValue hostnameProperty = context.getProperty(HOSTNAME);
        final String hostname = attributesEmpty
                ? hostnameProperty.evaluateAttributeExpressions().getValue()
                : hostnameProperty.evaluateAttributeExpressions(attributes).getValue();

        final PropertyValue portProperty = context.getProperty(PORT);
        final int port = attributesEmpty
                ? portProperty.evaluateAttributeExpressions().asInteger()
                : portProperty.evaluateAttributeExpressions(attributes).asInteger();
        final String address = String.format(ADDRESS_FORMAT, hostname, port);

        try {
            client.connect(hostname, port);
            final int dataTimeout = context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            client.setSoTimeout(dataTimeout);
        } catch (final IOException e) {
            disconnectClient(client);
            throw new ClientConnectException(String.format("FTP Client connection failed [%s]", address), e);
        }

        final PropertyValue usernameProperty = context.getProperty(USERNAME);
        final String username = attributesEmpty
                ? usernameProperty.evaluateAttributeExpressions().getValue()
                : usernameProperty.evaluateAttributeExpressions(attributes).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(attributes).getValue();

        try {
            final boolean authenticated = client.login(username, password);
            if (authenticated) {
                logger.debug("FTP login completed: [{}]", address);
            } else {
                disconnectClient(client);
                throw new ClientAuthenticationException(String.format("FTP Client login denied [%s]", address));
            }
        } catch (final IOException e) {
            disconnectClient(client);
            throw new ClientAuthenticationException(String.format("FTP Client login failed [%s]", address), e);
        }

        final String connectionMode = context.getProperty(CONNECTION_MODE).getValue();
        if (connectionMode.equalsIgnoreCase(CONNECTION_MODE_ACTIVE)) {
            client.enterLocalActiveMode();
        } else {
            client.enterLocalPassiveMode();
        }

        final String transferMode = context.getProperty(TRANSFER_MODE).getValue();
        final int fileType = (transferMode.equalsIgnoreCase(TRANSFER_MODE_ASCII)) ? FTPClient.ASCII_FILE_TYPE : FTPClient.BINARY_FILE_TYPE;
        try {
            if (client.setFileType(fileType)) {
                logger.debug("FTP set transfer mode [{}] completed: {}", transferMode, address);
            } else {
                throw new ClientConfigurationException(String.format("FTP Client set transfer mode [%s] rejected [%s]", transferMode, address));
            }
        } catch (final IOException e) {
            throw new ClientConfigurationException(String.format("FTP Client set transfer mode [%s] failed [%s]", transferMode, address), e);
        }

        return client;
    }

    private void setClientProperties(final FTPClient client, final PropertyContext context) {
        final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Duration dataTimeout = context.getProperty(DATA_TIMEOUT).asDuration();
        final int connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        client.setBufferSize(bufferSize);
        client.setDataTimeout(dataTimeout);
        client.setDefaultTimeout(connectionTimeout);
        client.setRemoteVerificationEnabled(false);
        client.setAutodetectUTF8(true);

        final boolean unicodeEnabled = context.getProperty(UTF8_ENCODING).isSet() ? context.getProperty(UTF8_ENCODING).asBoolean() : false;
        // in non-UTF-8 mode, FTP control encoding should be left as default (ISO-8859-1)
        if (unicodeEnabled) {
            client.setControlEncoding(StandardCharsets.UTF_8.name());
        }
    }

    private void disconnectClient(final FTPClient client) {
        try {
            client.disconnect();
        } catch (final IOException e) {
            throw new UncheckedIOException("FTP Client disconnect failed", e);
        }
    }

    private FTPClient createClient(final PropertyContext context) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        final Proxy.Type proxyType = proxyConfiguration.getProxyType();

        final FTPClient client = new FTPClient();
        if (Proxy.Type.HTTP == proxyType || Proxy.Type.SOCKS == proxyType) {
            final SocketFactory socketFactory = SOCKET_FACTORY_PROVIDER.getSocketFactory(proxyConfiguration);
            client.setSocketFactory(socketFactory);
            // Disable NAT workaround for proxy connections
            client.setPassiveNatWorkaroundStrategy(null);
        }

        return client;
    }
}
