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
import net.schmizz.sshj.Config;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.Connection;
import net.schmizz.sshj.transport.Transport;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyFormat;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.userauth.keyprovider.KeyProviderUtil;
import net.schmizz.sshj.userauth.method.AuthKeyboardInteractive;
import net.schmizz.sshj.userauth.method.AuthMethod;
import net.schmizz.sshj.userauth.method.AuthPassword;
import net.schmizz.sshj.userauth.method.AuthPublickey;
import net.schmizz.sshj.userauth.method.PasswordResponseProvider;
import net.schmizz.sshj.userauth.password.PasswordFinder;
import net.schmizz.sshj.userauth.password.PasswordUtils;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.standard.socket.ClientAuthenticationException;
import org.apache.nifi.processors.standard.socket.ClientConfigurationException;
import org.apache.nifi.processors.standard.socket.ClientConnectException;
import org.apache.nifi.processors.standard.socket.SocketFactoryProvider;
import org.apache.nifi.processors.standard.socket.StandardSocketFactoryProvider;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.StringUtils;

import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.standard.util.SFTPTransfer.CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.DATA_TIMEOUT;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.HOSTNAME;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.HOST_KEY_FILE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PASSWORD;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PORT;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PRIVATE_KEY_PASSPHRASE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PRIVATE_KEY_PATH;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.STRICT_HOST_KEY_CHECKING;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.USERNAME;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.USE_COMPRESSION;

/**
 * Standard implementation of SSH Client Provider
 */
public class StandardSSHClientProvider implements SSHClientProvider {

    private static final SSHConfigProvider SSH_CONFIG_PROVIDER = new StandardSSHConfigProvider();
    private static final SocketFactoryProvider SOCKET_FACTORY_PROVIDER = new StandardSocketFactoryProvider();

    private static final List<Proxy.Type> SUPPORTED_PROXY_TYPES = List.of(Proxy.Type.HTTP, Proxy.Type.SOCKS);

    private static final String ADDRESS_FORMAT = "%s:%d";

    /**
     * Get configured and authenticated SSH Client based on context properties
     *
     * @param context Property Context
     * @param attributes FlowFile attributes for property expression evaluation
     * @return Authenticated SSH Client
     */
    @Override
    public SSHClient getClient(final PropertyContext context, final Map<String, String> attributes) {
        Objects.requireNonNull(context, "Property Context required");
        Objects.requireNonNull(attributes, "Attributes required");

        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions(attributes).getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions(attributes).asInteger();
        final String address = String.format(ADDRESS_FORMAT, hostname, port);

        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(attributes).getValue();
        final List<AuthMethod> authMethods = getPasswordAuthMethods(context, attributes);

        final Config config = SSH_CONFIG_PROVIDER.getConfig(address, context);
        final SSHClient client = new StandardSSHClient(config);

        try {
            setClientProperties(client, context);
        } catch (final Exception e) {
            closeClient(client);
            throw new ClientConfigurationException(String.format("SSH Client configuration failed [%s]", address), e);
        }

        try {
            client.connect(hostname, port);
        } catch (final Exception e) {
            closeClient(client);
            throw new ClientConnectException(String.format("SSH Client connection failed [%s]", address), e);
        }

        try {
            final List<AuthMethod> publicKeyAuthMethods = getPublicKeyAuthMethods(client, context, attributes);
            authMethods.addAll(publicKeyAuthMethods);
            client.auth(username, authMethods);
        } catch (final Exception e) {
            closeClient(client);
            throw new ClientAuthenticationException(String.format("SSH Client authentication failed [%s]", address), e);
        }

        return client;
    }

    private void closeClient(final SSHClient client) {
        try {
            client.close();
        } catch (final IOException e) {
            throw new UncheckedIOException("SSH Client close failed", e);
        } finally {
            final Connection connection = client.getConnection();
            final KeepAlive keepAlive = connection.getKeepAlive();
            keepAlive.interrupt();
        }
    }

    private void setClientProperties(final SSHClient client, final PropertyContext context) {
        final int connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        client.setConnectTimeout(connectionTimeout);

        final int dataTimeout = context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        client.setTimeout(dataTimeout);

        // Set Transport and Connection timeouts using Socket Data Timeout property
        final Transport transport = client.getTransport();
        transport.setTimeoutMs(dataTimeout);
        final Connection connection = client.getConnection();
        connection.setTimeoutMs(dataTimeout);

        final boolean strictHostKeyChecking = context.getProperty(STRICT_HOST_KEY_CHECKING).asBoolean();
        final String hostKeyFilePath = context.getProperty(HOST_KEY_FILE).getValue();
        if (StringUtils.isNotBlank(hostKeyFilePath)) {
            final File knownHosts = new File(hostKeyFilePath);
            try {
                client.loadKnownHosts(knownHosts);
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Loading Known Hosts [%s] Failed", hostKeyFilePath), e);
            }
        } else if (strictHostKeyChecking) {
            try {
                client.loadKnownHosts();
            } catch (final IOException e) {
                throw new UncheckedIOException("Loading Known Hosts Failed", e);
            }
        } else {
            client.addHostKeyVerifier(new PromiscuousVerifier());
        }

        final boolean compressionEnabled = context.getProperty(USE_COMPRESSION).asBoolean();
        if (compressionEnabled) {
            try {
                client.useCompression();
            } catch (final TransportException e) {
                throw new UncheckedIOException("Enabling Compression Failed", e);
            }
        }

        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);
        final Proxy.Type proxyType = proxyConfiguration.getProxyType();
        if (SUPPORTED_PROXY_TYPES.contains(proxyType)) {
            final SocketFactory socketFactory = SOCKET_FACTORY_PROVIDER.getSocketFactory(proxyConfiguration);
            client.setSocketFactory(socketFactory);
        }
    }

    private List<AuthMethod> getPasswordAuthMethods(final PropertyContext context, final Map<String, String> attributes) {
        final List<AuthMethod> passwordAuthMethods = new ArrayList<>();

        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(attributes).getValue();
        if (password != null) {
            final AuthMethod authPassword = new AuthPassword(getPasswordFinder(password));
            passwordAuthMethods.add(authPassword);

            final PasswordResponseProvider passwordProvider = new PasswordResponseProvider(getPasswordFinder(password));
            final AuthMethod authKeyboardInteractive = new AuthKeyboardInteractive(passwordProvider);
            passwordAuthMethods.add(authKeyboardInteractive);
        }

        return passwordAuthMethods;
    }

    private List<AuthMethod> getPublicKeyAuthMethods(final SSHClient client, final PropertyContext context, final Map<String, String> attributes) {
        final List<AuthMethod> publicKeyAuthMethods = new ArrayList<>();

        final String privateKeyPath = context.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(attributes).getValue();
        if (privateKeyPath != null) {
            final String privateKeyPassphrase = context.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions(attributes).getValue();
            final KeyProvider keyProvider = getKeyProvider(client, privateKeyPath, privateKeyPassphrase);
            final AuthMethod authPublicKey = new AuthPublickey(keyProvider);
            publicKeyAuthMethods.add(authPublicKey);
        }

        return publicKeyAuthMethods;
    }

    private KeyProvider getKeyProvider(final SSHClient client, final String privateKeyLocation, final String privateKeyPassphrase) {
        final KeyFormat keyFormat = getKeyFormat(privateKeyLocation);
        try {
            return privateKeyPassphrase == null ? client.loadKeys(privateKeyLocation) : client.loadKeys(privateKeyLocation, privateKeyPassphrase);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("Loading Private Key File [%s] Format [%s] Failed", privateKeyLocation, keyFormat), e);
        }
    }

    private KeyFormat getKeyFormat(final String privateKeyLocation) {
        try {
            final File privateKeyFile = new File(privateKeyLocation);
            return KeyProviderUtil.detectKeyFileFormat(privateKeyFile);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("Reading Private Key File [%s] Format Failed", privateKeyLocation), e);
        }
    }

    private PasswordFinder getPasswordFinder(final String password) {
        return PasswordUtils.createOneOff(password.toCharArray());
    }
}
