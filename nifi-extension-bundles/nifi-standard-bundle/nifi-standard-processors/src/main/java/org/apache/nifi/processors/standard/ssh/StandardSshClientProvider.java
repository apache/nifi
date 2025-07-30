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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.standard.socket.ClientAuthenticationException;
import org.apache.nifi.processors.standard.socket.ClientConnectException;
import org.apache.nifi.processors.standard.ssh.netty.StandardNettyIoServiceFactoryFactory;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.sshd.client.ClientBuilder;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.keyverifier.DefaultKnownHostsServerKeyVerifier;
import org.apache.sshd.client.keyverifier.KnownHostsServerKeyVerifier;
import org.apache.sshd.client.keyverifier.RejectAllServerKeyVerifier;
import org.apache.sshd.client.keyverifier.ServerKeyVerifier;
import org.apache.sshd.client.keyverifier.StaticServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.compression.BuiltinCompressions;
import org.apache.sshd.common.compression.Compression;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.kex.BuiltinDHFactories;
import org.apache.sshd.common.kex.DHFactory;
import org.apache.sshd.common.kex.KeyExchangeFactory;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.apache.sshd.core.CoreModuleProperties;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.processor.util.file.transfer.FileTransfer.CONNECTION_TIMEOUT;
import static org.apache.nifi.processor.util.file.transfer.FileTransfer.HOSTNAME;
import static org.apache.nifi.processor.util.file.transfer.FileTransfer.PASSWORD;
import static org.apache.nifi.processor.util.file.transfer.FileTransfer.USERNAME;
import static org.apache.nifi.processor.util.file.transfer.FileTransfer.USE_COMPRESSION;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.ALGORITHM_CONFIGURATION;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.CIPHERS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.HOST_KEY_FILE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PORT;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PRIVATE_KEY_PASSPHRASE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PRIVATE_KEY_PATH;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.STRICT_HOST_KEY_CHECKING;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT;

/**
 * Standard Apache MINA SSHD Client Provider supporting configuration and authentication
 */
public class StandardSshClientProvider implements SshClientProvider {
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);

    private static final int HEARTBEAT_NO_REPLY_MAX = 5;

    private static final ServerKeyVerifier ACCEPT_ALL_SERVER_KEY_VERIFIER = new AcceptAllServerKeyVerifier();

    private static final List<NamedFactory<Compression>> COMPRESSION_FACTORIES = List.of(
            BuiltinCompressions.zlib,
            BuiltinCompressions.delayedZlib
    );

    /**
     * Get SSH Client Session connected and authenticated based on provided configuration
     *
     * @param context Property Context containing configuration properties
     * @param attributes FlowFile attributes for property expression evaluation
     * @return Connected and authenticated SSH Client Session
     */
    @Override
    public ClientSession getClientSession(final PropertyContext context, final Map<String, String> attributes) {
        Objects.requireNonNull(context, "Property Context required");
        Objects.requireNonNull(attributes, "Attributes required");

        final SshClient sshClient = buildSshClient(context);

        final boolean compressionEnabled = context.getProperty(USE_COMPRESSION).asBoolean();
        if (compressionEnabled) {
            sshClient.setCompressionFactories(COMPRESSION_FACTORIES);
        }

        final boolean keepAliveEnabled = context.getProperty(USE_KEEPALIVE_ON_TIMEOUT).asBoolean();
        if (keepAliveEnabled) {
            CoreModuleProperties.HEARTBEAT_INTERVAL.set(sshClient, HEARTBEAT_INTERVAL);
            CoreModuleProperties.HEARTBEAT_NO_REPLY_MAX.set(sshClient, HEARTBEAT_NO_REPLY_MAX);
        }

        setAlgorithmConfiguration(context, sshClient);
        setProxy(context, attributes, sshClient);
        setAuthentication(context, attributes, sshClient);

        sshClient.start();
        try {
            return getAuthenticatedClientSession(context, attributes, sshClient);
        } catch (final Exception e) {
            sshClient.stop();
            throw e;
        }
    }

    private ClientSession getAuthenticatedClientSession(final PropertyContext context, final Map<String, String> attributes, final SshClient sshClient) {
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(attributes).getValue();
        final Duration connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asDuration();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions(attributes).getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions(attributes).asInteger();

        final ClientSession clientSession;
        try {
            final ConnectFuture connectFuture = sshClient.connect(username, hostname, port).verify(connectionTimeout);
            clientSession = connectFuture.getClientSession();
        } catch (final IOException e) {
            throw new ClientConnectException("SSH Connection failed [%s:%d]".formatted(hostname, port), e);
        }

        try {
            clientSession.auth().verify(connectionTimeout);
        } catch (final IOException e) {
            throw new ClientAuthenticationException("SSH Authentication failed [%s:%d]".formatted(hostname, port), e);
        }

        // Close SshClient after closing Client Session
        clientSession.addCloseFutureListener(closeFuture -> sshClient.stop());
        return clientSession;
    }

    private SshClient buildSshClient(final PropertyContext context) {
        final ClientBuilder clientBuilder = ClientBuilder.builder();
        setHostKeyChecking(context, clientBuilder);

        final SFTPTransfer.AlgorithmConfiguration algorithmConfiguration = context.getProperty(ALGORITHM_CONFIGURATION).asAllowableValue(SFTPTransfer.AlgorithmConfiguration.class);
        if (SFTPTransfer.AlgorithmConfiguration.CUSTOM == algorithmConfiguration) {

            final String keyExchangeAlgorithmsAllowed = context.getProperty(KEY_EXCHANGE_ALGORITHMS_ALLOWED).evaluateAttributeExpressions().getValue();
            if (StringUtils.isNotBlank(keyExchangeAlgorithmsAllowed)) {
                final BuiltinDHFactories.ParseResult result = BuiltinDHFactories.parseDHFactoriesList(keyExchangeAlgorithmsAllowed);
                final List<DHFactory> parsedFactories = result.getParsedFactories();
                final List<KeyExchangeFactory> keyExchangeFactories = NamedFactory.setUpTransformedFactories(false, parsedFactories, ClientBuilder.DH2KEX);
                clientBuilder.keyExchangeFactories(keyExchangeFactories);
            }
        }

        return clientBuilder.build(true);
    }

    private void setHostKeyChecking(final PropertyContext context, final ClientBuilder clientBuilder) {
        final boolean strictHostKeyChecking = context.getProperty(STRICT_HOST_KEY_CHECKING).asBoolean();
        if (strictHostKeyChecking) {
            final String hostKeyFilePath = context.getProperty(HOST_KEY_FILE).getValue();
            if (hostKeyFilePath == null || hostKeyFilePath.isEmpty()) {
                clientBuilder.serverKeyVerifier(new DefaultKnownHostsServerKeyVerifier(RejectAllServerKeyVerifier.INSTANCE));
            } else {
                final Path hostKeyFile = Paths.get(hostKeyFilePath);
                clientBuilder.serverKeyVerifier(new KnownHostsServerKeyVerifier(RejectAllServerKeyVerifier.INSTANCE, hostKeyFile));
            }
        } else {
            clientBuilder.serverKeyVerifier(ACCEPT_ALL_SERVER_KEY_VERIFIER);
        }
    }

    private void setAlgorithmConfiguration(final PropertyContext context, final SshClient sshClient) {
        final SFTPTransfer.AlgorithmConfiguration algorithmConfiguration = context.getProperty(ALGORITHM_CONFIGURATION).asAllowableValue(SFTPTransfer.AlgorithmConfiguration.class);
        if (SFTPTransfer.AlgorithmConfiguration.CUSTOM == algorithmConfiguration) {
            final String ciphersAllowed = context.getProperty(CIPHERS_ALLOWED).evaluateAttributeExpressions().getValue();
            if (StringUtils.isNotBlank(ciphersAllowed)) {
                sshClient.setCipherFactoriesNameList(ciphersAllowed);
            }

            final String keyAlgorithmsAllowed = context.getProperty(KEY_ALGORITHMS_ALLOWED).evaluateAttributeExpressions().getValue();
            if (StringUtils.isNotBlank(keyAlgorithmsAllowed)) {
                sshClient.setSignatureFactoriesNameList(keyAlgorithmsAllowed);
            }

            final String macAlgorithmsAllowed = context.getProperty(MESSAGE_AUTHENTICATION_CODES_ALLOWED).evaluateAttributeExpressions().getValue();
            if (StringUtils.isNotBlank(macAlgorithmsAllowed)) {
                sshClient.setMacFactoriesNameList(macAlgorithmsAllowed);
            }
        }
    }

    private void setProxy(final PropertyContext context, final Map<String, String> attributes, final SshClient sshClient) {
        final ProxyConfiguration proxyConfiguration;

        final PropertyValue proxyConfigurationServiceProperty = context.getProperty(SFTPTransfer.PROXY_CONFIGURATION_SERVICE);
        if (proxyConfigurationServiceProperty.isSet()) {
            final ProxyConfigurationService proxyConfigurationService = proxyConfigurationServiceProperty.asControllerService(ProxyConfigurationService.class);
            proxyConfiguration = proxyConfigurationService.getConfiguration();
        } else {
            // Proxy Configuration with direct access
            proxyConfiguration = new ProxyConfiguration();
        }

        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions(attributes).getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions(attributes).asInteger();
        final InetSocketAddress remoteAddress = new InetSocketAddress(hostname, port);

        final Duration socketTimeout = context.getProperty(SFTPTransfer.DATA_TIMEOUT).asDuration();
        final StandardNettyIoServiceFactoryFactory ioServiceFactoryFactory = new StandardNettyIoServiceFactoryFactory(remoteAddress, proxyConfiguration, socketTimeout);
        sshClient.setIoServiceFactoryFactory(ioServiceFactoryFactory);
    }

    private void setAuthentication(final PropertyContext context, final Map<String, String> attributes, final SshClient sshClient) {
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(attributes).getValue();
        if (password != null) {
            sshClient.addPasswordIdentity(password);
        }

        final String privateKeyPath = context.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(attributes).getValue();
        if (privateKeyPath != null) {
            final Path privateKeyFilePath = Paths.get(privateKeyPath);
            final FileKeyPairProvider fileKeyPairProvider = new FileKeyPairProvider();
            fileKeyPairProvider.setPaths(List.of(privateKeyFilePath));

            final String privateKeyPassphrase = context.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions(attributes).getValue();
            if (privateKeyPassphrase != null) {
                final FilePasswordProvider filePasswordProvider = FilePasswordProvider.of(privateKeyPassphrase);
                fileKeyPairProvider.setPasswordFinder(filePasswordProvider);
            }

            sshClient.setKeyIdentityProvider(fileKeyPairProvider);
        }
    }

    protected static class AcceptAllServerKeyVerifier extends StaticServerKeyVerifier {

        protected AcceptAllServerKeyVerifier() {
            super(true);
        }

        /**
         * Handle Accepted Server Key and log at INFO instead of WARN
         *
         * @param sshClientSession SSH Client Session connected
         * @param remoteAddress Server Address accepted
         * @param serverKey Server Key accepted
         */
        @Override
        protected void handleAcceptance(final ClientSession sshClientSession, final SocketAddress remoteAddress, final PublicKey serverKey) {
            if (log.isInfoEnabled()) {
                final String algorithm = serverKey == null ? null : serverKey.getAlgorithm();
                final String fingerprint = KeyUtils.getFingerPrint(serverKey);
                log.info("Accepted unverified server {} [{}] Key {}", remoteAddress, algorithm, fingerprint);
            }
        }
    }
}
