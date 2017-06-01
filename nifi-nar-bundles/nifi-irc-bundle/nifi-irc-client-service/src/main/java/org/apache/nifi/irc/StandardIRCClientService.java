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
package org.apache.nifi.irc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.irc.handlers.ServiceEventHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.kitteh.irc.client.library.Client;
import org.kitteh.irc.client.library.feature.sending.SingleDelaySender;


import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

@Tags({ "irc"})
@CapabilityDescription("IRC client controller.")
public class StandardIRCClientService extends AbstractControllerService implements IRCClientService {

    public static final PropertyDescriptor IRC_SERVER = new PropertyDescriptor
            .Builder().name("IRC_SERVER")
            .displayName("IRC Server")
            .description("The IRC server you want to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_SERVER_PORT = new PropertyDescriptor
            .Builder().name("IRC_SERVER_PORT")
            .displayName("IRC Server Port")
            .description("The IRC server port you want to connect to")
            .required(true)
            .defaultValue("6667")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_TIMEOUT = new PropertyDescriptor
            .Builder().name("IRC_TIMEOUT")
            .displayName("IRC Timeout")
            .description("The amount of time to wait for certain actions to complete before timing-out")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL_CONTEXT_SERVICE")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, "
                    + "IRC connection will be established over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor IRC_NICK = new PropertyDescriptor
            .Builder().name("IRC_NICK")
            .displayName("Nickname")
            .description("The Nickname to use when connecting to the IRC server")
            .required(true)
            .defaultValue("NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_SERVER_PASSWORD = new PropertyDescriptor
            .Builder().name("IRC_SERVER_PASSWORD")
            .displayName("Password")
            .description("The password to be user for authentication")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;
    private Client ircClient;
    private ComponentLog logger;
    protected AtomicBoolean connectionStatus = new AtomicBoolean(false);

    protected String clientIdentification;
    private static final Set<String> requestedChannels = new HashSet<>();

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(IRC_SERVER);
        props.add(IRC_SERVER_PORT);
        props.add(IRC_TIMEOUT);
        props.add(IRC_NICK);
        props.add(IRC_SERVER_PASSWORD);
        props.add(SSL_CONTEXT_SERVICE);
        properties = Collections.unmodifiableList(props);
    }

    // Initialize MESSAGE_DELAY using KICL's default
    private AtomicInteger MESSAGE_DELAY = new AtomicInteger(SingleDelaySender.DEFAULT_MESSAGE_DELAY);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create an IRC connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
         clientIdentification = this.getIdentifier();

        this.logger = this.getLogger();

        Client.Builder clientSkeleton = Client
                .builder()
                .serverHost(context.getProperty(IRC_SERVER).getValue())
                .serverPort(context.getProperty(IRC_SERVER_PORT).asInteger())
                .nick(context.getProperty(IRC_NICK).getValue())
                .user("nifi")
                .realName(String.join(" - ", new String[] { this.getClass().getSimpleName(), this.clientIdentification }));

        // Setup Security
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService == null) {
            // Disabled...
            clientSkeleton.secure(false);
        } else {
            // Enabled
            clientSkeleton.secure(true);
            SSLContext sslContext;
            TrustManagerFactory tmf;

            // Is key configured? If yes, populate and let it go...
            if (sslContextService.isKeyStoreConfigured()) {
                final String keyPassword = sslContextService.getKeyPassword();
                final String keyFile = sslContextService.getKeyStoreFile();

                if (!StringUtils.isEmpty(keyPassword) && !StringUtils.isEmpty(keyFile) ) {
                    clientSkeleton.secureKeyPassword(keyPassword);
                    clientSkeleton.secureKey(new File(keyFile));
                    sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
                } else {
                    sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
                }

                try {
                    Provider tlsProvider = sslContext.getProvider();

                    final KeyStore trustStore = KeyStoreUtils.getTrustStore(sslContextService.getTrustStoreType());
                    try (final InputStream trustStoreStream = new FileInputStream(new File(sslContextService.getTrustStoreFile()))) {
                        trustStore.load(trustStoreStream, sslContextService.getTrustStorePassword().toCharArray());
                    }

                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);

                    clientSkeleton.secureTrustManagerFactory(tmf);
                } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {
                    logger.error("Failed to initialize secure IRC service due to {}", new Object[]{e.getMessage()}, e);
                }

            }
        }
        this.ircClient = clientSkeleton.build();

        // Setup the Server Handlers
        ServiceEventHandler serviceEventHandler = new ServiceEventHandler(connectionStatus, requestedChannels, getLogger());
        ircClient.getEventManager().registerEventListener(serviceEventHandler);
    }

    @OnDisabled
    public void shutdown() {
        Long timeOut = getConfigurationContext().getProperty(IRC_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final StopWatch stopWatch = new StopWatch();

        stopWatch.start();

        this.ircClient.shutdown(clientIdentification + " - is going to rest a bit...");
        while (this.connectionStatus.get() || ( stopWatch.getElapsed(TimeUnit.MILLISECONDS) <= timeOut ) ) {
            // Wait for the disconnection
        }
        stopWatch.stop();
        logger.info("Disconnected from server after {} milliseconds", new Object[]{stopWatch.getDuration(TimeUnit.MILLISECONDS)});
    }

    @Override
    public void joinChannel(String channel) throws ProcessException {
        this.requestedChannels.add(channel);
        this.getClient().addChannel(channel);
    }

    @Override
    public Client getClient() throws ProcessException {
        return this.ircClient;
    }

    @Override
    public AtomicBoolean getIsConnected() throws ProcessException {
        return this.connectionStatus;
    }

    @Override
    public int setAndGetDelay(int delay) {
        synchronized (this.MESSAGE_DELAY) {
            this.MESSAGE_DELAY.getAndSet(delay);
            this.ircClient.setMessageSendingQueueSupplier(SingleDelaySender.getSupplier(this.MESSAGE_DELAY.get()));
        return this.MESSAGE_DELAY.get();
        }
    }


}
