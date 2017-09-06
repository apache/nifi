/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.processors.email;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.email.smtp.SmtpConsumer;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.springframework.util.StringUtils;
import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandlerFactory;
import org.subethamail.smtp.server.SMTPServer;

@Tags({"listen", "email", "smtp"})
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor implements a lightweight SMTP server to an arbitrary port, "
        + "allowing nifi to listen for incoming email. Note this server does not perform any email "
        + "validation. If direct exposure to the internet is sought, it may be a better idea to use "
        + "the combination of NiFi and an industrial scale MTA (e.g. Postfix). Threading for this "
        + "processor is managed by the underlying smtp server used so the processor need not support "
        + "more than one thread.")
@WritesAttributes({
    @WritesAttribute(attribute = "smtp.helo", description = "The value used during HELO"),
    @WritesAttribute(attribute = "smtp.certificates.*.serial", description = "The serial numbers for each of the "
            + "certificates used by an TLS peer"),
    @WritesAttribute(attribute = "smtp.certificates.*.principal", description = "The principal for each of the "
            + "certificates used by an TLS peer"),
    @WritesAttribute(attribute = "smtp.src", description = "The source IP and port of the SMTP connection"),
    @WritesAttribute(attribute = "smtp.from", description = "The value used during MAIL FROM (i.e. envelope)"),
    @WritesAttribute(attribute = "smtp.recipient.*", description = "The values used during RCPT TO (i.e. envelope)"),
    @WritesAttribute(attribute = "mime.type", description = "Mime type of the message")})
public class ListenSMTP extends AbstractSessionFactoryProcessor {

    static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP_PORT")
            .displayName("Listening Port")
            .description("The TCP port the ListenSMTP processor will bind to."
                    + "NOTE that on Unix derivative operating  systems this port must "
                    + "be higher than 1024 unless NiFi is running as with root user permissions.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_MAXIMUM_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_CONNECTIONS")
            .displayName("Maximum number of SMTP connection")
            .description("The maximum number of simultaneous SMTP connections.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("SMTP_TIMEOUT")
            .displayName("SMTP connection timeout")
            .description("The maximum time to wait for an action of SMTP client.")
            .defaultValue("60 seconds")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_MAXIMUM_MSG_SIZE = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_MSG_SIZE")
            .displayName("SMTP Maximum Message Size")
            .description("The maximum number of bytes the server will accept.")
            .required(true)
            .defaultValue("20 MB")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Integer.MAX_VALUE))
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL_CONTEXT_SERVICE")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, "
                    + "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("CLIENT_AUTH")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.NONE.toString(), SSLContextService.ClientAuth.REQUIRED.toString())
            .build();

    protected static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP_HOSTNAME")
            .displayName("SMTP hostname")
            .description("The hostname to be embedded into the banner displayed when an "
                    + "SMTP client connects to the processor TCP port .")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All new messages will be routed as FlowFiles to this relationship")
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    private final static Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SMTP_PORT);
        _propertyDescriptors.add(SMTP_MAXIMUM_CONNECTIONS);
        _propertyDescriptors.add(SMTP_TIMEOUT);
        _propertyDescriptors.add(SMTP_MAXIMUM_MSG_SIZE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(SMTP_HOSTNAME);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(_relationships);
    }

    private volatile SMTPServer smtp;

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (smtp == null) {
            try {
                final SMTPServer server = prepareServer(context, sessionFactory);
                server.start();
                getLogger().debug("Started SMTP Server on port " + server.getPort());
                smtp = server;
            } catch (final Exception ex) {//have to catch exception due to awkward exception handling in subethasmtp
                smtp = null;
                getLogger().error("Unable to start SMTP server due to " + ex.getMessage(), ex);
            }
        }
        context.yield();//nothing really to do here since threading managed by smtp server sessions
    }

    @OnStopped
    public void stop() {
        try {
            smtp.stop();
            getLogger().debug("Stopped SMTP server on port " + smtp.getPort());
        }catch (Exception ex){
            getLogger().error("Error stopping SMTP server: " + ex.getMessage());
        }finally {
            smtp = null;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();

        String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && !StringUtils.hasText(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .subject(CLIENT_AUTH.getDisplayName())
                    .explanation(CLIENT_AUTH.getDisplayName() + " must be provided when using " + SSL_CONTEXT_SERVICE.getDisplayName())
                    .valid(false)
                    .build());
        } else if (sslContextService == null && StringUtils.hasText(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .explanation(SSL_CONTEXT_SERVICE.getDisplayName() + " must be provided when selecting " + CLIENT_AUTH.getDisplayName())
                    .valid(false)
                    .build());
        }
        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private SMTPServer prepareServer(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        final int port = context.getProperty(SMTP_PORT).asInteger();
        final String host = context.getProperty(SMTP_HOSTNAME).getValue();
        final ComponentLog log = getLogger();
        final int maxMessageSize = context.getProperty(SMTP_MAXIMUM_MSG_SIZE).asDataSize(DataUnit.B).intValue();
        //create message handler factory
        final MessageHandlerFactory messageHandlerFactory = (final MessageContext mc) -> {
            return new SmtpConsumer(mc, sessionFactory, port, host, log, maxMessageSize);
        };
        //create smtp server
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SMTPServer smtpServer = sslContextService == null ? new SMTPServer(messageHandlerFactory) : new SMTPServer(messageHandlerFactory) {
            @Override
            public SSLSocket createSSLSocket(Socket socket) throws IOException {
                InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                String clientAuth = context.getProperty(CLIENT_AUTH).getValue();
                SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuth));
                SSLSocketFactory socketFactory = sslContext.getSocketFactory();
                SSLSocket sslSocket = (SSLSocket) (socketFactory.createSocket(socket, remoteAddress.getHostName(), socket.getPort(), true));
                sslSocket.setUseClientMode(false);

                if (SSLContextService.ClientAuth.REQUIRED.toString().equals(clientAuth)) {
                    this.setRequireTLS(true);
                    sslSocket.setNeedClientAuth(true);
                }
                return sslSocket;
            }
        };
        if (sslContextService != null) {
            smtpServer.setEnableTLS(true);
        } else {
            smtpServer.setHideTLS(true);
        }
        smtpServer.setSoftwareName("Apache NiFi SMTP");
        smtpServer.setPort(port);
        smtpServer.setMaxConnections(context.getProperty(SMTP_MAXIMUM_CONNECTIONS).asInteger());
        smtpServer.setMaxMessageSize(maxMessageSize);
        smtpServer.setConnectionTimeout(context.getProperty(SMTP_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        if (context.getProperty(SMTP_HOSTNAME).isSet()) {
            smtpServer.setHostName(context.getProperty(SMTP_HOSTNAME).getValue());
        }
        return smtpServer;
    }
}
