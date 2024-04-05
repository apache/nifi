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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.email.smtp.SmtpConsumer;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandlerFactory;
import org.subethamail.smtp.server.SMTPServer;

import javax.net.ssl.SSLContext;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_MAXIMUM_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_CONNECTIONS")
            .displayName("Maximum number of SMTP connection")
            .description("The maximum number of simultaneous SMTP connections.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("SMTP_TIMEOUT")
            .displayName("SMTP connection timeout")
            .description("The maximum time to wait for an action of SMTP client.")
            .defaultValue("60 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor SMTP_MAXIMUM_MSG_SIZE = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_MSG_SIZE")
            .displayName("SMTP Maximum Message Size")
            .description("The maximum number of bytes the server will accept.")
            .required(true)
            .defaultValue("20 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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
            .required(true)
            .allowableValues(ClientAuth.NONE.name(), ClientAuth.REQUIRED.name())
            .dependsOn(SSL_CONTEXT_SERVICE)
            .build();

    protected static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP_HOSTNAME")
            .displayName("SMTP hostname")
            .description("The hostname to be embedded into the banner displayed when an "
                    + "SMTP client connects to the processor TCP port .")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All new messages will be routed as FlowFiles to this relationship")
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMTP_PORT,
            SMTP_MAXIMUM_CONNECTIONS,
            SMTP_TIMEOUT,
            SMTP_MAXIMUM_MSG_SIZE,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH,
            SMTP_HOSTNAME
    );

    private final static Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile SMTPServer smtp;

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (smtp == null) {
            try {
                final SMTPServer server = prepareServer(context, sessionFactory);
                server.start();
                getLogger().info("Started SMTP Server on {}", server.getPortAllocated());
                smtp = server;
            } catch (final Exception e) {//have to catch exception due to awkward exception handling in subethasmtp
                smtp = null;
                getLogger().error("Unable to start SMTP server", e);
            }
        }
        context.yield();//nothing really to do here since threading managed by smtp server sessions
    }

    public int getListeningPort() {
        return smtp == null ? 0 : smtp.getPortAllocated();
    }

    @OnStopped
    public void stop() {
        try {
            smtp.stop();
        } catch (final Exception e) {
            getLogger().error("Failed to stop SMTP Server", e);
        } finally {
            smtp = null;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private SMTPServer prepareServer(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        final SMTPServer.Builder smtpServerBuilder = new SMTPServer.Builder();

        final int port = context.getProperty(SMTP_PORT).asInteger();
        final String host = context.getProperty(SMTP_HOSTNAME).getValue();
        final ComponentLog log = getLogger();
        final int maxMessageSize = context.getProperty(SMTP_MAXIMUM_MSG_SIZE).asDataSize(DataUnit.B).intValue();
        final MessageHandlerFactory messageHandlerFactory = (final MessageContext mc) -> new SmtpConsumer(mc, sessionFactory, port, host, log, maxMessageSize);

        smtpServerBuilder.messageHandlerFactory(messageHandlerFactory);
        smtpServerBuilder.port(port);
        smtpServerBuilder.softwareName("Apache NiFi SMTP");
        smtpServerBuilder.maxConnections(context.getProperty(SMTP_MAXIMUM_CONNECTIONS).asInteger());
        smtpServerBuilder.maxMessageSize(maxMessageSize);
        smtpServerBuilder.connectionTimeout(context.getProperty(SMTP_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);
        if (context.getProperty(SMTP_HOSTNAME).isSet()) {
            smtpServerBuilder.hostName(context.getProperty(SMTP_HOSTNAME).getValue());
        }

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService == null) {
            smtpServerBuilder.hideTLS();
        } else {
            smtpServerBuilder.enableTLS();

            final String clientAuth = context.getProperty(CLIENT_AUTH).getValue();
            final boolean requireClientCertificate = ClientAuth.REQUIRED.getType().equalsIgnoreCase(clientAuth);

            final SSLContext sslContext = sslContextService.createContext();
            smtpServerBuilder.startTlsSocketFactory(sslContext, requireClientCertificate);
        }

        return smtpServerBuilder.build();
    }
}
