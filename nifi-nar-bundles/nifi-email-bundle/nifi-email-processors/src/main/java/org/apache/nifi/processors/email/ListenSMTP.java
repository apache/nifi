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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;

import org.subethamail.smtp.server.SMTPServer;


import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.ssl.SSLContextService;

import org.apache.nifi.processors.email.smtp.event.SmtpEvent;
import org.apache.nifi.processors.email.smtp.handler.SMTPResultCode;
import org.apache.nifi.processors.email.smtp.handler.SMTPMessageHandlerFactory;

@Tags({"listen", "email", "smtp"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor implements a lightweight SMTP server to an arbitrary port, " +
        "allowing nifi to listen for incoming email. " +
        "" +
        "Note this server does not perform any email validation. If direct exposure to the internet is sought," +
        "it may be a better idea to use the combination of NiFi and an industrial scale MTA (e.g. Postfix)")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The value used during HELO"),
        @WritesAttribute(attribute = "smtp.helo", description = "The value used during HELO"),
        @WritesAttribute(attribute = "smtp.certificates.*.serial", description = "The serial numbers for each of the " +
                "certificates used by an TLS peer"),
        @WritesAttribute(attribute = "smtp.certificates.*.principal", description = "The principal for each of the " +
                "certificates used by an TLS peer"),
        @WritesAttribute(attribute = "smtp.from", description = "The value used during MAIL FROM (i.e. envelope)"),
        @WritesAttribute(attribute = "smtp.to", description = "The value used during RCPT TO (i.e. envelope)"),
        @WritesAttribute(attribute = "smtp.src", description = "The source IP of the SMTP connection")})

public class ListenSMTP extends AbstractProcessor {
    public static final String SMTP_HELO = "smtp.helo";
    public static final String SMTP_FROM = "smtp.from";
    public static final String SMTP_TO = "smtp.to";
    public static final String MIME_TYPE = "message/rfc822";
    public static final String SMTP_SRC_IP = "smtp.src";


    protected static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP_PORT")
            .displayName("Listening Port")
            .description("The TCP port the ListenSMTP processor will bind to." +
                    "NOTE that on Unix derivative operating  systems this port must " +
                    "be higher than 1024 unless NiFi is running as with root user permissions.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP_HOSTNAME")
            .displayName("SMTP hostname")
            .description("The hostname to be embedded into the banner displayed when an " +
                    "SMTP client connects to the processor TCP port .")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SMTP_MAXIMUM_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_CONNECTIONS")
            .displayName("Maximum number of SMTP connection")
            .description("The maximum number of simultaneous SMTP connections.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SMTP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("SMTP_TIMEOUT")
            .displayName("SMTP connection timeout")
            .description("The maximum time to wait for an action of SMTP client.")
            .defaultValue("60 seconds")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SMTP_MAXIMUM_MSG_SIZE = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_MSG_SIZE")
            .displayName("SMTP Maximum Message Size")
            .description("The maximum number of bytes the server will accept.")
            .required(true)
            .defaultValue("20MB")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SMTP_MAXIMUM_INCOMING_MESSAGE_QUEUE = new PropertyDescriptor.Builder()
            .name("SMTP_MAXIMUM_INCOMING_MESSAGE_QUEUE")
            .displayName("SMTP message buffer length")
            .description("This property control the size of the Queue utilised by the processor to hold messages as they are processed. " +
                    "Setting a very small value will decrease the number of emails the processor simultaneously, while setting an very large" +
                    "queue will result in higher memory and CPU utilisation. The default setting of 1024 is generally a fair number.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("1024")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL_CONTEXT_SERVICE")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("CLIENT_AUTH")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.NONE.toString(), SSLContextService.ClientAuth.REQUIRED.toString())
            .build();

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;

    }


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Extraction was successful")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> propertyDescriptors;
    private volatile LinkedBlockingQueue<SmtpEvent> incomingMessages;

    private volatile SMTPServer server;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean stopping = new AtomicBoolean(false);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SMTP_PORT);
        props.add(SMTP_HOSTNAME);
        props.add(SMTP_MAXIMUM_CONNECTIONS);
        props.add(SMTP_TIMEOUT);
        props.add(SMTP_MAXIMUM_MSG_SIZE);
        props.add(SMTP_MAXIMUM_INCOMING_MESSAGE_QUEUE);
        props.add(SSL_CONTEXT_SERVICE);
        props.add(CLIENT_AUTH);
        this.propertyDescriptors = Collections.unmodifiableList(props);

    }

    // Upon Schedule, reset the initialized state to false
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        initialized.set(false);
        stopping.set(false);
    }

    protected synchronized void initializeSMTPServer(final ProcessContext context) throws Exception {

        // check if we are already running or if it is stopping
        if (initialized.get() && server.isRunning() || stopping.get() ) {
            return;
        }

        incomingMessages = new LinkedBlockingQueue<>(context.getProperty(SMTP_MAXIMUM_INCOMING_MESSAGE_QUEUE).asInteger());

        String clientAuth = null;

        // If an SSLContextService was provided then create an SSLContext to pass down to the server
        SSLContext sslContext = null;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            clientAuth = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuth));
        }

        final SSLContext finalSslContext = sslContext;

        SMTPMessageHandlerFactory smtpMessageHandlerFactory = new SMTPMessageHandlerFactory(incomingMessages, getLogger());
        final SMTPServer server = new SMTPServer(smtpMessageHandlerFactory) {

            @Override
            public SSLSocket createSSLSocket(Socket socket) throws IOException {
                InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();

                SSLSocketFactory socketFactory = finalSslContext.getSocketFactory();

                SSLSocket s = (SSLSocket) (socketFactory.createSocket(socket, remoteAddress.getHostName(), socket.getPort(), true));

                s.setUseClientMode(false);


                // For some reason the createSSLContext above is not enough to enforce
                // client side auth
                // If client auth is required...
                if (SSLContextService.ClientAuth.REQUIRED.toString().equals(context.getProperty(CLIENT_AUTH).getValue())) {
                    s.setNeedClientAuth(true);
                }


                return s;
            }
        };

        // Set some parameters to our server
        server.setSoftwareName("Apache NiFi");


        // Set the Server options based on properties
        server.setPort(context.getProperty(SMTP_PORT).asInteger());
        server.setHostName(context.getProperty(SMTP_HOSTNAME).getValue());
        server.setMaxMessageSize(context.getProperty(SMTP_MAXIMUM_MSG_SIZE).asDataSize(DataUnit.B).intValue());
        server.setMaxConnections(context.getProperty(SMTP_MAXIMUM_CONNECTIONS).asInteger());
        server.setConnectionTimeout(context.getProperty(SMTP_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());


        // Check if TLS should be enabled
        if (sslContextService != null) {
            server.setEnableTLS(true);
        } else {
            server.setHideTLS(true);
        }

        // Set TLS to required in case CLIENT_AUTH = required
        if (SSLContextService.ClientAuth.REQUIRED.toString().equals(context.getProperty(CLIENT_AUTH).getValue())) {
            server.setRequireTLS(true);
        }

        this.server = server;
        server.start();

        getLogger().info("Server started and listening on port " + server.getPort());

        initialized.set(true);
        stopping.set(false);
    }

    @OnUnscheduled
    public void startShutdown() throws Exception {
        if (server != null) {
            stopping.set(true);
            getLogger().info("Shutting down processor P{}", new Object[]{server});
            server.stop();
            getLogger().info("Shut down {}", new Object[]{server});
        }
    }

    @OnStopped
    public void completeShutdown() throws Exception {
        if (server != null) {
            if (!server.isRunning() && stopping.get() ) {
                stopping.set(false);
            }
            getLogger().info("Completed shut down {}", new Object[]{server});
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            initializeSMTPServer(context);
        } catch (Exception e) {
            context.yield();
            throw new ProcessException("Failed to initialize the SMTP server", e);
        }

        while (!incomingMessages.isEmpty()) {
            SmtpEvent message = incomingMessages.poll();


            if (message == null) {
                return;
            }

            synchronized (message) {

                FlowFile flowfile = session.create();

                if (message.getMessageData() != null) {
                    ByteArrayOutputStream messageData = message.getMessageData();
                    flowfile = session.write(flowfile, new OutputStreamCallback() {

                        // Write the messageData to flowfile content
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(messageData.toByteArray());
                        }
                    });
                }

                HashMap<String, String> attributes = new HashMap<>();
                // Gather message attributes
                attributes.put(SMTP_HELO, message.getHelo());
                attributes.put(SMTP_SRC_IP, message.getHelo());
                attributes.put(SMTP_FROM, message.getFrom());
                attributes.put(SMTP_TO, message.getTo());

                List<Map<String, String>> details = message.getCertifcateDetails();
                int c = 0;

                // Add a selection of each X509 certificates to the already gathered attributes

                for (Map<String, String> detail : details) {
                    attributes.put("smtp.certificate." + c + ".serial", detail.getOrDefault("SerialNumber", null));
                    attributes.put("smtp.certificate." + c + ".subjectName", detail.getOrDefault("SubjectName", null));
                    c++;
                }

                // Set Mime-Type
                attributes.put(CoreAttributes.MIME_TYPE.key(), MIME_TYPE);

                // Add the attributes. to flowfile
                flowfile = session.putAllAttributes(flowfile, attributes);
                session.getProvenanceReporter().receive(flowfile, "smtp://" + SMTP_HOSTNAME + ":" + SMTP_PORT + "/");
                session.transfer(flowfile, REL_SUCCESS);
                getLogger().info("Transferring {} to success", new Object[]{flowfile});

                // Finished processing,
                message.setProcessed();

                // update the latch so data() can process the rest of the method
                message.updateProcessedLatch();

                // End of synchronized block
            }

            // Wait for SMTPMessageHandler data() and done() to complete
            // their side of the work (i.e. acknowledgement)
            while (!message.getAcknowledged()) {
                // Busy wait
                }

            // Lock one last time
            synchronized (message) {
                SMTPResultCode resultCode = SMTPResultCode.fromCode(message.getReturnCode());
                switch (resultCode) {
                    case UNEXPECTED_ERROR:
                    case TIMEOUT_ERROR:
                        session.rollback();
                        getLogger().warn(resultCode.getLogMessage());
                    case SUCCESS:
                        getLogger().info(resultCode.getLogMessage());
                        break;
                    default:
                        getLogger().error(resultCode.getLogMessage());
                }
            }
        }
    }

    // Same old... same old... used for testing to access the random port that was selected
    protected int getPort() {
        return server == null ? 0 : server.getPort();
    }


}
