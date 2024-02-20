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
package org.apache.nifi.processors.email.smtp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processors.email.ListenSMTP;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandler;
import org.subethamail.smtp.TooMuchDataException;
import org.subethamail.smtp.server.SMTPServer;

import javax.security.auth.x500.X500Principal;

/**
 * A simple consumer that provides a bridge between 'push' message distribution
 * provided by {@link SMTPServer} and NiFi polling scheduler mechanism.
 */
public class SmtpConsumer implements MessageHandler {

    private String from = null;
    private final List<String> recipientList = new ArrayList<>();
    private final MessageContext context;
    private final ProcessSessionFactory sessionFactory;
    private final int port;
    private final int maxMessageSize;
    private final ComponentLog log;
    private final String host;

    public SmtpConsumer(
            final MessageContext context,
            final ProcessSessionFactory sessionFactory,
            final int port,
            final String host,
            final ComponentLog log,
            final int maxMessageSize
    ) {
        this.context = context;
        this.sessionFactory = sessionFactory;
        this.port = port;
        if (host == null || host.trim().isEmpty()) {
            this.host = context.getSMTPServer().getHostName();
        } else {
            this.host = host;
        }
        this.log = log;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public String data(final InputStream data) throws IOException {
        final ProcessSession processSession = sessionFactory.createSession();
        final StopWatch watch = new StopWatch();
        watch.start();
        try {
            FlowFile flowFile = processSession.create();
            final AtomicBoolean limitExceeded = new AtomicBoolean(false);
            flowFile = processSession.write(flowFile, (OutputStream out) -> {
                final LimitingInputStream lis = new LimitingInputStream(data, maxMessageSize);
                StreamUtils.copy(lis, out);
                if (lis.hasReachedLimit()) {
                    limitExceeded.set(true);
                }
            });
            if (limitExceeded.get()) {
                throw new TooMuchDataException("Maximum message size limit reached - client must send smaller messages");
            }
            flowFile = processSession.putAllAttributes(flowFile, extractMessageAttributes());
            watch.stop();
            processSession.getProvenanceReporter().receive(flowFile, "smtp://" + host + ":" + port + "/", watch.getDuration(TimeUnit.MILLISECONDS));
            processSession.transfer(flowFile, ListenSMTP.REL_SUCCESS);
            processSession.commitAsync();
        } catch (final FlowFileAccessException | IllegalStateException | IOException e) {
            log.error("SMTP data processing failed", e);
            throw e;
        }

        return null;
    }

    @Override
    public void from(final String from) {
        this.from = from;
    }

    @Override
    public void recipient(final String recipient) {
        if (recipient != null && recipient.length() < 100 && recipientList.size() < 100) {
            recipientList.add(recipient);
        }
    }

    @Override
    public void done() {
    }

    private Map<String, String> extractMessageAttributes() {
        final Map<String, String> attributes = new HashMap<>();
        final Certificate[] tlsPeerCertificates = context.getTlsPeerCertificates();
        if (tlsPeerCertificates != null) {
            for (int i = 0; i < tlsPeerCertificates.length; i++) {
                if (tlsPeerCertificates[i] instanceof final X509Certificate x509Cert) {
                    attributes.put("smtp.certificate." + i + ".serial", x509Cert.getSerialNumber().toString());
                    attributes.put("smtp.certificate." + i + ".subjectName", x509Cert.getSubjectX500Principal().getName(X500Principal.RFC1779));
                }
            }
        }

        SocketAddress address = context.getRemoteAddress();
        if (address != null) {
            // will extract and format source address if available
            String strAddress = address instanceof InetSocketAddress
                    ? ((InetSocketAddress) address).getHostString() + ":" + ((InetSocketAddress) address).getPort()
                    : context.getRemoteAddress().toString();
            attributes.put("smtp.src", strAddress);
        }

        final Optional<String> helo = context.getHelo();
        helo.ifPresent(s -> attributes.put("smtp.helo", s));

        attributes.put("smtp.from", from);
        for (int i = 0; i < recipientList.size(); i++) {
            attributes.put("smtp.recipient." + i, recipientList.get(i));
        }
        attributes.put(CoreAttributes.MIME_TYPE.key(), "message/rfc822");
        return attributes;
    }
}
