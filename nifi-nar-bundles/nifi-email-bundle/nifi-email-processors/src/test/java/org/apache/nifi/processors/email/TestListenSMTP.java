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

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestListenSMTP {

    @Test
    public void testListenSMTP() throws Exception {
        final ListenSMTP processor = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");

        runner.run(1, false);

        assertTrue(String.format("expected server listening on %s:%d", "localhost", port), NetworkUtils.isListening("localhost", port, 5000));

        final Properties config = new Properties();
        config.put("mail.smtp.host", "localhost");
        config.put("mail.smtp.port", String.valueOf(port));
        config.put("mail.smtp.connectiontimeout", "5000");
        config.put("mail.smtp.timeout", "5000");
        config.put("mail.smtp.writetimeout", "5000");

        final Session session = Session.getInstance(config);
        session.setDebug(true);

        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            final Message email = new MimeMessage(session);
            email.setFrom(new InternetAddress("alice@nifi.apache.org"));
            email.setRecipients(Message.RecipientType.TO, InternetAddress.parse("bob@nifi.apache.org"));
            email.setSubject("This is a test");
            email.setText("MSG-" + i);
            Transport.send(email);
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, numMessages);
    }

    @Test
    public void testListenSMTPwithTLS() throws Exception {
        final ListenSMTP processor = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");

        // Setup the SSL Context
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        // and add the SSL context to the runner
        runner.setProperty(ListenSMTP.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(ListenSMTP.CLIENT_AUTH, SslContextFactory.ClientAuth.NONE.name());
        runner.assertValid();

        runner.run(1, false);

        assertTrue(String.format("expected server listening on %s:%d", "localhost", port), NetworkUtils.isListening("localhost", port, 5000));

        final Properties config = new Properties();
        config.put("mail.smtp.host", "localhost");
        config.put("mail.smtp.port", String.valueOf(port));
        config.put("mail.smtp.auth", "false");
        config.put("mail.smtp.starttls.enable", "true");
        config.put("mail.smtp.starttls.required", "true");
        config.put("mail.smtp.ssl.trust", "*");
        config.put("mail.smtp.connectiontimeout", "5000");
        config.put("mail.smtp.timeout", "5000");
        config.put("mail.smtp.writetimeout", "5000");

        final Session session = Session.getInstance(config);
        session.setDebug(true);

        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            final Message email = new MimeMessage(session);
            email.setFrom(new InternetAddress("alice@nifi.apache.org"));
            email.setRecipients(Message.RecipientType.TO, InternetAddress.parse("bob@nifi.apache.org"));
            email.setSubject("This is a test");
            email.setText("MSG-" + i);
            Transport.send(email);
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, numMessages);
    }

    @Test(expected = MessagingException.class)
    public void testListenSMTPwithTooLargeMessage() throws Exception {
        final ListenSMTP processor = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_MSG_SIZE, "10 B");

        runner.run(1, false);

        assertTrue(String.format("expected server listening on %s:%d", "localhost", port), NetworkUtils.isListening("localhost", port, 5000));

        final Properties config = new Properties();
        config.put("mail.smtp.host", "localhost");
        config.put("mail.smtp.port", String.valueOf(port));
        config.put("mail.smtp.connectiontimeout", "5000");
        config.put("mail.smtp.timeout", "5000");
        config.put("mail.smtp.writetimeout", "5000");

        final Session session = Session.getInstance(config);
        session.setDebug(true);

        MessagingException messagingException = null;
        try {
            final Message email = new MimeMessage(session);
            email.setFrom(new InternetAddress("alice@nifi.apache.org"));
            email.setRecipients(Message.RecipientType.TO, InternetAddress.parse("bob@nifi.apache.org"));
            email.setSubject("This is a test");
            email.setText("MSG-0");
            Transport.send(email);
        } catch (final MessagingException e) {
            messagingException = e;
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, 0);

        if (messagingException != null) throw messagingException;
    }

}
