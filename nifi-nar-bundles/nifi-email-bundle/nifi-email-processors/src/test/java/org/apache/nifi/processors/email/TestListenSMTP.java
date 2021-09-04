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

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestListenSMTP {
    private static final String SSL_SERVICE_IDENTIFIER = "ssl-context";

    private static TlsConfiguration tlsConfiguration;

    private static SSLContextService sslContextService;

    private static final int MESSAGES = 2;

    @BeforeAll
    public static void setTlsConfiguration() throws GeneralSecurityException {
        final TlsConfiguration testTlsConfiguration = new TemporaryKeyStoreBuilder().build();

        tlsConfiguration = new StandardTlsConfiguration(
                testTlsConfiguration.getKeystorePath(),
                testTlsConfiguration.getKeystorePassword(),
                testTlsConfiguration.getKeyPassword(),
                testTlsConfiguration.getKeystoreType(),
                testTlsConfiguration.getTruststorePath(),
                testTlsConfiguration.getTruststorePassword(),
                testTlsConfiguration.getTruststoreType(),
                TlsConfiguration.TLS_1_2_PROTOCOL
        );

        final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
        sslContextService = mock(RestrictedSSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(SSL_SERVICE_IDENTIFIER);
        when(sslContextService.createContext()).thenReturn(sslContext);


        when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);
    }

    @Test
    public void testListenSMTP() throws MessagingException {
        final int port = NetworkUtils.availablePort();
        final TestRunner runner = newTestRunner(port);

        runner.run(1, false);
        assertPortListening(port);

        final Session session = getSession(port);
        for (int i = 0; i < MESSAGES; i++) {
            sendMessage(session, i);
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, MESSAGES);
    }

    @Test
    public void testListenSMTPwithTLSCurrentVersion() throws Exception {
        final int port = NetworkUtils.availablePort();
        final TestRunner runner = newTestRunner(port);

        configureSslContextService(runner);
        runner.setProperty(ListenSMTP.SSL_CONTEXT_SERVICE, SSL_SERVICE_IDENTIFIER);
        runner.setProperty(ListenSMTP.CLIENT_AUTH, ClientAuth.NONE.name());
        runner.assertValid();

        runner.run(1, false);
        assertPortListening(port);
        final Session session = getSessionTls(port, tlsConfiguration.getProtocol());

        for (int i = 0; i < MESSAGES; i++) {
            sendMessage(session, i);
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, MESSAGES);
    }

    @Test
    public void testListenSMTPwithTLSLegacyProtocolException() throws Exception {
        final int port = NetworkUtils.availablePort();
        final TestRunner runner = newTestRunner(port);

        configureSslContextService(runner);
        runner.setProperty(ListenSMTP.SSL_CONTEXT_SERVICE, SSL_SERVICE_IDENTIFIER);
        runner.setProperty(ListenSMTP.CLIENT_AUTH, ClientAuth.NONE.name());
        runner.assertValid();

        runner.run(1, false);
        assertPortListening(port);

        final Session session = getSessionTls(port, TlsConfiguration.TLS_1_0_PROTOCOL);
        final MessagingException exception = assertThrows(MessagingException.class, () -> sendMessage(session, 0));
        assertEquals(exception.getMessage(), "Could not convert socket to TLS");

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, 0);
    }

    @Test
    public void testListenSMTPwithTooLargeMessage() {
        final int port = NetworkUtils.availablePort();
        final TestRunner runner = newTestRunner(port);
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_MSG_SIZE, "10 B");

        runner.run(1, false);
        assertPortListening(port);

        final Session session = getSession(port);
        assertThrows(MessagingException.class, () -> sendMessage(session, 0));

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, 0);
    }

    private TestRunner newTestRunner(final int port) {
        final ListenSMTP processor = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        return runner;
    }

    private void assertPortListening(final int port) {
        assertTrue(NetworkUtils.isListening("localhost", port, 5000), String.format("expected server listening on %s:%d", "localhost", port));
    }

    private Session getSession(final int port) {
        final Properties config = new Properties();
        config.put("mail.smtp.host", "localhost");
        config.put("mail.smtp.port", String.valueOf(port));
        config.put("mail.smtp.connectiontimeout", "5000");
        config.put("mail.smtp.timeout", "5000");
        config.put("mail.smtp.writetimeout", "5000");
        final Session session = Session.getInstance(config);
        session.setDebug(true);
        return session;
    }

    private Session getSessionTls(final int port, final String tlsProtocol) {
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
        config.put("mail.smtp.ssl.protocols", tlsProtocol);

        final Session session = Session.getInstance(config);
        session.setDebug(true);
        return session;
    }

    private void sendMessage(final Session session, final int i) throws MessagingException {
        final Message email = new MimeMessage(session);
        email.setFrom(new InternetAddress("alice@nifi.apache.org"));
        email.setRecipients(Message.RecipientType.TO, InternetAddress.parse("bob@nifi.apache.org"));
        email.setSubject("This is a test");
        email.setText("MSG-" + i);
        Transport.send(email);
    }

    private void configureSslContextService(final TestRunner runner) throws InitializationException {
        runner.addControllerService(SSL_SERVICE_IDENTIFIER, sslContextService);
        runner.enableControllerService(sslContextService);
    }
}
