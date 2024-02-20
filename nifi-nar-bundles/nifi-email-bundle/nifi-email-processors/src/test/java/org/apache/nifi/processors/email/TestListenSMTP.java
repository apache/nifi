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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import javax.net.ssl.SSLContext;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
                testTlsConfiguration.getTruststoreType()
        );

        final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
        sslContextService = mock(RestrictedSSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(SSL_SERVICE_IDENTIFIER);
        when(sslContextService.createContext()).thenReturn(sslContext);


        when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);
    }

    @Test
    public void testListenSMTP() throws MessagingException, InterruptedException {
        final TestRunner runner = newTestRunner();

        runner.run(1, false);
        final int port = ((ListenSMTP) runner.getProcessor()).getListeningPort();
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
        final TestRunner runner = newTestRunner();

        configureSslContextService(runner);
        runner.setProperty(ListenSMTP.SSL_CONTEXT_SERVICE, SSL_SERVICE_IDENTIFIER);
        runner.setProperty(ListenSMTP.CLIENT_AUTH, ClientAuth.NONE.name());
        runner.assertValid();

        runner.run(1, false);

        final int port = ((ListenSMTP) runner.getProcessor()).getListeningPort();
        assertPortListening(port);
        final Session session = getSessionTls(port, tlsConfiguration.getProtocol());

        for (int i = 0; i < MESSAGES; i++) {
            sendMessage(session, i);
        }

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, MESSAGES);
    }

    @Test
    public void testListenSMTPwithTooLargeMessage() throws InterruptedException {
        final TestRunner runner = newTestRunner();

        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_MSG_SIZE, "10 B");

        runner.run(1, false);

        final int port = ((ListenSMTP) runner.getProcessor()).getListeningPort();
        assertPortListening(port);

        final Session session = getSession(port);
        assertThrows(MessagingException.class, () -> sendMessage(session, 0));

        runner.shutdown();
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, 0);
    }

    private TestRunner newTestRunner() {
        final ListenSMTP processor = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(ListenSMTP.SMTP_PORT, "0");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        return runner;
    }

    private void assertPortListening(final int port) throws InterruptedException {
        final long endTime = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() <= endTime) {
            try (final Socket socket = new Socket("localhost", port)) {
                if (socket.isConnected()) {
                    return;
                }
            } catch (final Exception e) {
                Thread.sleep(10L);
            }
        }

        Assertions.fail(String.format("expected server listening on %s:%d", "localhost", port));
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
