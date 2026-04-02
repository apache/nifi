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

package org.apache.nifi.processors.email;

import com.icegreen.greenmail.user.GreenMailUser;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.integration.mail.inbound.AbstractMailReceiver;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestConsumeEmail {

    private static final String SENDER_ADDRESS = "sender@nifi.apache.org";
    private static final String RECIPIENT_ADDRESS = "test@nifi.apache.org";
    private static final String RECIPIENT_USER = "recipient-user";
    private static final String RECIPIENT_PASSWORD = UUID.randomUUID().toString();
    private static final String INBOX_FOLDER = "INBOX";

    private GreenMail imapServer;
    private GreenMail popServer;
    private GreenMailUser imapUser;
    private GreenMailUser popUser;

    @BeforeEach
    void setUp() {
        imapServer = new GreenMail(ServerSetupTest.IMAP);
        imapServer.start();
        popServer = new GreenMail(ServerSetupTest.POP3);
        popServer.start();

        setUsers();
    }

    @AfterEach
    void cleanUp() {
        imapServer.stop();
        popServer.stop();
    }

    @Test
    void testConsumeIMAP4() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ConsumeIMAP());
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.IMAP.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.IMAP.getPort()));
        runner.setProperty(ConsumeIMAP.USER, RECIPIENT_USER);
        runner.setProperty(ConsumeIMAP.PASSWORD, RECIPIENT_PASSWORD);
        runner.setProperty(ConsumeIMAP.FOLDER, INBOX_FOLDER);
        runner.setProperty(ConsumeIMAP.USE_SSL, Boolean.FALSE.toString());

        addMessage("testConsumeImap1", imapUser);
        addMessage("testConsumeImap2", imapUser);

        runner.run();

        runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        assertTrue(result.contains("test test test chocolate"));
        assertTrue(result.contains(SENDER_ADDRESS));
        assertTrue(result.contains("testConsumeImap1"));
    }

    @Test
    void testConsumePOP3() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ConsumePOP3());
        runner.setProperty(ConsumePOP3.HOST, ServerSetupTest.POP3.getBindAddress());
        runner.setProperty(ConsumePOP3.PORT, String.valueOf(ServerSetupTest.POP3.getPort()));
        runner.setProperty(ConsumePOP3.USER, RECIPIENT_USER);
        runner.setProperty(ConsumePOP3.PASSWORD, RECIPIENT_PASSWORD);
        runner.setProperty(ConsumePOP3.FOLDER, INBOX_FOLDER);

        addMessage("testConsumePop1", popUser);
        addMessage("testConsumePop2", popUser);

        runner.run();

        runner.assertTransferCount(ConsumePOP3.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumePOP3.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        assertTrue(result.contains("test test test chocolate"));
        assertTrue(result.contains(SENDER_ADDRESS));
        assertTrue(result.contains("Pop1"));
    }

    @Test
    void testValidProtocols() {
        AbstractEmailProcessor<? extends AbstractMailReceiver> consume = new ConsumeIMAP();
        TestRunner runner = TestRunners.newTestRunner(consume);
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        assertEquals("imap", consume.getProtocol(runner.getProcessContext()));

        runner = TestRunners.newTestRunner(consume);
        runner.setProperty(ConsumeIMAP.USE_SSL, "true");

        assertEquals("imaps", consume.getProtocol(runner.getProcessContext()));

        consume = new ConsumePOP3();

        assertEquals("pop3", consume.getProtocol(runner.getProcessContext()));
    }

    @Test
    void testServerReconnected() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ConsumeIMAP());
        setImapServerProperties(runner);

        addMessage("testServerReconnected-1", imapUser);

        runner.run(1, false, true);
        runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 1);

        imapServer.stop();

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run(1, false, false));
        final Throwable cause = assertionError.getCause();
        assertInstanceOf(ProcessException.class, cause);
        final Throwable processExceptionCause = cause.getCause();
        assertInstanceOf(MessagingException.class, processExceptionCause);

        // Configure replacement server on different port for verified reconnection
        final GreenMail replacementServer = new GreenMail(ServerSetupTest.IMAP.port(0));
        try {
            replacementServer.start();
            final GreenMailUser replacementUser = replacementServer.setUser(RECIPIENT_ADDRESS, RECIPIENT_USER, RECIPIENT_PASSWORD);
            final int replacementPort = replacementServer.getImap().getPort();
            runner.setProperty(ConsumeIMAP.PORT, Integer.toString(replacementPort));

            addMessage("testServerReconnected-2", replacementUser);

            runner.clearTransferState();
            runner.run(1, false, false);
            runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 1);
        } finally {
            replacementServer.stop();
        }
    }

    @Test
    void testMigrateProperties() {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeIMAP.class);
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("host", AbstractEmailProcessor.HOST.getName()),
                Map.entry("port", AbstractEmailProcessor.PORT.getName()),
                Map.entry("authorization-mode", AbstractEmailProcessor.AUTHORIZATION_MODE.getName()),
                Map.entry("oauth2-access-token-provider", AbstractEmailProcessor.OAUTH2_ACCESS_TOKEN_PROVIDER.getName()),
                Map.entry("user", AbstractEmailProcessor.USER.getName()),
                Map.entry("password", AbstractEmailProcessor.PASSWORD.getName()),
                Map.entry("folder", AbstractEmailProcessor.FOLDER.getName()),
                Map.entry("fetch.size", AbstractEmailProcessor.FETCH_SIZE.getName()),
                Map.entry("delete.messages", AbstractEmailProcessor.SHOULD_DELETE_MESSAGES.getName()),
                Map.entry("connection.timeout", AbstractEmailProcessor.CONNECTION_TIMEOUT.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private void setImapServerProperties(final TestRunner runner) {
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.IMAP.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.IMAP.getPort()));
        runner.setProperty(ConsumeIMAP.USER, RECIPIENT_USER);
        runner.setProperty(ConsumeIMAP.PASSWORD, RECIPIENT_PASSWORD);
        runner.setProperty(ConsumeIMAP.FOLDER, INBOX_FOLDER);
        runner.setProperty(ConsumeIMAP.USE_SSL, Boolean.FALSE.toString());
    }

    private void setUsers() {
        imapUser = imapServer.setUser(RECIPIENT_ADDRESS, RECIPIENT_USER, RECIPIENT_PASSWORD);
        popUser = popServer.setUser(RECIPIENT_ADDRESS, RECIPIENT_USER, RECIPIENT_PASSWORD);
    }

    void addMessage(final String testName, final GreenMailUser user) throws MessagingException {
        Properties prop = new Properties();
        Session session = Session.getDefaultInstance(prop);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(SENDER_ADDRESS));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(RECIPIENT_ADDRESS));
        message.setSubject("Test email" + testName);
        message.setText("test test test chocolate");
        user.deliver(message);
    }
}
