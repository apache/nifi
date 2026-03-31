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
import jakarta.mail.FolderClosedException;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.StoreClosedException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConsumeEmail {

    private static final String IMAP_USER = "nifiUserImap";
    private static final String IMAP_PASSWORD = "nifiPassword";
    @SuppressWarnings("unused")
    private static final String POP3_USER = "nifiUserPop";
    @SuppressWarnings("unused")
    private static final String POP3_PASSWORD = "nifiPassword";
    private static final String INBOX_FOLDER = "INBOX";
    private static final long RECONNECT_WAIT_BUFFER_MILLIS = 1500L;

    private GreenMail mockIMAP4Server;
    private GreenMail mockPOP3Server;
    private GreenMailUser imapUser;
    private GreenMailUser popUser;

    @BeforeEach
    public void setUp() {
        mockIMAP4Server = new GreenMail(ServerSetupTest.IMAP);
        mockIMAP4Server.start();
        mockPOP3Server = new GreenMail(ServerSetupTest.POP3);
        mockPOP3Server.start();

        imapUser = mockIMAP4Server.setUser("test@nifi.org", "nifiUserImap", "nifiPassword");
        popUser = mockPOP3Server.setUser("test@nifi.org", "nifiUserPop", "nifiPassword");
    }

    @AfterEach
    public void cleanUp() {
        mockIMAP4Server.stop();
        mockPOP3Server.stop();
    }

    public void addMessage(String testName, GreenMailUser user) throws MessagingException {
        Properties prop = new Properties();
        Session session = Session.getDefaultInstance(prop);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress("alice@nifi.org"));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress("test@nifi.org"));
        message.setSubject("Test email" + testName);
        message.setText("test test test chocolate");
        user.deliver(message);
    }

    @Test
    public void testConsumeIMAP4() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ConsumeIMAP());
        configureImapRunner(runner);

        addMessage("testConsumeImap1", imapUser);
        addMessage("testConsumeImap2", imapUser);

        runner.run(5);

        runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        // Verify body
        assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result.contains("testConsumeImap1"));

        // Verify second email message
        String result2 = new String(runner.getContentAsByteArray(messages.get(1)));

        // Verify body
        assertTrue(result2.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result2.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result2.contains("testConsumeImap2"));

    }

    @Test
    public void testConsumePOP3() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ConsumePOP3());
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.POP3.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.POP3.getPort()));
        runner.setProperty(ConsumeIMAP.USER, "nifiUserPop");
        runner.setProperty(ConsumeIMAP.PASSWORD, "nifiPassword");
        runner.setProperty(ConsumeIMAP.FOLDER, "INBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        addMessage("testConsumePop1", popUser);
        addMessage("testConsumePop2", popUser);

        // We can not run the processor more than twice due to fact that
        // GreenMail does not have a "SEEN" concept.
        runner.run(2);

        runner.assertTransferCount(ConsumePOP3.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumePOP3.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        // Verify body
        assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result.contains("testConsumePop1"));

        // Verify second email message
        String result2 = new String(runner.getContentAsByteArray(messages.get(1)));

        // Verify body
        assertTrue(result2.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result2.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result2.contains("testConsumePop2"));

    }

    @Test
    public void validateProtocol() {
        ConsumeIMAP consumeIMAP = new ConsumeIMAP();
        TestRunner runner = TestRunners.newTestRunner(consumeIMAP);
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        assertEquals("imap", consumeIMAP.getProtocol(runner.getProcessContext()));

        runner = TestRunners.newTestRunner(consumeIMAP);
        runner.setProperty(ConsumeIMAP.USE_SSL, "true");

        assertEquals("imaps", consumeIMAP.getProtocol(runner.getProcessContext()));

        ConsumePOP3 consumePOP3 = new ConsumePOP3();

        assertEquals("pop3", consumePOP3.getProtocol(runner.getProcessContext()));
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

    @Test
    public void testIsConnectionLostException() throws Exception {
        ConsumeIMAP consume = new ConsumeIMAP();
        TestRunner runner = TestRunners.newTestRunner(consume);
        configureImapRunner(runner);

        // Use reflection to access the private method
        Method isConnectionLostException = AbstractEmailProcessor.class.getDeclaredMethod(
                "isConnectionLostException", MessagingException.class);
        isConnectionLostException.setAccessible(true);

        // Test StoreClosedException - should return true
        StoreClosedException storeClosedException = new StoreClosedException(null, "Connection dropped by server");
        assertTrue((boolean) isConnectionLostException.invoke(consume, storeClosedException));

        // Test FolderClosedException - should return true
        FolderClosedException folderClosedException = new FolderClosedException(null, "Folder closed");
        assertTrue((boolean) isConnectionLostException.invoke(consume, folderClosedException));

        // Test MessagingException with connection-related message - should return true
        MessagingException connectionDropped = new MessagingException("Connection dropped by server");
        assertTrue((boolean) isConnectionLostException.invoke(consume, connectionDropped));

        // Test MessagingException with IOException cause - should return true
        MessagingException ioExceptionCause = new MessagingException("Error", new IOException("Connection reset"));
        assertTrue((boolean) isConnectionLostException.invoke(consume, ioExceptionCause));

        // Test regular MessagingException - should return false
        MessagingException regularException = new MessagingException("Some other error");
        assertFalse((boolean) isConnectionLostException.invoke(consume, regularException));

        // Test MessagingException with non-connection IOException cause - should return
        // false
        MessagingException nonConnectionIoException = new MessagingException("Error", new IOException("Some IO error"));
        assertFalse((boolean) isConnectionLostException.invoke(consume, nonConnectionIoException));
    }

    @Test
    public void testHandleConnectionErrorSetsReceiverToNull() throws Exception {
        // Arrange - Create and configure processor
        ConsumeIMAP consume = new ConsumeIMAP();
        TestRunner runner = TestRunners.newTestRunner(consume);
        configureImapRunner(runner);
        runner.setProperty(AbstractEmailProcessor.RECONNECT_WAIT_TIME, "1 sec");

        // Set up reflection access to private members
        java.lang.reflect.Field messageReceiverField = getAccessibleField("messageReceiver");
        Method handleConnectionError = getAccessibleMethod("handleConnectionError", Exception.class);

        // Act - Initialize processor by processing first message
        addMessage("testHandleConnectionError", imapUser);
        runner.run(1, false);

        // Assert - Verify initial state after initialization
        assertNotNull(messageReceiverField.get(consume), "messageReceiver should be initialized after first run");

        // Act - Simulate connection error
        StoreClosedException storeClosedException = new StoreClosedException(null, "Connection dropped by server");
        handleConnectionError.invoke(consume, storeClosedException);

        // Assert - Verify receiver is null after connection error
        assertNull(messageReceiverField.get(consume), "messageReceiver should be null after handleConnectionError");

        // Act - Wait for reconnection delay and trigger reinitialization
        Thread.sleep(RECONNECT_WAIT_BUFFER_MILLIS);
        addMessage("testHandleConnectionErrorReconnect", imapUser);
        runner.run(1, false);

        // Assert - Verify reconnection succeeded
        assertNotNull(messageReceiverField.get(consume), "messageReceiver should be reinitialized after reconnection");
        runner.assertAllFlowFilesTransferred(ConsumeIMAP.REL_SUCCESS, 2);
    }

    /**
     * Configures the test runner with standard IMAP properties for testing.
     *
     * @param runner the test runner to configure
     */
    private void configureImapRunner(TestRunner runner) {
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.IMAP.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.IMAP.getPort()));
        runner.setProperty(ConsumeIMAP.USER, IMAP_USER);
        runner.setProperty(ConsumeIMAP.PASSWORD, IMAP_PASSWORD);
        runner.setProperty(ConsumeIMAP.FOLDER, INBOX_FOLDER);
        runner.setProperty(ConsumeIMAP.USE_SSL, Boolean.FALSE.toString());
    }

    /**
     * Gets an accessible field from AbstractEmailProcessor using reflection.
     *
     * @param fieldName the name of the field to access
     * @return an accessible Field object
     * @throws NoSuchFieldException if the field does not exist
     */
    private java.lang.reflect.Field getAccessibleField(String fieldName) throws NoSuchFieldException {
        java.lang.reflect.Field field = AbstractEmailProcessor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    /**
     * Gets an accessible method from AbstractEmailProcessor using reflection.
     *
     * @param methodName     the name of the method to access
     * @param parameterTypes the parameter types of the method
     * @return an accessible Method object
     * @throws NoSuchMethodException if the method does not exist
     */
    private Method getAccessibleMethod(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
        Method method = AbstractEmailProcessor.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method;
    }
}
