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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.integration.mail.AbstractMailReceiver;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConsumeEmail {

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
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.IMAP.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.IMAP.getPort()));
        runner.setProperty(ConsumeIMAP.USER, "nifiUserImap");
        runner.setProperty(ConsumeIMAP.PASSWORD, "nifiPassword");
        runner.setProperty(ConsumeIMAP.FOLDER, "INBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        addMessage("testConsumeImap1", imapUser);
        addMessage("testConsumeImap2", imapUser);

        runner.run();

        runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        // Verify body
        assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result.contains("testConsumeImap1"));

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

        runner.run();

        runner.assertTransferCount(ConsumePOP3.REL_SUCCESS, 2);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumePOP3.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.getFirst()));

        // Verify body
        assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        assertTrue(result.contains("Pop1"));

    }

    @Test
    public void validateProtocol() {
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
}
