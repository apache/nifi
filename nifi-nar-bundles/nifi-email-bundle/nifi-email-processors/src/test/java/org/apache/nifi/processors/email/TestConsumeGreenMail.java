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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;


public class TestConsumeGreenMail {

    private GreenMail mockImapServer;
    private GreenMail mockPopServer;
    private GreenMailUser imapUser;
    private GreenMailUser popUser;

    // Setup mock imap server
    @Before
    public void setUp() {
        mockImapServer = new GreenMail(ServerSetupTest.IMAP);
        mockImapServer.start();
        mockPopServer = new GreenMail(ServerSetupTest.POP3);
        mockPopServer.start();
        imapUser = mockImapServer.setUser("test@nifi.org", "nifiUser", "nifiPassword");
        popUser = mockPopServer.setUser("test@nifi.org", "nifiUser", "nifiPassword");
    }

    @After
    public void cleanUp() {
        mockImapServer.stop();
        mockPopServer.stop();
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

    // Start the testing units
    @Test
    public void testConsumeImap() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new ConsumeIMAP());
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.IMAP.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.IMAP.getPort()));
        runner.setProperty(ConsumeIMAP.USER, "nifiUser");
        runner.setProperty(ConsumeIMAP.PASSWORD, "nifiPassword");
        runner.setProperty(ConsumeIMAP.FOLDER, "INBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        addMessage("testConsumeImap", imapUser);

        runner.run();

        runner.assertTransferCount(ConsumeIMAP.REL_SUCCESS, 1);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.get(0)));

        // Verify body
        Assert.assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        Assert.assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        Assert.assertTrue(result.contains("Imap"));

    }

    @Test
    public void testConsumePop() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new ConsumePOP3());
        runner.setProperty(ConsumeIMAP.HOST, ServerSetupTest.POP3.getBindAddress());
        runner.setProperty(ConsumeIMAP.PORT, String.valueOf(ServerSetupTest.POP3.getPort()));
        runner.setProperty(ConsumeIMAP.USER, "nifiUser");
        runner.setProperty(ConsumeIMAP.PASSWORD, "nifiPassword");
        runner.setProperty(ConsumeIMAP.FOLDER, "INBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        addMessage("testConsumePop", popUser);

        runner.run();

        runner.assertTransferCount(ConsumePOP3.REL_SUCCESS, 1);
        final List<MockFlowFile> messages = runner.getFlowFilesForRelationship(ConsumePOP3.REL_SUCCESS);
        String result = new String(runner.getContentAsByteArray(messages.get(0)));

        // Verify body
        Assert.assertTrue(result.contains("test test test chocolate"));

        // Verify sender
        Assert.assertTrue(result.contains("alice@nifi.org"));

        // Verify subject
        Assert.assertTrue(result.contains("Pop"));

    }

}
