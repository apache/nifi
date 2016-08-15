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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.mail.Message;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.integration.mail.AbstractMailReceiver;
import org.springframework.integration.mail.ImapMailReceiver;

public class ConsumeEmailTest {

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

    @Test
    public void validateUrl() throws Exception {
        Field displayUrlField = AbstractEmailProcessor.class.getDeclaredField("displayUrl");
        displayUrlField.setAccessible(true);

        AbstractEmailProcessor<? extends AbstractMailReceiver> consume = new ConsumeIMAP();
        TestRunner runner = TestRunners.newTestRunner(consume);
        runner.setProperty(ConsumeIMAP.HOST, "foo.bar.com");
        runner.setProperty(ConsumeIMAP.PORT, "1234");
        runner.setProperty(ConsumeIMAP.USER, "jon");
        runner.setProperty(ConsumeIMAP.PASSWORD, "qhgwjgehr");
        runner.setProperty(ConsumeIMAP.FOLDER, "MYBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");

        assertEquals("imap://jon:qhgwjgehr@foo.bar.com:1234/MYBOX", consume.buildUrl(runner.getProcessContext()));
        assertEquals("imap://jon:[password]@foo.bar.com:1234/MYBOX", displayUrlField.get(consume));
    }

    @Test
    public void validateConsumeIMAP() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new TestImapProcessor(0));
        runner.setProperty(ConsumeIMAP.HOST, "foo.bar.com");
        runner.setProperty(ConsumeIMAP.PORT, "1234");
        runner.setProperty(ConsumeIMAP.USER, "jon");
        runner.setProperty(ConsumeIMAP.PASSWORD, "qhgwjgehr");
        runner.setProperty(ConsumeIMAP.FOLDER, "MYBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");
        runner.setProperty(ConsumeIMAP.SHOULD_DELETE_MESSAGES, "false");

        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        assertTrue(flowFiles.isEmpty());

        runner = TestRunners.newTestRunner(new TestImapProcessor(2));
        runner.setProperty(ConsumeIMAP.HOST, "foo.bar.com");
        runner.setProperty(ConsumeIMAP.PORT, "1234");
        runner.setProperty(ConsumeIMAP.USER, "jon");
        runner.setProperty(ConsumeIMAP.PASSWORD, "qhgwjgehr");
        runner.setProperty(ConsumeIMAP.FOLDER, "MYBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");
        runner.setProperty(ConsumeIMAP.SHOULD_DELETE_MESSAGES, "false");
        runner.setProperty(ConsumeIMAP.CONNECTION_TIMEOUT, "130 ms");

        runner.run(2);
        flowFiles = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        assertTrue(flowFiles.size() == 2);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("You've Got Mail - 0".getBytes(StandardCharsets.UTF_8));
        ff = flowFiles.get(1);
        ff.assertContentEquals("You've Got Mail - 1".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void validateConsumeIMAPWithTimeout() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new TestImapProcessor(1));
        runner.setProperty(ConsumeIMAP.HOST, "foo.bar.com");
        runner.setProperty(ConsumeIMAP.PORT, "1234");
        runner.setProperty(ConsumeIMAP.USER, "jon");
        runner.setProperty(ConsumeIMAP.PASSWORD, "qhgwjgehr");
        runner.setProperty(ConsumeIMAP.FOLDER, "MYBOX");
        runner.setProperty(ConsumeIMAP.USE_SSL, "false");
        runner.setProperty(ConsumeIMAP.SHOULD_DELETE_MESSAGES, "false");
        runner.setProperty(ConsumeIMAP.CONNECTION_TIMEOUT, "${random():mod(10):plus(1)} secs");

        runner.run(1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeIMAP.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("You've Got Mail - 0".getBytes(StandardCharsets.UTF_8));
    }

    public static class TestImapProcessor extends ConsumeIMAP {

        private final int messagesToGenerate;

        TestImapProcessor(int messagesToGenerate) {
            this.messagesToGenerate = messagesToGenerate;
        }

        @Override
        protected ImapMailReceiver buildMessageReceiver(ProcessContext processContext) {
            ImapMailReceiver receiver = mock(ImapMailReceiver.class);
            try {
                Message[] messages = new Message[this.messagesToGenerate];
                for (int i = 0; i < this.messagesToGenerate; i++) {
                    Message message = mock(Message.class);
                    when(message.getInputStream()).thenReturn(
                            new ByteArrayInputStream(("You've Got Mail - " + i).getBytes(StandardCharsets.UTF_8)));
                    messages[i] = message;
                }
                when(receiver.receive()).thenReturn(messages);
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
            return receiver;
        }
    }
}
