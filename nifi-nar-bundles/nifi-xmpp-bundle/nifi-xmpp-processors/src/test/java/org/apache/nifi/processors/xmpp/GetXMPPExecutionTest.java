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
package org.apache.nifi.processors.xmpp;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Message;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class GetXMPPExecutionTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TestableGetXMPPProcessor.class);
        testRunner.setProperty(GetXMPP.HOSTNAME, "localhost");
        testRunner.setProperty(GetXMPP.PORT, "5222");
        testRunner.setProperty(GetXMPP.XMPP_DOMAIN, "domain");
        testRunner.setProperty(GetXMPP.USERNAME, "user");
        testRunner.setProperty(GetXMPP.PASSWORD, "password");
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void whenNoXmppMessagesReceived_noFlowFileSent() {
        testRunner.run();

        assertThat(getFlowFiles().size(), is(0));
    }

    @Test
    public void whenDirectMessageReceived_flowFileRoutedToSuccess() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessage();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetXMPP.SUCCESS);
    }

    @Test
    public void whenDirectMessageReceived_oneFlowFileIsCreated() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessage();

        testRunner.run();

        assertThat(getFlowFiles().size(), is(1));
    }

    @Test
    public void whenDirectMessageReceived_flowFileContentIsMessageBody() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessage("Direct message");

        testRunner.run();

        singleFlowFile().assertContentEquals("Direct message");
    }

    @Test
    public void whenMultipleDirectMessagesReceived_oneFlowFileIsCreatedPerMessage() {
        useDirectMessages();
        initialiseProcessor();
        IntStream.rangeClosed(1, 5).forEach(i -> sendDirectMessage());

        testRunner.run();

        assertThat(getFlowFiles().size(), is(5));
    }

    @Test
    public void whenMultipleDirectMessagesReceived_flowFileContentCorrespondsToMessageBodies() {
        useDirectMessages();
        initialiseProcessor();
        IntStream.rangeClosed(1, 3).forEach(i -> sendDirectMessage("message " + i));

        testRunner.run();

        IntStream.rangeClosed(1, 3).forEach(i -> verifyContentForFlowFile(i, "message " + i));
    }

    @Test
    public void whenChatRoomMessageReceived_flowFileRoutedToSuccess() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessage();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetXMPP.SUCCESS);
    }

    @Test
    public void whenChatRoomMessageReceived_oneFlowFileIsCreated() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessage();

        testRunner.run();

        assertThat(getFlowFiles().size(), is(1));
    }

    @Test
    public void whenChatRoomMessageReceived_flowFileContentIsMessageBody() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessage("Chat-room message");

        testRunner.run();

        singleFlowFile().assertContentEquals("Chat-room message");
    }

    @Test
    public void whenMultipleChatRoomMessagesReceived_oneFlowFileIsCreatedPerMessage() {
        useChatRoom();
        initialiseProcessor();
        IntStream.rangeClosed(1, 5).forEach(i -> sendChatRoomMessage());

        testRunner.run();

        assertThat(getFlowFiles().size(), is(5));
    }

    @Test
    public void whenMultipleChatRoomMessagesReceived_flowFileContentCorrespondsToMessageBodies() {
        useChatRoom();
        initialiseProcessor();
        IntStream.rangeClosed(1, 3).forEach(i -> sendChatRoomMessage("message " + i));

        testRunner.run();

        IntStream.rangeClosed(1, 3).forEach(i -> verifyContentForFlowFile(i, "message " + i));
    }

    private XMPPClientSpy getXmppClientSpy() {
        return ((TestableGetXMPPProcessor) testRunner.getProcessor()).xmppClientSpy;
    }

    private ChatRoomSpy getChatRoomSpy() {
        return getXmppClientSpy().chatRoomSpy;
    }

    private void useDirectMessages() {
        testRunner.removeProperty(PutXMPP.CHAT_ROOM);
    }

    private void useChatRoom() {
        testRunner.setProperty(PutXMPP.CHAT_ROOM, "chatRoom");
    }

    private void initialiseProcessor() {
        testRunner.run(1, false, true);
    }

    private void sendDirectMessage() {
        sendDirectMessage("message");
    }

    private void sendDirectMessage(String body) {
        getXmppClientSpy().sendDirectMessage(body);
    }

    private void sendChatRoomMessage() {
        sendChatRoomMessage("message");
    }

    private void sendChatRoomMessage(String body) {
        getChatRoomSpy().sendChatRoomMessage(body);
    }

    private MockFlowFile singleFlowFile() {
        return getFlowFiles().get(0);
    }

    private List<MockFlowFile> getFlowFiles() {
        return testRunner.getFlowFilesForRelationship(GetXMPP.SUCCESS);
    }

    private void verifyContentForFlowFile(int flowFileNumber, String flowFileContent) {
        getFlowFiles().get(flowFileNumber - 1).assertContentEquals(flowFileContent);
    }

    public static class TestableGetXMPPProcessor extends GetXMPP {
        public XMPPClientSpy xmppClientSpy;

        @Override
        protected XMPPClient createXmppClient(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
            xmppClientSpy = new XMPPClientSpy();
            return xmppClientSpy;
        }
    }

    private static class XMPPClientSpy extends XMPPClientStub {
        ChatRoomSpy chatRoomSpy;

        private Consumer<MessageEvent> messageListener;

        @Override
        public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public ChatRoom createChatRoom(Jid chatService, String roomName) {
            chatRoomSpy = new ChatRoomSpy();
            return chatRoomSpy;
        }

        void sendDirectMessage(String body) {
            if (this.messageListener != null) {
                final MessageEvent messageEvent =
                        new MessageEvent(new Object(), new Message(null, null, body), true);
                this.messageListener.accept(messageEvent);
            }
        }
    }

    private static class ChatRoomSpy extends ChatRoomStub {
        private Consumer<MessageEvent> messageListener;

        @Override
        public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
            this.messageListener = messageListener;
        }

        void sendChatRoomMessage(String body) {
            if (this.messageListener != null) {
                final MessageEvent messageEvent =
                        new MessageEvent(new Object(), new Message(null, null, body), true);
                this.messageListener.accept(messageEvent);
            }
        }
    }
}
