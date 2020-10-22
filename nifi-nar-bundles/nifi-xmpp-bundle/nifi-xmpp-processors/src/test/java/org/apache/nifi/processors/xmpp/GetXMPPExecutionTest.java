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
import java.util.Locale;
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
    public void whenDirectMessageReceived_andTypeIsSet_typeIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessage();

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TYPE, "CHAT");
    }

    @Test
    public void whenDirectMessageReceived_andSubjectIsSet_subjectIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageWithSubject("subject");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.SUBJECT, "subject");
    }

    @Test
    public void whenDirectMessageReceived_andThreadIsSet_threadIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageWithThread("thread");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.THREAD, "thread");
    }

    @Test
    public void whenDirectMessageReceived_andParentThreadIsSet_parentThreadIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageWithParentThread("parent thread");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.PARENT_THREAD, "parent thread");
    }

    @Test
    public void whenDirectMessageReceived_andIdIsSet_idIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageWithId("ID");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.ID, "ID");
    }

    @Test
    public void whenDirectMessageReceived_andToIsSet_fullToJidIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO, "local@domain/resource");
    }

    @Test
    public void whenDirectMessageReceived_andToIsSet_bareToJidIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_BARE_JID, "local@domain");
    }

    @Test
    public void whenDirectMessageReceived_andToIsSet_toLocalIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_LOCAL, "local");
    }

    @Test
    public void whenDirectMessageReceived_andToIsSet_toDomainIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_DOMAIN, "domain");
    }

    @Test
    public void whenDirectMessageReceived_andToIsSet_toResourceIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_RESOURCE, "resource");
    }

    @Test
    public void whenDirectMessageReceived_andFromIsSet_fullFromJidIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM, "local@domain/resource");
    }

    @Test
    public void whenDirectMessageReceived_andFromIsSet_bareFromJidIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_BARE_JID, "local@domain");
    }

    @Test
    public void whenDirectMessageReceived_andFromIsSet_fromLocalIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_LOCAL, "local");
    }

    @Test
    public void whenDirectMessageReceived_andFromIsSet_fromDomainIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_DOMAIN, "domain");
    }

    @Test
    public void whenDirectMessageReceived_andFromIsSet_fromResourceIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_RESOURCE, "resource");
    }

    @Test
    public void whenDirectMessageReceived_andLanguageIsSet_languageIsProvidedAsAnAttribute() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessageWithLanguage(Locale.ENGLISH);

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.LANGUAGE, Locale.ENGLISH.toString());
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
    public void whenChatRoomMessageReceived_andTypeIsSet_typeIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessage();

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TYPE, "GROUPCHAT");
    }

    @Test
    public void whenChatRoomMessageReceived_andSubjectIsSet_subjectIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageWithSubject("subject");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.SUBJECT, "subject");
    }

    @Test
    public void whenChatRoomMessageReceived_andThreadIsSet_threadIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageWithThread("thread");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.THREAD, "thread");
    }

    @Test
    public void whenChatRoomMessageReceived_andParentThreadIsSet_parentThreadIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageWithParentThread("parent thread");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.PARENT_THREAD, "parent thread");
    }

    @Test
    public void whenChatRoomMessageReceived_andIdIsSet_idIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageWithId("ID");

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.ID, "ID");
    }

    @Test
    public void whenChatRoomMessageReceived_andToIsSet_fullToJidIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO, "local@domain/resource");
    }

    @Test
    public void whenChatRoomMessageReceived_andToIsSet_bareToJidIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_BARE_JID, "local@domain");
    }

    @Test
    public void whenChatRoomMessageReceived_andToIsSet_toLocalIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_LOCAL, "local");
    }

    @Test
    public void whenChatRoomMessageReceived_andToIsSet_toDomainIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_DOMAIN, "domain");
    }

    @Test
    public void whenChatRoomMessageReceived_andToIsSet_toResourceIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageTo(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.TO_RESOURCE, "resource");
    }

    @Test
    public void whenChatRoomMessageReceived_andFromIsSet_fullFromJidIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM, "local@domain/resource");
    }

    @Test
    public void whenChatRoomMessageReceived_andFromIsSet_bareFromJidIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_BARE_JID, "local@domain");
    }

    @Test
    public void whenChatRoomMessageReceived_andFromIsSet_fromLocalIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_LOCAL, "local");
    }

    @Test
    public void whenChatRoomMessageReceived_andFromIsSet_fromDomainIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_DOMAIN, "domain");
    }

    @Test
    public void whenChatRoomMessageReceived_andFromIsSet_fromResourceIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageFrom(Jid.of("local", "domain", "resource"));

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.FROM_RESOURCE, "resource");
    }

    @Test
    public void whenChatRoomMessageReceived_andLanguageIsSet_languageIsProvidedAsAnAttribute() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessageWithLanguage(Locale.ENGLISH);

        testRunner.run();

        singleFlowFile().assertAttributeEquals(GetXMPP.LANGUAGE, Locale.ENGLISH.toString());
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

    private void sendDirectMessage(Message message) {
        getXmppClientSpy().sendDirectMessage(message);
    }

    private void sendDirectMessageWithSubject(String subject) {
        sendDirectMessage(new Message(null, null, "Direct message with subject", subject));
    }

    private void sendDirectMessageWithThread(String thread) {
        sendDirectMessage(new Message(null, null, "Direct message with thread", null, thread));
    }

    private void sendDirectMessageWithParentThread(String parentThread) {
        sendDirectMessage(new Message(null, null, "Direct message with parent thread", null, null, parentThread, null, null, null, null, null));
    }

    private void sendDirectMessageWithId(String id) {
        sendDirectMessage(new Message(null, null, "Direct message with ID", null, null, null, id, null, null, null, null));
    }

    private void sendDirectMessageTo(Jid to) {
        sendDirectMessage(new Message(to, null, "Direct message with to"));
    }

    private void sendDirectMessageFrom(Jid from) {
        sendDirectMessage(new Message(null, null, "Direct message with from",null, null, null, null, from, null, null, null));
    }

    private void sendDirectMessageWithLanguage(Locale language) {
        sendDirectMessage(new Message(null, null, "Direct message with language",null, null, null, null, null, language, null, null));
    }

    private void sendChatRoomMessage() {
        sendChatRoomMessage("message");
    }

    private void sendChatRoomMessage(String body) {
        getChatRoomSpy().sendChatRoomMessage(body);
    }

    private void sendChatRoomMessage(Message message) {
        getChatRoomSpy().sendChatRoomMessage(message);
    }

    private void sendChatRoomMessageWithSubject(String subject) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with subject", subject));
    }

    private void sendChatRoomMessageWithThread(String thread) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with thread", null, thread));
    }

    private void sendChatRoomMessageWithParentThread(String parentThread) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with parent thread", null, null, parentThread, null, null, null, null, null));
    }

    private void sendChatRoomMessageWithId(String id) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with ID", null, null, null, id, null, null, null, null));
    }

    private void sendChatRoomMessageTo(Jid to) {
        sendChatRoomMessage(new Message(to, null, "Chat-room message with to"));
    }

    private void sendChatRoomMessageFrom(Jid from) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with from",null, null, null, null, from, null, null, null));
    }

    private void sendChatRoomMessageWithLanguage(Locale language) {
        sendChatRoomMessage(new Message(null, null, "Chat-room message with language",null, null, null, null, null, language, null, null));
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

        void sendDirectMessage(Message message) {
            if (this.messageListener != null) {
                final MessageEvent messageEvent =
                        new MessageEvent(new Object(), message, true);
                this.messageListener.accept(messageEvent);
            }
        }

        void sendDirectMessage(String body) {
            sendDirectMessage(new Message(null, Message.Type.CHAT, body));
        }
    }

    private static class ChatRoomSpy extends ChatRoomStub {
        private Consumer<MessageEvent> messageListener;

        @Override
        public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
            this.messageListener = messageListener;
        }

        void sendChatRoomMessage(Message message) {
            if (this.messageListener != null) {
                final MessageEvent messageEvent =
                        new MessageEvent(new Object(), message, true);
                this.messageListener.accept(messageEvent);
            }
        }

        void sendChatRoomMessage(String body) {
            sendChatRoomMessage(new Message(null, Message.Type.GROUPCHAT, body));
        }
    }
}
