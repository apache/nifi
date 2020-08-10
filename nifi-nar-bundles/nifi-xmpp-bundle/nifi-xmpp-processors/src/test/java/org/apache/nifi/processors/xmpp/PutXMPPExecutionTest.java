package org.apache.nifi.processors.xmpp;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.stanza.model.Message;
import rocks.xmpp.core.stream.model.StreamElement;

import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PutXMPPExecutionTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TestablePutXMPPProcessor.class);
        testRunner.setProperty(PutXMPP.HOSTNAME, "localhost");
        testRunner.setProperty(PutXMPP.PORT, "5222");
        testRunner.setProperty(PutXMPP.XMPP_DOMAIN, "domain");
        testRunner.setProperty(PutXMPP.USERNAME, "user");
        testRunner.setProperty(PutXMPP.PASSWORD, "password");
        testRunner.setProperty(PutXMPP.TARGET_USER, "target");
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void whenNoFlowFileReceived_noFlowFileSent() {
        testRunner.run();

        assertThat(testRunner.getFlowFilesForRelationship(PutXMPP.SUCCESS).size(), is(0));
    }

    @Test
    public void whenNoFlowFileReceived_noDirectMessagesSent() {
        useDirectMessages();

        testRunner.run();

        assertThat(getXmppClientSpy().sentMessage, nullValue());
    }

    @Test
    public void whenNoFlowFileReceived_noChatRoomMessagesSent() {
        useChatRoom();

        testRunner.run();

        assertThat(getChatRoomSpy().sentMessage, nullValue());
    }

    @Test
    public void whenAFlowFileIsReceived_itIsRoutedToSuccess() {
        enqueueFlowFile();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutXMPP.SUCCESS);
    }

    @Test
    public void whenAFlowFileIsReceived_andConfiguredForDirectMessages_aMessageIsSentWithTheCorrectDomain() {
        enqueueFlowFile();
        useDirectMessages();
        testRunner.setProperty(PutXMPP.XMPP_DOMAIN, "domain");

        testRunner.run();

        assertThat(getXmppClientSpy().sentMessage.getTo().getDomain(), is("domain"));
    }

    @Test
    public void whenAFlowFileIsReceived_andConfiguredForDirectMessages_aMessageIsSentWithTheCorrectLocalPart() {
        enqueueFlowFile();
        useDirectMessagesTo("targetUser");

        testRunner.run();

        assertThat(getXmppClientSpy().sentMessage.getTo().getLocal(), is("targetuser"));
    }

    @Test
    public void whenAFlowFileIsReceived_andConfiguredForDirectMessages_aMessageIsSentWithATypeOfChat() {
        enqueueFlowFile();
        useDirectMessages();

        testRunner.run();

        assertThat(getXmppClientSpy().sentMessage.getType(), is(Message.Type.CHAT));
    }

    @Test
    public void whenAFlowFileIsReceived_andConfiguredForDirectMessages_aMessageIsSentWithTheCorrectBody() {
        enqueueFlowFileWithContent("FlowFile content");
        useDirectMessages();

        testRunner.run();

        assertThat(getXmppClientSpy().sentMessage.getBody(), is("FlowFile content"));
    }

    @Test
    public void whenAFlowFileIsReceived_andConfiguredForAChatRoom_aMessageIsSentWithTheCorrectBody() {
        enqueueFlowFileWithContent("FlowFile content");
        useChatRoom();

        testRunner.run();

        assertThat(getChatRoomSpy().sentMessage, is("FlowFile content"));
    }

    private XMPPClientSpy getXmppClientSpy() {
        return ((TestablePutXMPPProcessor) testRunner.getProcessor()).xmppClientSpy;
    }

    private ChatRoomSpy getChatRoomSpy() {
        return getXmppClientSpy().chatRoomSpy;
    }

    private void enqueueFlowFile() {
        enqueueFlowFileWithContent("message");
    }

    private void enqueueFlowFileWithContent(String content) {
        testRunner.enqueue(content);
    }

    private void useDirectMessages() {
        useDirectMessagesTo("target");
    }

    private void useDirectMessagesTo(String targetUser) {
        testRunner.removeProperty(PutXMPP.CHAT_ROOM);
        testRunner.setProperty(PutXMPP.TARGET_USER, targetUser);
    }

    private void useChatRoom() {
        testRunner.removeProperty(PutXMPP.TARGET_USER);
        testRunner.setProperty(PutXMPP.CHAT_ROOM, "chatRoomName");
    }

    public static class TestablePutXMPPProcessor extends PutXMPP {
        public XMPPClientSpy xmppClientSpy;

        @Override
        protected XMPPClient createXmppClient(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
            xmppClientSpy = new XMPPClientSpy();
            return xmppClientSpy;
        }
    }

    private static class XMPPClientSpy extends XMPPClientStub {
        Message sentMessage;
        ChatRoomSpy chatRoomSpy;

        @Override
        public Future<Void> send(StreamElement element) {
            sentMessage = (Message) element;
            return super.send(element);
        }

        @Override
        public ChatRoom createChatRoom(Jid chatService, String roomName) {
            chatRoomSpy = new ChatRoomSpy();
            return chatRoomSpy;
        }
    }

    private static class ChatRoomSpy extends ChatRoomStub {
        String sentMessage;

        @Override
        public Future<Void> sendMessage(String message) {
            sentMessage = message;
            return super.sendMessage(message);
        }
    }
}
