package org.apache.nifi.processors.xmpp;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Message;

import java.util.function.Consumer;

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

        assertThat(testRunner.getFlowFilesForRelationship(GetXMPP.SUCCESS).size(), is(0));
    }

    @Test
    public void whenDirectMessageReceived_flowFileRoutedToSuccess() {
        useDirectMessages();
        initialiseProcessor();
        sendDirectMessage("message");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetXMPP.SUCCESS);
    }

    @Test
    public void whenChatRoomMessageReceived_flowFileRoutedToSuccess() {
        useChatRoom();
        initialiseProcessor();
        sendChatRoomMessage("message");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetXMPP.SUCCESS);
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

    private void sendDirectMessage(String body) {
        getXmppClientSpy().sendDirectMessage(body);
    }

    private void sendChatRoomMessage(String body) {
        getChatRoomSpy().sendChatRoomMessage(body);
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
