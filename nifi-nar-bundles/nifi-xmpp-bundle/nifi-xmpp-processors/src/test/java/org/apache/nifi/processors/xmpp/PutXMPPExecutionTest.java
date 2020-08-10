package org.apache.nifi.processors.xmpp;

import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.stanza.model.Message;
import rocks.xmpp.core.stream.model.StreamElement;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
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
        XMPPClientSpy.sendFailure = false;
        ChatRoomSpy.sendFailure = false;
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

    @Test
    public void whenAFlowFileIsReceived_andSendingADirectMessageFails_theFlowFileIsRoutedToFailure() {
        enqueueFlowFile();
        useDirectMessages();
        XMPPClientSpy.sendFailure = true;

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutXMPP.FAILURE);
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingADirectMessageFails_anErrorIsLogged() {
        enqueueFlowFile();
        useDirectMessages();
        XMPPClientSpy.sendFailure = true;

        testRunner.run();

        assertThat(getLoggedErrors().size(), is(1));
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingADirectMessageFails_anErrorIsLoggedWithTheCorrectMessage() {
        enqueueFlowFile();
        useDirectMessages();
        XMPPClientSpy.sendFailure = true;

        testRunner.run();

        assertThat(getOnlyLoggedError().getMsg(), containsString("Failed to send XMPP message"));
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingADirectMessageFails_anErrorIsLoggedWithTheCorrectThrowable() {
        enqueueFlowFile();
        useDirectMessages();
        XMPPClientSpy.sendFailure = true;

        testRunner.run();

        verifyThrowable(getOnlyLoggedError().getThrowable().getCause(), RuntimeException.class, XMPPClientSpy.SEND_FAILED_MESSAGE);
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingAChatRoomMessageFails_theFlowFileIsRoutedToFailure() {
        enqueueFlowFile();
        useChatRoom();
        ChatRoomSpy.sendFailure = true;

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutXMPP.FAILURE);
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingAChatRoomMessageFails_anErrorIsLogged() {
        enqueueFlowFile();
        useChatRoom();
        ChatRoomSpy.sendFailure = true;

        testRunner.run();

        assertThat(getLoggedErrors().size(), is(1));
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingAChatRoomMessageFails_anErrorIsLoggedWithTheCorrectMessage() {
        enqueueFlowFile();
        useChatRoom();
        ChatRoomSpy.sendFailure = true;

        testRunner.run();

        assertThat(getOnlyLoggedError().getMsg(), containsString("Failed to send XMPP message"));
    }

    @Test
    public void whenAFlowFileIsReceived_andSendingAChatRoomMessageFails_anErrorIsLoggedWithTheCorrectThrowable() {
        enqueueFlowFile();
        useChatRoom();
        ChatRoomSpy.sendFailure = true;

        testRunner.run();

        verifyThrowable(getOnlyLoggedError().getThrowable().getCause(), RuntimeException.class, ChatRoomSpy.SEND_FAILED_MESSAGE);
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

    private List<LogMessage> getLoggedErrors() {
        return testRunner.getLogger().getErrorMessages();
    }

    private LogMessage getOnlyLoggedError() {
        return getLoggedErrors().get(0);
    }

    private void verifyThrowable(Throwable throwable, Class<?> exceptionClass, String message) {
        assertThat(throwable, instanceOf(exceptionClass));
        assertThat(throwable.getMessage(), is(message));
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
        static final String SEND_FAILED_MESSAGE = "Failed to send direct message";

        static boolean sendFailure = false;

        Message sentMessage;
        ChatRoomSpy chatRoomSpy;

        @Override
        public Future<Void> send(StreamElement element) {
            sentMessage = (Message) element;
            if (sendFailure) {
                return (Future<Void>) Executors.newSingleThreadExecutor().submit((Runnable) () -> {
                    throw new RuntimeException(SEND_FAILED_MESSAGE);
                });
            }
            return super.send(element);
        }

        @Override
        public ChatRoom createChatRoom(Jid chatService, String roomName) {
            chatRoomSpy = new ChatRoomSpy();
            return chatRoomSpy;
        }
    }

    private static class ChatRoomSpy extends ChatRoomStub {
        static final String SEND_FAILED_MESSAGE = "Failed to send chat-room message";

        static boolean sendFailure = false;

        String sentMessage;

        @Override
        public Future<Void> sendMessage(String message) {
            sentMessage = message;
            if (sendFailure) {
                return (Future<Void>) Executors.newSingleThreadExecutor().submit((Runnable) () -> {
                    throw new RuntimeException(SEND_FAILED_MESSAGE);
                });
            }
            return super.sendMessage(message);
        }
    }
}
