package org.apache.nifi.processors.xmpp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.net.ChannelEncryption;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractXMPPProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TestableAbstractXMPPProcessor.class);
        testRunner.setProperty(AbstractXMPPProcessor.HOSTNAME, "localhost");
        testRunner.setProperty(AbstractXMPPProcessor.PORT, "5222");
        testRunner.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");
        testRunner.setProperty(AbstractXMPPProcessor.USERNAME, "user");
        testRunner.setProperty(AbstractXMPPProcessor.PASSWORD, "password");
        testRunner.setValidateExpressionUsage(false);
        XMPPClientSpy.connectionError = false;
        XMPPClientSpy.loginError = false;
        XMPPClientSpy.closeError = false;
    }

    @Test
    public void createsAClientUsingTheCorrectHostname() {
        testRunner.setProperty(AbstractXMPPProcessor.HOSTNAME, "hostname");

        testRunner.run();

        assertThat(getXmppClientSpy().connectionConfiguration.getHostname(), is("hostname"));
    }

    @Test
    public void createsAClientUsingTheCorrectPort() {
        testRunner.setProperty(AbstractXMPPProcessor.PORT, "1234");

        testRunner.run();

        assertThat(getXmppClientSpy().connectionConfiguration.getPort(), is(1234));
    }

    @Test
    public void createsAClientUsingTheCorrectXmppDomain() {
        testRunner.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");

        testRunner.run();

        assertThat(getXmppClientSpy().xmppDomain, is("domain"));
    }

    @Test
    public void whenNoSslContextServiceIsProvided_doesNotUseChannelEncryption() {
        testRunner.removeProperty(AbstractXMPPProcessor.SSL_CONTEXT_SERVICE);

        testRunner.run();

        assertThat(getXmppClientSpy().connectionConfiguration.getChannelEncryption(), is(ChannelEncryption.DISABLED));
    }

    @Test
    public void connectsTheClient() {
        runTheProcessorWithoutStoppingIt();

        assertThat(getXmppClientSpy().isConnected(), is(true));
    }

    @Test
    public void logsInTheClient() {
        runTheProcessorWithoutStoppingIt();

        assertThat(getXmppClientSpy().isLoggedIn(), is(true));
    }

    @Test
    public void logsInUsingTheCorrectUsername() {
        testRunner.setProperty(AbstractXMPPProcessor.USERNAME, "username");

        testRunner.run();

        assertThat(getXmppClientSpy().loggedInUser, is("username"));
    }

    @Test
    public void logsInUsingTheCorrectPassword() {
        testRunner.setProperty(AbstractXMPPProcessor.PASSWORD, "password");

        testRunner.run();

        assertThat(getXmppClientSpy().providedPassword, is("password"));
    }

    @Test
    public void logsInUsingTheCorrectResource() {
        testRunner.setProperty(AbstractXMPPProcessor.RESOURCE, "resource");

        testRunner.run();

        assertThat(getXmppClientSpy().providedResource, is("resource"));
    }

    @Test
    public void whenNoChatRoomIsProvided_doesNotCreateAChatRoom() {
        testRunner.removeProperty(AbstractXMPPProcessor.CHAT_ROOM);

        testRunner.run();

        assertThat(getChatRoomSpy(), nullValue());
    }

    @Test
    public void whenChatRoomIsProvided_createsAChatRoom() {
        provideChatRoom();

        testRunner.run();

        assertThat(getChatRoomSpy(), notNullValue());
    }

    @Test
    public void whenChatRoomIsProvided_usesTheCorrectChatServiceForTheChatRoom() {
        provideChatRoom();
        testRunner.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");

        testRunner.run();

        assertThat(getChatRoomSpy().chatService.getDomain(), is("conference.domain"));
    }

    @Test
    public void whenChatRoomIsProvided_usesTheCorrectRoomNameForTheChatRoom() {
        provideChatRoomWithName("chatRoom");

        testRunner.run();

        assertThat(getChatRoomSpy().roomName, is("chatRoom"));
    }

    @Test
    public void whenChatRoomIsProvided_entersTheChatRoom() {
        provideChatRoom();

        runTheProcessorWithoutStoppingIt();

        assertThat(getChatRoomSpy().isInChatRoom(), is(true));
    }

    @Test
    public void whenChatRoomIsProvided_usesTheCorrectNicknameToEnterTheChatRoom() {
        provideChatRoom();
        testRunner.setProperty(AbstractXMPPProcessor.USERNAME, "username");

        testRunner.run();

        assertThat(getChatRoomSpy().providedNickname, is("username"));
    }

    @Test
    public void whenChatRoomIsProvided_usesTheCorrectDiscussionHistoryToEnterTheChatRoom() {
        provideChatRoom();

        testRunner.run();

        assertThat(getChatRoomSpy().requestedDiscussionHistory.toString(), is(DiscussionHistory.none().toString()));
    }

    @Test
    public void stoppingTheProcessor_closesTheClient() {
        runTheProcessorThenStopIt();

        assertThat(getXmppClientSpy().isConnected(), is(false));
    }

    @Test
    public void stoppingTheProcessor_whenClosingTheClientFails_stillContinues() {
        XMPPClientSpy.closeError = true;

        runTheProcessorThenStopIt();
    }

    @Test
    public void stoppingTheProcessor_whenClosingTheClientFails_logsAnError() {
        XMPPClientSpy.closeError = true;

        runTheProcessorThenStopIt();

        assertThat(getLoggedErrors().size(), is(1));
    }

    @Test
    public void stoppingTheProcessor_whenClosingTheClientFails_logsAnErrorWithTheCorrectMessage() {
        XMPPClientSpy.closeError = true;

        runTheProcessorThenStopIt();

        assertThat(getOnlyLoggedError().getMsg(), containsString("Failed to close the XMPP client"));
    }

    @Test
    public void stoppingTheProcessor_whenClosingTheClientFails_logsAnErrorWithTheCorrectThrowable() {
        XMPPClientSpy.closeError = true;

        runTheProcessorThenStopIt();

        verifyThrowable(getOnlyLoggedError().getThrowable(), XmppException.class, XMPPClientSpy.CLOSE_ERROR_MESSAGE);
    }

    @Test
    public void stoppingTheProcessor_exitsTheChatRoom() {
        provideChatRoom();

        runTheProcessorThenStopIt();

        assertThat(getChatRoomSpy().isInChatRoom(), is(false));
    }

    private XMPPClientSpy getXmppClientSpy() {
        return ((TestableAbstractXMPPProcessor) testRunner.getProcessor()).xmppClientSpy;
    }

    private ChatRoomSpy getChatRoomSpy() {
        return getXmppClientSpy().chatRoomSpy;
    }

    private List<LogMessage> getLoggedErrors() {
        return testRunner.getLogger().getErrorMessages();
    }

    private LogMessage getOnlyLoggedError() {
        return getLoggedErrors().get(0);
    }

    private void runTheProcessorWithoutStoppingIt() {
        testRunner.run(1, false);
    }

    private void runTheProcessorThenStopIt() {
        testRunner.run(1, true);
    }

    private void provideChatRoom() {
        provideChatRoomWithName("chatRoomName");
    }

    private void provideChatRoomWithName(String chatRoomName) {
        testRunner.setProperty(AbstractXMPPProcessor.CHAT_ROOM, chatRoomName);
    }

    private void verifyThrowable(Throwable throwable, Class<XmppException> exceptionClass, String message) {
        assertThat(throwable.getClass(), is(exceptionClass));
        assertThat(throwable.getMessage(), is(message));
    }

    public static class TestableAbstractXMPPProcessor extends AbstractXMPPProcessor {
        public XMPPClientSpy xmppClientSpy;

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.unmodifiableList(getBasePropertyDescriptors());
        }

        @Override
        public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException { }

        @Override
        protected XMPPClient createXmppClient(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
            xmppClientSpy = new XMPPClientSpy(xmppDomain, connectionConfiguration);
            return xmppClientSpy;
        }
    }

    private static class XMPPClientSpy extends XMPPClientStub {
        static final String CLOSE_ERROR_MESSAGE = "Could not close";

        static boolean connectionError = false;
        static boolean loginError = false;
        static boolean closeError = false;

        final String xmppDomain;
        final SocketConnectionConfiguration connectionConfiguration;

        String loggedInUser;
        String providedPassword;
        String providedResource;

        ChatRoomSpy chatRoomSpy;

        private boolean connected = false;
        private boolean loggedIn = false;

        XMPPClientSpy(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
            this.xmppDomain = xmppDomain;
            this.connectionConfiguration = connectionConfiguration;
        }

        @Override
        public void connect() throws XmppException {
            if (connectionError) {
                throw new XmppException("Could not connect");
            }
            connected = true;
        }

        @Override
        public void login(String user, String password, String resource) throws XmppException {
            if (!connected) {
                throw new IllegalStateException("Cannot log in when not connected");
            }
            loggedInUser = user;
            providedPassword = password;
            providedResource = resource;
            if (loginError) {
                throw new XmppException("Could not log in");
            }
            loggedIn = true;
        }

        @Override
        public void close() throws XmppException {
            if (closeError) {
                throw new XmppException(CLOSE_ERROR_MESSAGE);
            }
            loggedIn = false;
            connected = false;
        }

        @Override
        public ChatRoom createChatRoom(Jid chatService, String roomName) {
            chatRoomSpy = new ChatRoomSpy(chatService, roomName);
            return chatRoomSpy;
        }

        boolean isConnected() {
            return connected;
        }

        boolean isLoggedIn() {
            return loggedIn;
        }
    }

    private static class ChatRoomSpy extends ChatRoomStub {
        final Jid chatService;
        final String roomName;

        String providedNickname;
        DiscussionHistory requestedDiscussionHistory;

        private boolean inChatRoom = false;

        ChatRoomSpy(Jid chatService, String roomName) {
            this.chatService = chatService;
            this.roomName = roomName;
        }

        @Override
        public Future<Presence> enter(String nick, DiscussionHistory history) {
            providedNickname = nick;
            requestedDiscussionHistory = history;
            inChatRoom = true;
            return super.enter(nick, history);
        }

        @Override
        public Future<Void> exit() {
            inChatRoom = false;
            return super.exit();
        }

        boolean isInChatRoom() {
            return inChatRoom;
        }
    }
}
