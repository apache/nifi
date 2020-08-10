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

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
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

import javax.net.ssl.SSLContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class AbstractXMPPProcessorTest {

    private TestRunner testRunner;

    private SSLContextService sslContextService;
    private SSLContext sslContext;

    @Before
    public void init() throws Exception {
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
        ChatRoomSpy.exitError = false;
        sslContext = SSLContext.getDefault();
        sslContextService = new TestableStandardSSLContextService(sslContext);
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
    public void whenSslContextServiceIsProvided_usesChannelEncryption() throws Exception {
        configureSslContextService();

        testRunner.run();

        assertThat(getXmppClientSpy().connectionConfiguration.getChannelEncryption(), is(ChannelEncryption.REQUIRED));
    }

    @Test
    public void whenSslContextServiceIsProvided_setsTheSslContext() throws Exception {
        configureSslContextService();

        testRunner.run();

        assertThat(getXmppClientSpy().connectionConfiguration.getSSLContext(), is(sslContext));
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
    public void whenConnectionFails_anExceptionIsThrown() {
        XMPPClientSpy.connectionError = true;

        assertThrows(ProcessException.class, this::runOnScheduledMethod);
    }

    @Test
    public void whenConnectionFails_theExceptionHasTheCorrectMessage() {
        XMPPClientSpy.connectionError = true;

        final ProcessException exception = assertThrows(ProcessException.class, this::runOnScheduledMethod);

        assertThat(exception.getMessage(), is("Failed to connect to the XMPP server"));
    }

    @Test
    public void whenConnectionFails_theCauseOfTheExceptionIsCorrect() {
        XMPPClientSpy.connectionError = true;

        final ProcessException exception = assertThrows(ProcessException.class, this::runOnScheduledMethod);

        verifyThrowable(exception.getCause(), XmppException.class, XMPPClientSpy.CONNECT_ERROR_MESSAGE);
    }

    @Test
    public void logsInTheClient() {
        runTheProcessorWithoutStoppingIt();

        assertThat(getXmppClientSpy().isLoggedIn(), is(true));
    }

    @Test
    public void whenLoginFails_anExceptionIsThrown() {
        XMPPClientSpy.loginError = true;

        assertThrows(ProcessException.class, this::runOnScheduledMethod);
    }

    @Test
    public void whenLoginFails_theExceptionHasTheCorrectMessage() {
        XMPPClientSpy.loginError = true;

        final ProcessException exception = assertThrows(ProcessException.class, this::runOnScheduledMethod);

        assertThat(exception.getMessage(), is("Failed to login to the XMPP server"));
    }

    @Test
    public void whenLoginFails_theCauseOfTheExceptionIsCorrect() {
        XMPPClientSpy.loginError = true;

        final ProcessException exception = assertThrows(ProcessException.class, this::runOnScheduledMethod);

        verifyThrowable(exception.getCause(), XmppException.class, XMPPClientSpy.LOGIN_ERROR_MESSAGE);
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

    @Test
    public void stoppingTheProcessor_whenExitingTheChatRoomFails_stillContinues() {
        provideChatRoom();
        ChatRoomSpy.exitError = true;

        runTheProcessorThenStopIt();
    }

    @Test
    public void stoppingTheProcessor_whenExitingTheChatRoomFails_logsAnError() {
        provideChatRoom();
        ChatRoomSpy.exitError = true;

        runTheProcessorThenStopIt();

        assertThat(getLoggedErrors().size(), is(1));
    }

    @Test
    public void stoppingTheProcessor_whenExitingTheChatRoomFails_logsAnErrorWithTheCorrectMessage() {
        provideChatRoom();
        ChatRoomSpy.exitError = true;

        runTheProcessorThenStopIt();

        assertThat(getOnlyLoggedError().getMsg(), containsString("Failed to exit the chat room"));
    }

    @Test
    public void stoppingTheProcessor_whenExitingTheChatRoomFails_logsAnErrorWithTheCorrectThrowable() {
        provideChatRoom();
        ChatRoomSpy.exitError = true;

        runTheProcessorThenStopIt();

        verifyThrowable(getOnlyLoggedError().getThrowable().getCause(), RuntimeException.class, ChatRoomSpy.EXIT_ERROR_MESSAGE);
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

    private void verifyThrowable(Throwable throwable, Class<?> exceptionClass, String message) {
        assertThat(throwable, instanceOf(exceptionClass));
        assertThat(throwable.getMessage(), is(message));
    }

    private void configureSslContextService() throws InitializationException {
        final PropertyDescriptor sslContextServiceProperty = AbstractXMPPProcessor.SSL_CONTEXT_SERVICE;
        testRunner.addControllerService(sslContextServiceProperty.getName(), sslContextService);
        testRunner.enableControllerService(sslContextService);
        testRunner.setProperty(sslContextServiceProperty, sslContextServiceProperty.getName());
    }

    private void runOnScheduledMethod() {
        final TestableAbstractXMPPProcessor processor = new TestableAbstractXMPPProcessor();
        final MockProcessContext mockProcessContext = createMockProcessContext(processor);
        initialiseProcessor(processor, mockProcessContext);
        processor.onScheduled(mockProcessContext);
    }

    private void initialiseProcessor(TestableAbstractXMPPProcessor processor, MockProcessContext mockProcessContext) {
        final MockProcessorInitializationContext mockInitializationContext =
                new MockProcessorInitializationContext(processor, mockProcessContext);
        processor.initialize(mockInitializationContext);
    }

    private MockProcessContext createMockProcessContext(TestableAbstractXMPPProcessor processor) {
        final MockProcessContext mockProcessContext = new MockProcessContext(processor);
        mockProcessContext.setProperty(AbstractXMPPProcessor.HOSTNAME, "localhost");
        mockProcessContext.setProperty(AbstractXMPPProcessor.PORT, "5222");
        mockProcessContext.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");
        mockProcessContext.setProperty(AbstractXMPPProcessor.USERNAME, "user");
        mockProcessContext.setProperty(AbstractXMPPProcessor.PASSWORD, "password");
        return mockProcessContext;
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
        static final String CONNECT_ERROR_MESSAGE = "Could not connect";
        static final String LOGIN_ERROR_MESSAGE = "Could not login";
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
                throw new XmppException(CONNECT_ERROR_MESSAGE);
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
                throw new XmppException(LOGIN_ERROR_MESSAGE);
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
        static final String EXIT_ERROR_MESSAGE = "Could not exit chat room";

        static boolean exitError = false;

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
            if (exitError) {
                return (Future<Void>) Executors.newSingleThreadExecutor().submit((Runnable) () -> {
                    throw new RuntimeException(EXIT_ERROR_MESSAGE);
                });
            }
            inChatRoom = false;
            return super.exit();
        }

        boolean isInChatRoom() {
            return inChatRoom;
        }
    }

    private static class TestableStandardSSLContextService extends StandardSSLContextService {
        private final SSLContext sslContext;

        TestableStandardSSLContextService(SSLContext sslContext) {
            this.sslContext = sslContext;
        }

        @Override
        @OnEnabled
        public void onConfigured(final ConfigurationContext context) { }

        @Override
        protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
            return Collections.emptyList();
        }

        @Override
        public SSLContext createSSLContext(SSLContextService.ClientAuth clientAuth) {
            return sslContext;
        }
    }
}
