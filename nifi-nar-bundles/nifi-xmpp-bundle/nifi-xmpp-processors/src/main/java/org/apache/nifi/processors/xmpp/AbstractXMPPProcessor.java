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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.net.ChannelEncryption;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.session.XmppClient;
import rocks.xmpp.extensions.muc.ChatRoom;
import rocks.xmpp.extensions.muc.ChatService;
import rocks.xmpp.extensions.muc.MultiUserChatManager;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public abstract class AbstractXMPPProcessor extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("hostname")
            .displayName("Hostname")
            .description("The IP address or hostname of the XMPP server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("The port on the XMPP server")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor XMPP_DOMAIN = new PropertyDescriptor.Builder()
            .name("xmpp-domain")
            .displayName("XMPP Domain")
            .description("The XMPP domain")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("The name of the user to log in to the XMPP server as")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("Password to use to log in to the XMPP server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor RESOURCE = new PropertyDescriptor.Builder()
            .name("resource")
            .displayName("Resource")
            .description("The name of the XMPP resource on the XMPP server")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHAT_ROOM = new PropertyDescriptor.Builder()
            .name("chat-room")
            .displayName("Chat Room")
            .description("The name of the chat room in which to send the XMPP message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections; channel encryption will only be used if this property is set")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected XmppClient xmppClient;
    protected ChatRoom chatRoom;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        createClient(context);
        connectClient();
        loginClient(context);
        enterChatRoomIfProvided(context);
    }

    @OnStopped
    public void close() {
        exitChatRoom();
        closeClient();
    }

    protected List<PropertyDescriptor> getBasePropertyDescriptors() {
        return createPropertyDescriptors(
                HOSTNAME,
                PORT,
                XMPP_DOMAIN,
                USERNAME,
                PASSWORD,
                RESOURCE,
                CHAT_ROOM,
                SSL_CONTEXT_SERVICE
        );
    }

    protected List<PropertyDescriptor> createPropertyDescriptors(PropertyDescriptor... descriptors) {
        return Arrays.asList(descriptors.clone());
    }

    protected List<PropertyDescriptor> basePropertyDescriptorsPlus(PropertyDescriptor... additionalDescriptors) {
        final List<PropertyDescriptor> descriptors = getBasePropertyDescriptors();
        descriptors.addAll(Arrays.asList(additionalDescriptors));
        return descriptors;
    }

    protected Set<Relationship> createRelationships(Relationship... relationships) {
        return Collections.unmodifiableSet(Arrays.stream(relationships).collect(Collectors.toSet()));
    }

    private void createClient(ProcessContext context) {
        xmppClient = XmppClient.create(
                context.getProperty(XMPP_DOMAIN).getValue(),
                createSocketConnectionConfiguration(context));
    }

    private void connectClient() {
        try {
            xmppClient.connect();
        } catch (XmppException e) {
            handleConnectionFailure(e);
        }
    }

    private void loginClient(ProcessContext context) {
        try {
            xmppClient.login(
                    context.getProperty(USERNAME).getValue(),
                    context.getProperty(PASSWORD).getValue(),
                    context.getProperty(RESOURCE).getValue());
        } catch (XmppException e) {
            handleLoginFailure(e);
        }
    }

    private void enterChatRoomIfProvided(ProcessContext context) {
        final PropertyValue chatRoomProperty = context.getProperty(CHAT_ROOM);
        if (chatRoomProperty.isSet()) {
            enterChatRoom(context, chatRoomProperty.getValue());
        }
    }

    private void exitChatRoom() {
        if (chatRoom != null) {
            try {
                chatRoom.exit().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                getLogger().error("Failed to exit the chat room", e);
            } finally {
                chatRoom = null;
            }
        }
    }

    private void closeClient() {
        if (xmppClient != null) {
            try {
                xmppClient.close();
            } catch (XmppException e) {
                getLogger().error("Failed to close the XMPP client", e);
            } finally {
                xmppClient = null;
            }
        }
    }

    private SocketConnectionConfiguration createSocketConnectionConfiguration(ProcessContext context) {
        return SocketConnectionConfiguration.builder()
                .hostname(context.getProperty(HOSTNAME).getValue())
                .port(context.getProperty(PORT).asInteger())
                .channelEncryption(channelEncryptionFor(context))
                .build();
    }

    private void handleConnectionFailure(XmppException e) {
        getLogger().error("Failed to connect to the XMPP server", e);
        throw new RuntimeException(e);
    }

    private void handleLoginFailure(XmppException e) {
        getLogger().error("Failed to login to the XMPP server", e);
        throw new RuntimeException(e);
    }

    private void enterChatRoom(ProcessContext context, String chatRoomName) {
        obtainChatRoom(context, chatRoomName);
        enterChatRoom(context);
    }

    private ChannelEncryption channelEncryptionFor(ProcessContext context) {
        return context.getProperty(SSL_CONTEXT_SERVICE).isSet()
                ? ChannelEncryption.REQUIRED
                : ChannelEncryption.DISABLED;
    }

    private void obtainChatRoom(ProcessContext context, String chatRoomName) {
        chatRoom = createChatService(context).createRoom(chatRoomName);
    }

    private void enterChatRoom(ProcessContext context) {
        chatRoom.enter(context.getProperty(USERNAME).getValue(), DiscussionHistory.none());
    }

    private ChatService createChatService(ProcessContext context) {
        final MultiUserChatManager mucManager = xmppClient.getManager(MultiUserChatManager.class);
        return mucManager.createChatService(conferenceServer(context));
    }

    private Jid conferenceServer(ProcessContext context) {
        return Jid.of("conference." + context.getProperty(XMPP_DOMAIN).getValue());
    }
}
