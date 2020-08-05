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

import java.util.ArrayList;
import java.util.List;

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
        final ChannelEncryption channelEncryption = context.getProperty(SSL_CONTEXT_SERVICE).isSet()
                ? ChannelEncryption.REQUIRED
                : ChannelEncryption.DISABLED;
        final SocketConnectionConfiguration socketConfiguration = SocketConnectionConfiguration.builder()
                .hostname(context.getProperty(HOSTNAME).getValue())
                .port(context.getProperty(PORT).asInteger())
                .channelEncryption(channelEncryption)
                .build();
        final String xmppDomain = context.getProperty(XMPP_DOMAIN).getValue();
        xmppClient = XmppClient.create(xmppDomain, socketConfiguration);
        try {
            xmppClient.connect();
        } catch (XmppException e) {
            getLogger().error("Failed to connect to the XMPP server", e);
            throw new RuntimeException(e);
        }
        final String username = context.getProperty(USERNAME).getValue();
        try {
            xmppClient.login(
                    username,
                    context.getProperty(PASSWORD).getValue(),
                    context.getProperty(RESOURCE).getValue());
        } catch (XmppException e) {
            getLogger().error("Failed to login to the XMPP server", e);
            throw new RuntimeException(e);
        }
        final PropertyValue chatRoomName = context.getProperty(CHAT_ROOM);
        if (chatRoomName.isSet()) {
            final MultiUserChatManager mucManager = xmppClient.getManager(MultiUserChatManager.class);
            final ChatService chatService = mucManager.createChatService(Jid.of("conference." + xmppDomain));
            chatRoom = chatService.createRoom(chatRoomName.getValue());
            chatRoom.enter(username, DiscussionHistory.none());
        }
    }

    @OnStopped
    public void close() {
        if (chatRoom != null) {
            chatRoom.exit();
            chatRoom = null;
        }
        if (xmppClient != null) {
            try {
                xmppClient.close();
            } catch (XmppException e) {
                getLogger().error("Failed to close the XMPP client", e);
            }
            xmppClient = null;
        }
    }

    protected List<PropertyDescriptor> getBasePropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(XMPP_DOMAIN);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(RESOURCE);
        descriptors.add(CHAT_ROOM);
        descriptors.add(SSL_CONTEXT_SERVICE);
        return descriptors;
    }
}
