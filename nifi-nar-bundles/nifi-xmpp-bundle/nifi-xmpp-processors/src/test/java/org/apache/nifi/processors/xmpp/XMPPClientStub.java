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

import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stream.model.StreamElement;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class XMPPClientStub implements XMPPClient {
    @Override
    public void connect() throws XmppException {

    }

    @Override
    public void login(String user, String password, String resource) throws XmppException {

    }

    @Override
    public void close() throws XmppException {

    }

    @Override
    public Future<Void> send(StreamElement element) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {

    }

    @Override
    public ChatRoom createChatRoom(Jid chatService, String roomName) {
        return new ChatRoomStub();
    }
}
