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

import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public class RocksChatRoom implements ChatRoom {

    private final rocks.xmpp.extensions.muc.ChatRoom chatRoom;

    public RocksChatRoom(rocks.xmpp.extensions.muc.ChatRoom chatRoom) {
        this.chatRoom = chatRoom;
    }

    @Override
    public Future<Presence> enter(String nick, DiscussionHistory history) {
        return chatRoom.enter(nick, history);
    }

    @Override
    public Future<Void> exit() {
        return chatRoom.exit();
    }

    @Override
    public Future<Void> sendMessage(String message) {
        return chatRoom.sendMessage(message);
    }

    @Override
    public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
        chatRoom.addInboundMessageListener(messageListener);
    }
}
