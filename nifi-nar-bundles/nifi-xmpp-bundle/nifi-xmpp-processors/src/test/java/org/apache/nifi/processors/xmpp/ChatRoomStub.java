package org.apache.nifi.processors.xmpp;

import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class ChatRoomStub implements ChatRoom {
    @Override
    public Future<Presence> enter(String nick, DiscussionHistory history) {
        return CompletableFuture.completedFuture(new Presence());
    }

    @Override
    public Future<Void> exit() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> sendMessage(String message) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {

    }
}
