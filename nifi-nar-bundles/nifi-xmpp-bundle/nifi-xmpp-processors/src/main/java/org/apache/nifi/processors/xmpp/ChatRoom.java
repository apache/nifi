package org.apache.nifi.processors.xmpp;

import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface ChatRoom {
    Future<Presence> enter(String nick, DiscussionHistory history);
    Future<Void> exit();
    Future<Void> sendMessage(String message);
    void addInboundMessageListener(Consumer<MessageEvent> messageListener);
}
