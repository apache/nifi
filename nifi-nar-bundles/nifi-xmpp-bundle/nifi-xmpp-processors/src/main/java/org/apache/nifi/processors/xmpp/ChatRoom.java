package org.apache.nifi.processors.xmpp;

import rocks.xmpp.core.session.SendTask;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Message;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;
import rocks.xmpp.util.concurrent.AsyncResult;

import java.util.function.Consumer;

public interface ChatRoom {
    AsyncResult<Presence> enter(String nick, DiscussionHistory history);
    AsyncResult<Void> exit();
    SendTask<Message> sendMessage(String message);
    void addInboundMessageListener(Consumer<MessageEvent> messageListener);
}
