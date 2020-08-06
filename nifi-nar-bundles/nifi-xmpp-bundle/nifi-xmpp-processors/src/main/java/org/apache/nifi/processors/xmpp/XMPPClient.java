package org.apache.nifi.processors.xmpp;

import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stream.model.StreamElement;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface XMPPClient {
    void connect() throws XmppException;
    void login(String user, String password, String resource) throws XmppException;
    void close() throws XmppException;
    Future<Void> send(StreamElement element);
    void addInboundMessageListener(Consumer<MessageEvent> messageListener);
    ChatRoom createChatRoom(Jid chatService, String roomName);
}
