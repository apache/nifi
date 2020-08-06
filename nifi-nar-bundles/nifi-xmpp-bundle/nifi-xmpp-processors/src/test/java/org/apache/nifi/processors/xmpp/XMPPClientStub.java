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
