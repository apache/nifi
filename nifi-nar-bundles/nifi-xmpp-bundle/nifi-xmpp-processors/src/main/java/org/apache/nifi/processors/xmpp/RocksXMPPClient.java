package org.apache.nifi.processors.xmpp;

import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.session.XmppClient;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stream.model.StreamElement;
import rocks.xmpp.extensions.muc.MultiUserChatManager;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public class RocksXMPPClient implements XMPPClient {

    private final XmppClient xmppClient;

    public RocksXMPPClient(String xmppServiceDomain, SocketConnectionConfiguration connectionConfiguration) {
        xmppClient = XmppClient.create(xmppServiceDomain, connectionConfiguration);
    }

    @Override
    public void connect() throws XmppException {
        xmppClient.connect();
    }

    @Override
    public void login(String user, String password, String resource) throws XmppException {
        xmppClient.login(user, password, resource);
    }

    @Override
    public void close() throws XmppException {
        xmppClient.close();
    }

    @Override
    public Future<Void> send(StreamElement element) {
        return xmppClient.send(element);
    }

    @Override
    public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
        xmppClient.addInboundMessageListener(messageListener);
    }

    @Override
    public ChatRoom createChatRoom(Jid chatService, String roomName) {
        final MultiUserChatManager mucManager = xmppClient.getManager(MultiUserChatManager.class);
        return new RocksChatRoom(mucManager.createChatService(chatService).createRoom(roomName));
    }
}
