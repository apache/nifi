package org.apache.nifi.processors.gettcp;

public interface DisconnectListener {

    void onDisconnect(ReceivingClient client);
}
