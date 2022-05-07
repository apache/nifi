package org.apache.nifi.cdc.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class MockBinlogClientJava extends BinaryLogClient {
    String hostname;
    int port;
    String username;
    String password;

    boolean connected;
    public boolean connectionTimeout = false;
    public boolean connectionError = false;

    List<LifecycleListener> lifecycleListeners = new ArrayList<>();
    SSLSocketFactory sslSocketFactory;

    List<EventListener> eventListeners = new ArrayList<>();


    public MockBinlogClientJava(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }
    @Override
    public void connect(long timeoutInMilliseconds) throws IOException, TimeoutException {
        if (connectionTimeout) {
            throw new TimeoutException("Connection timed out");
        }
        if (connectionError) {
            throw new IOException("Error during connect");
        }
        if (password == null) {
            throw new NullPointerException("Password can't be null");
        }
        connected = true;
    }
    @Override
    public void disconnect() throws IOException {
        connected = false;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void registerEventListener(EventListener eventListener) {
        System.out.println("Registering event listener");
        eventListeners.add(eventListener);
    }

    public void unregisterEventListener(EventListener eventListener) {
        eventListeners.remove (eventListener);
    }

    @Override
    public void registerLifecycleListener(LifecycleListener lifecycleListener) {
        if (!lifecycleListeners.contains(lifecycleListener)) {
            lifecycleListeners.add (lifecycleListener);
        }
    }

    @Override
    public void unregisterLifecycleListener(LifecycleListener lifecycleListener) {
        lifecycleListeners.remove(lifecycleListener);
    }

    @Override
    public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        super.setSslSocketFactory(sslSocketFactory);
        this.sslSocketFactory = sslSocketFactory;
    }

    public void sendEvent(Event event) {
        for (EventListener eventListener : eventListeners) {
            eventListener.onEvent(event);
        }
    }
}
