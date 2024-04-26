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
package org.apache.nifi.cdc.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * A mock implementation for BinaryLogClient, in order to unit test the connection and event handling logic
 */
public class MockBinlogClient extends BinaryLogClient {
    private final String password;
    private boolean connected;
    private boolean connectionTimeout = false;
    private boolean connectionError = false;

    private final List<EventListener> eventListeners = new ArrayList<>();
    private final List<BinaryLogClient.LifecycleListener> lifecycleListeners = new ArrayList<>();

    SSLSocketFactory sslSocketFactory;

    public MockBinlogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
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
    public void disconnect() {
        connected = false;
    }


    @Override
    public void registerEventListener(BinaryLogClient.EventListener eventListener) {
        if (!eventListeners.contains(eventListener)) {
            eventListeners.add(eventListener);
        }
    }

    @Override
    public void unregisterEventListener(BinaryLogClient.EventListener eventListener) {
        eventListeners.remove(eventListener);
    }

    @Override
    public void registerLifecycleListener(BinaryLogClient.LifecycleListener lifecycleListener) {
        if (!lifecycleListeners.contains(lifecycleListener)) {
            lifecycleListeners.add(lifecycleListener);
        }
    }

    @Override
    public void unregisterLifecycleListener(BinaryLogClient.LifecycleListener lifecycleListener) {
        lifecycleListeners.remove(lifecycleListener);
    }

    @Override
    public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        super.setSslSocketFactory(sslSocketFactory);
        this.sslSocketFactory = sslSocketFactory;
    }

    public void sendEvent(Event event) {
        eventListeners.forEach(eventListener -> eventListener.onEvent(event));
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnectionTimeout(boolean connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setConnectionError(boolean connectionError) {
        this.connectionError = connectionError;
    }

    @Override
    public List<EventListener> getEventListeners() {
        return eventListeners;
    }

    @Override
    public List<LifecycleListener> getLifecycleListeners() {
        return lifecycleListeners;
    }

    public SSLSocketFactory getSslSocketFactory() {
        return sslSocketFactory;
    }
}