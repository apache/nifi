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
package org.apache.nifi.cdc.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory

import java.util.concurrent.TimeoutException

/**
 * A mock implementation for BinaryLogClient, in order to unit test the connection and event handling logic
 */
class MockBinlogClient extends BinaryLogClient {

    String hostname
    int port
    String username
    String password

    boolean connected
    boolean connectionTimeout = false
    boolean connectionError = false

    List<BinaryLogClient.EventListener> eventListeners = []
    List<BinaryLogClient.LifecycleListener> lifecycleListeners = []

    SSLSocketFactory sslSocketFactory

    MockBinlogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password)
        this.hostname = hostname
        this.port = port
        this.username = username
        this.password = password
    }

    @Override
    void connect(long timeoutInMilliseconds) throws IOException, TimeoutException {
        if (connectionTimeout) {
            throw new TimeoutException('Connection timed out')
        }
        if (connectionError) {
            throw new IOException('Error during connect')
        }
        if (password == null) {
            throw new NullPointerException('''Password can't be null''')
        }
        connected = true
    }

    @Override
    void disconnect() throws IOException {
        connected = false
    }


    @Override
    void registerEventListener(BinaryLogClient.EventListener eventListener) {
        if (!eventListeners.contains(eventListener)) {
            eventListeners.add eventListener
        }
    }

    @Override
    void unregisterEventListener(BinaryLogClient.EventListener eventListener) {
        eventListeners.remove eventListener
    }

    @Override
    void registerLifecycleListener(BinaryLogClient.LifecycleListener lifecycleListener) {
        if (!lifecycleListeners.contains(lifecycleListener)) {
            lifecycleListeners.add lifecycleListener
        }
    }

    @Override
    void unregisterLifecycleListener(BinaryLogClient.LifecycleListener lifecycleListener) {
        lifecycleListeners.remove lifecycleListener
    }

    @Override
    void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        super.setSslSocketFactory(sslSocketFactory)
        this.sslSocketFactory = sslSocketFactory
    }

    def sendEvent(Event event) {
        eventListeners.each { it.onEvent(event) }
    }
}
