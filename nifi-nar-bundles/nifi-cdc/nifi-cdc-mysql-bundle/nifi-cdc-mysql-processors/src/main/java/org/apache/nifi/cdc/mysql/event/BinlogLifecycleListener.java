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
package org.apache.nifi.cdc.mysql.event;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An listener wrapper for mysql-binlog-connector lifecycle events.
 */
public class BinlogLifecycleListener implements BinaryLogClient.LifecycleListener {

    AtomicReference<BinaryLogClient> client = new AtomicReference<>(null);
    AtomicReference<Exception> exception = new AtomicReference<>(null);

    @Override
    public void onConnect(BinaryLogClient binaryLogClient) {
        client.set(binaryLogClient);
        exception.set(null);
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
        client.set(binaryLogClient);
        exception.set(e);
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
        client.set(binaryLogClient);
        exception.set(e);
    }

    @Override
    public void onDisconnect(BinaryLogClient binaryLogClient) {
        client.set(binaryLogClient);
    }

    public BinaryLogClient getClient() {
        return client.get();
    }

    public Exception getException() {
        return exception.get();
    }
}
