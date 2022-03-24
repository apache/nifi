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
package org.apache.nifi.remote.client.socket;

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;

public class EndpointConnection {

    private final Peer peer;
    private final SocketClientProtocol socketClientProtocol;
    private final FlowFileCodec codec;
    private volatile long lastUsed;

    public EndpointConnection(final Peer peer, final SocketClientProtocol socketClientProtocol, final FlowFileCodec codec) {
        this.peer = peer;
        this.socketClientProtocol = socketClientProtocol;
        this.codec = codec;
    }

    public FlowFileCodec getCodec() {
        return codec;
    }

    public SocketClientProtocol getSocketClientProtocol() {
        return socketClientProtocol;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setLastTimeUsed() {
        lastUsed = System.currentTimeMillis();
    }

    public long getLastTimeUsed() {
        return lastUsed;
    }
}
