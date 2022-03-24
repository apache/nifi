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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.websocket.AbstractWebSocketSession;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class JettyWebSocketSession extends AbstractWebSocketSession {

    private final String sessionId;
    private final Session session;

    public JettyWebSocketSession(final String sessionId, final Session session) {
        this.sessionId = sessionId;
        this.session = session;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public void sendString(final String message) throws IOException {
        session.getRemote().sendString(message);
    }

    @Override
    public void sendBinary(final ByteBuffer data) throws IOException {
        session.getRemote().sendBytes(data);
    }

    @Override
    public void close(final String reason) throws IOException {
        session.close(StatusCode.NORMAL, reason);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return session.getRemoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return session.getLocalAddress();
    }

    @Override
    public boolean isSecure() {
        return session.isSecure();
    }

}
