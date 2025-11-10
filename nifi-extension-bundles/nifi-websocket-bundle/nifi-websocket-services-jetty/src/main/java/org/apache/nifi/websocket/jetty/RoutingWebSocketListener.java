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

import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.Session.Listener.AbstractAutoDemanding;

import java.nio.ByteBuffer;
import java.util.UUID;

public class RoutingWebSocketListener extends AbstractAutoDemanding {
    private final WebSocketMessageRouter router;
    private String sessionId;
    private boolean secure;

    public RoutingWebSocketListener(final WebSocketMessageRouter router) {
        this.router = router;
    }

    @Override
    public void onWebSocketOpen(final Session session) {
        super.onWebSocketOpen(session);
        if (sessionId == null || sessionId.isEmpty()) {
            // If sessionId is already assigned to this instance, don't publish new one.
            // So that existing sesionId can be reused when reconnecting.
            sessionId = UUID.randomUUID().toString();
        }
        final boolean secureConnection = secure || isSessionSecure(session);
        final JettyWebSocketSession webSocketSession = new JettyWebSocketSession(sessionId, session, secureConnection);
        router.captureSession(webSocketSession);
    }

    @Override
    public void onWebSocketClose(final int statusCode, final String reason, Callback callback) {
        super.onWebSocketClose(statusCode, reason, callback);
        router.onWebSocketClose(sessionId, statusCode, reason);
    }

    @Override
    public void onWebSocketText(final String message) {
        router.onWebSocketText(sessionId, message);
    }

    @Override
    public void onWebSocketBinary(final ByteBuffer payload, final Callback callback) {
        router.onWebSocketBinary(sessionId, payload.array(), payload.arrayOffset(), payload.limit());
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setSecure(final boolean secure) {
        this.secure = secure;
    }

    public String getSessionId() {
        return sessionId;
    }

    private boolean isSessionSecure(final Session session) {
        try {
            return session.isSecure();
        } catch (final Exception e) {
            return false;
        }
    }
}
