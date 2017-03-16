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
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

import java.util.UUID;

public class RoutingWebSocketListener extends WebSocketAdapter {
    private final WebSocketMessageRouter router;
    private String sessionId;

    public RoutingWebSocketListener(final WebSocketMessageRouter router) {
        this.router = router;
    }

    @Override
    public void onWebSocketConnect(final Session session) {
        super.onWebSocketConnect(session);
        if (sessionId == null || sessionId.isEmpty()) {
            // If sessionId is already assigned to this instance, don't publish new one.
            // So that existing sesionId can be reused when reconnecting.
            sessionId = UUID.randomUUID().toString();
        }
        final JettyWebSocketSession webSocketSession = new JettyWebSocketSession(sessionId, session);
        router.captureSession(webSocketSession);
    }

    @Override
    public void onWebSocketClose(final int statusCode, final String reason) {
        super.onWebSocketClose(statusCode, reason);
        router.onWebSocketClose(sessionId, statusCode, reason);
    }

    @Override
    public void onWebSocketText(final String message) {
        router.onWebSocketText(sessionId, message);
    }

    @Override
    public void onWebSocketBinary(final byte[] payload, final int offset, final int len) {
        router.onWebSocketBinary(sessionId, payload, offset, len);
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getSessionId() {
        return sessionId;
    }
}
