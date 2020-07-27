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
package org.apache.nifi.websocket;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketMessageRouter {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketMessageRouter.class);
    private final String endpointId;
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private volatile Processor processor;

    public WebSocketMessageRouter(final String endpointId) {
        this.endpointId = endpointId;
    }

    public synchronized void registerProcessor(final Processor processor) throws WebSocketConfigurationException {
        if (this.processor != null) {
            throw new WebSocketConfigurationException("Processor " + this.processor + " is already assigned to this router.");
        }
        this.processor = processor;
    }

    public boolean isProcessorRegistered(final Processor processor) {
        return this.processor != null && this.processor.getIdentifier().equals(processor.getIdentifier());
    }

    public synchronized void deregisterProcessor(final Processor processor) {
        if (!isProcessorRegistered(processor)) {
            if (this.processor == null) {
                logger.info("Deregister processor {}, do nothing because this router doesn't have registered processor",
                        new Object[]{processor});
            } else {
                logger.info("Deregister processor {}, do nothing because this router is assigned to different processor {}",
                        new Object[]{processor, this.processor});
            }
            return;
        }

        this.processor = null;
        sessions.keySet().forEach(sessionId -> {
            try {
                disconnect(sessionId, "Processing has stopped.");
            } catch (IOException e) {
                logger.warn("Failed to disconnect session {} due to {}", sessionId, e, e);
            }
        });
    }

    public void captureSession(final WebSocketSession session) {
        final String sessionId = session.getSessionId();
        sessions.put(sessionId, session);

        if (processor != null && processor instanceof ConnectedListener) {
            ((ConnectedListener)processor).connected(session);
        }
    }

    public void onWebSocketClose(final String sessionId, final int statusCode, final String reason) {
        sessions.remove(sessionId);
    }

    public void onWebSocketText(final String sessionId, final String message) {
        if (processor != null && processor instanceof TextMessageConsumer) {
            ((TextMessageConsumer)processor).consume(getSessionOrFail(sessionId), message);
        }
    }

    public void onWebSocketBinary(final String sessionId, final byte[] payload, final int offset, final int length) {
        if (processor != null && processor instanceof BinaryMessageConsumer) {
            ((BinaryMessageConsumer)processor).consume(getSessionOrFail(sessionId), payload, offset, length);
        }
    }

    private WebSocketSession getSessionOrFail(final String sessionId) {
        final WebSocketSession session = sessions.get(sessionId);
        if (session == null) {
            throw new IllegalStateException("Session was not found for the sessionId: " + sessionId);
        }
        return session;
    }

    public void sendMessage(final String sessionId, final SendMessage sendMessage) throws IOException {
        if (!StringUtils.isEmpty(sessionId)) {
            final WebSocketSession session = getSessionOrFail(sessionId);
            sendMessage.send(session);
        } else {
            //The sessionID is not specified so broadcast the message to all connected client sessions.
            sessions.keySet().forEach(itrSessionId -> {
                try {
                    final WebSocketSession session = getSessionOrFail(itrSessionId);
                    sendMessage.send(session);
                } catch (IOException e) {
                    logger.warn("Failed to send message to session {} due to {}", itrSessionId, e, e);
                }
            });
        }
    }

    public void disconnect(final String sessionId, final String reason) throws IOException {
        final WebSocketSession session = getSessionOrFail(sessionId);
        session.close(reason);
        sessions.remove(sessionId);
    }

    public boolean containsSession(final String sessionId) {
        return sessions.containsKey(sessionId);
    }

}
