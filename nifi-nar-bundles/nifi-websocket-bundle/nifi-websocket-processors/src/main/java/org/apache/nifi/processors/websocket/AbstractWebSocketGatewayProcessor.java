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

package org.apache.nifi.processors.websocket;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.websocket.BinaryMessageConsumer;
import org.apache.nifi.websocket.ConnectedListener;
import org.apache.nifi.websocket.TextMessageConsumer;
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketConnectedMessage;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketService;
import org.apache.nifi.websocket.WebSocketSessionInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;

@TriggerSerially
public abstract class AbstractWebSocketGatewayProcessor extends AbstractSessionFactoryProcessor implements ConnectedListener, TextMessageConsumer, BinaryMessageConsumer {

    protected volatile ComponentLog logger;
    protected volatile ProcessSessionFactory processSessionFactory;

    protected WebSocketService webSocketService;
    protected String endpointId;

    public static final Relationship REL_CONNECTED = new Relationship.Builder()
            .name("connected")
            .description("The WebSocket session is established")
            .build();

    public static final Relationship REL_MESSAGE_TEXT = new Relationship.Builder()
            .name("text message")
            .description("The WebSocket text message output")
            .build();

    public static final Relationship REL_MESSAGE_BINARY = new Relationship.Builder()
            .name("binary message")
            .description("The WebSocket binary message output")
            .build();

    static Set<Relationship> getAbstractRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_CONNECTED);
        relationships.add(REL_MESSAGE_TEXT);
        relationships.add(REL_MESSAGE_BINARY);
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    public interface WebSocketFunction {
        void execute(final WebSocketService webSocketService) throws IOException, WebSocketConfigurationException;
    }

    @Override
    public void connected(WebSocketSessionInfo sessionInfo) {
        final WebSocketMessage message = new WebSocketConnectedMessage(sessionInfo);
        sessionInfo.setTransitUri(getTransitUri(sessionInfo));
        enqueueMessage(message);
    }

    @Override
    public void consume(WebSocketSessionInfo sessionInfo, String messageStr) {
        final WebSocketMessage message = new WebSocketMessage(sessionInfo);
        sessionInfo.setTransitUri(getTransitUri(sessionInfo));
        message.setPayload(messageStr);
        enqueueMessage(message);
    }

    @Override
    public void consume(WebSocketSessionInfo sessionInfo, byte[] payload, int offset, int length) {
        final WebSocketMessage message = new WebSocketMessage(sessionInfo);
        sessionInfo.setTransitUri(getTransitUri(sessionInfo));
        message.setPayload(payload, offset, length);
        enqueueMessage(message);
    }

    // @OnScheduled can not report error messages well on bulletin since it's an async method.
    // So, let's do it in onTrigger().
    public void onWebSocketServiceReady(final WebSocketService webSocketService) throws IOException {

        if (webSocketService instanceof WebSocketClientService) {
            // If it's a ws client, then connect to the remote here.
            // Otherwise, ws server is already started at WebSocketServerService
            ((WebSocketClientService) webSocketService).connect(endpointId);
        }

    }

    protected void registerProcessorToService(final ProcessContext context, final WebSocketFunction afterRegistration) throws IOException, WebSocketConfigurationException {
        webSocketService = getWebSocketService(context);
        endpointId = getEndpointId(context);
        webSocketService.registerProcessor(endpointId, this);

        afterRegistration.execute(webSocketService);
    }

    protected abstract WebSocketService getWebSocketService(final ProcessContext context);
    protected abstract String getEndpointId(final ProcessContext context);

    protected boolean isProcessorRegisteredToService() {
        return webSocketService != null
                && !StringUtils.isEmpty(endpointId)
                && webSocketService.isProcessorRegistered(endpointId, this);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws IOException {
        deregister();
    }

    private void deregister() {
        if (webSocketService == null) {
            return;
        }

        try {
            // Deregister processor, so that it won't receive messages anymore.
            webSocketService.deregisterProcessor(endpointId, this);
            webSocketService = null;
        } catch (WebSocketConfigurationException e) {
            logger.warn("Failed to deregister processor {} due to: {}", new Object[]{this, e}, e);
        }
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (processSessionFactory == null) {
            processSessionFactory = sessionFactory;
        }

        if (!isProcessorRegisteredToService()) {
            try {
                registerProcessorToService(context, webSocketService -> onWebSocketServiceReady(webSocketService));
            } catch (IOException|WebSocketConfigurationException e) {
                // Deregister processor if it failed so that it can retry next onTrigger.
                deregister();
                context.yield();
                throw new ProcessException("Failed to register processor to WebSocket service due to: " + e, e);
            }
        }

        context.yield();//nothing really to do here since handling WebSocket messages is done at ControllerService.
    }


    private void enqueueMessage(final WebSocketMessage incomingMessage){
        final ProcessSession session = processSessionFactory.createSession();
        try {
            FlowFile messageFlowFile = session.create();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put(ATTR_WS_CS_ID, webSocketService.getIdentifier());
            final WebSocketSessionInfo sessionInfo = incomingMessage.getSessionInfo();
            attrs.put(ATTR_WS_SESSION_ID, sessionInfo.getSessionId());
            attrs.put(ATTR_WS_ENDPOINT_ID, endpointId);
            attrs.put(ATTR_WS_LOCAL_ADDRESS, sessionInfo.getLocalAddress().toString());
            attrs.put(ATTR_WS_REMOTE_ADDRESS, sessionInfo.getRemoteAddress().toString());
            final WebSocketMessage.Type messageType = incomingMessage.getType();
            if (messageType != null) {
                attrs.put(ATTR_WS_MESSAGE_TYPE, messageType.name());
            }

            messageFlowFile = session.putAllAttributes(messageFlowFile, attrs);

            final byte[] payload = incomingMessage.getPayload();
            if (payload != null) {
                messageFlowFile = session.write(messageFlowFile, out -> {
                    out.write(payload, incomingMessage.getOffset(), incomingMessage.getLength());
                });
            }

            session.getProvenanceReporter().receive(messageFlowFile, getTransitUri(sessionInfo));

            if (incomingMessage instanceof WebSocketConnectedMessage) {
                session.transfer(messageFlowFile, REL_CONNECTED);
            } else {
                switch (messageType) {
                    case TEXT:
                        session.transfer(messageFlowFile, REL_MESSAGE_TEXT);
                        break;
                    case BINARY:
                        session.transfer(messageFlowFile, REL_MESSAGE_BINARY);
                        break;
                }
            }
            session.commit();

        } catch (Exception e) {
            logger.error("Unable to fully process input due to " + e, e);
            session.rollback();
        }
    }

    abstract protected String getTransitUri(final WebSocketSessionInfo sessionInfo);

}
