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
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

@TriggerSerially
public abstract class AbstractWebSocketGatewayProcessor extends AbstractWebSocketProcessor implements ConnectedListener, TextMessageConsumer, BinaryMessageConsumer {

    public static final PropertyDescriptor PROP_MAX_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("max-queue-size")
            .displayName("Max Queue Size")
            .description("The WebSocket messages are kept in an on-memory queue," +
                    " then transferred to relationships when this processor is triggered." +
                    " If the 'Run Schedule' is significantly behind the rate" +
                    " at which the messages are arriving to this processor then a back up can occur." +
                    " This property specifies the maximum number of messages this processor will hold in memory at one time." +
                    " CAUTION: Any incoming WebSocket message arrived while the queue being full" +
                    " will be discarded and a warning message will be logged.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    private volatile LinkedBlockingQueue<WebSocketMessage> incomingMessageQueue;
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

    static List<PropertyDescriptor> getAbstractPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_MAX_QUEUE_SIZE);
        return descriptors;
    }

    static Set<Relationship> getAbstractRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_CONNECTED);
        relationships.add(REL_MESSAGE_TEXT);
        relationships.add(REL_MESSAGE_BINARY);
        return relationships;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // resize the receive buffer, but preserve data
        if (PROP_MAX_QUEUE_SIZE.equals(descriptor)) {
            // it's a mandatory integer, never null
            int newSize = Integer.valueOf(newValue);
            if (incomingMessageQueue != null) {
                int msgPending = incomingMessageQueue.size();
                if (msgPending > newSize) {
                    logger.warn("New receive buffer size ({}) is smaller than the number of messages pending ({}), ignoring resize request. Processor will be invalid.",
                            new Object[]{newSize, msgPending});
                    return;
                }
                LinkedBlockingQueue<WebSocketMessage> newBuffer = new LinkedBlockingQueue<>(newSize);
                incomingMessageQueue.drainTo(newBuffer);
                incomingMessageQueue = newBuffer;
            }

        }
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>(super.customValidate(context));
        int newSize = context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger();
        if (incomingMessageQueue == null) {
            incomingMessageQueue = new LinkedBlockingQueue<>(context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger());
        }
        int msgPending = incomingMessageQueue.size();
        if (msgPending > newSize) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(PROP_MAX_QUEUE_SIZE.getDisplayName())
                    .explanation(String.format("%s (%d) is smaller than the number of messages pending (%d).",
                            PROP_MAX_QUEUE_SIZE.getDisplayName(), newSize, msgPending))
                    .build());
        }

        return results;
    }

    private void enqueueMessage(WebSocketMessage message) {
        try {
            incomingMessageQueue.add(message);
        } catch (Exception e) {
            logger.warn("Failed to enqueue message from {}, due to: {}",
                    new Object[]{message.getSessionInfo().getRemoteAddress(), e.getMessage()}, e);
        }
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
    public void onWebSocketServiceReady(final WebSocketService webSocketService, final ProcessContext context) throws IOException {

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

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        if (webSocketService == null) {
            return;
        }

        try {
            // Deregister processor, so that it won't receive messages anymore.
            webSocketService.deregisterProcessor(endpointId, this);
        } catch (WebSocketConfigurationException e) {
            logger.warn("Failed to deregister processor {} due to: {}", new Object[]{this, e}, e);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws IOException {
        try {
            if (incomingMessageQueue != null && !incomingMessageQueue.isEmpty() && processSessionFactory != null) {
                logger.info("Finishing processing leftover messages");
                ProcessSession session = processSessionFactory.createSession();
                transferQueue(session);
            } else {
                if (incomingMessageQueue != null && !incomingMessageQueue.isEmpty()){
                    throw new ProcessException("Stopping the processor but there is no ProcessSessionFactory stored" +
                            " and there are messages in the WebSocket internal queue. Removing the processor now will" +
                            " clear the queue but will result in DATA LOSS. This is normally due to starting the processor," +
                            " receiving messages and stopping before the onTrigger happens. The messages" +
                            " in the WebSocket internal queue cannot finish processing until the processor is triggered to run.");
                }
            }
        } finally {
            webSocketService = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        if (!isProcessorRegisteredToService()) {
            try {
                registerProcessorToService(context, webSocketService -> onWebSocketServiceReady(webSocketService, context));
            } catch (IOException|WebSocketConfigurationException e) {
                throw new ProcessException("Failed to register processor to WebSocket service due to: " + e, e);
            }
        }

        if (incomingMessageQueue.isEmpty()) {
            return;
        }

        transferQueue(session);
    }

    private void transferQueue(final ProcessSession session){
        while (!incomingMessageQueue.isEmpty()) {
            final WebSocketMessage incomingMessage = incomingMessageQueue.peek();
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

            if (!incomingMessageQueue.remove(incomingMessage) && logger.isWarnEnabled()) {
                logger.warn(new StringBuilder("FlowFile ")
                        .append(messageFlowFile.getAttribute(CoreAttributes.UUID.key()))
                        .append(" for WebSocket message ")
                        .append(incomingMessage)
                        .append(" had already been removed from queue, possible duplication of flow files")
                        .toString());
            }
        }
    }

    abstract protected String getTransitUri(final WebSocketSessionInfo sessionInfo);

}
