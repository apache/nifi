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

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_FAILURE_DETAIL;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_BROADCAST_SUCCEEDED;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_BROADCAST_FAILED;
import static org.apache.nifi.websocket.WebSocketMessage.CHARSET_NAME;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.websocket.SessionNotFoundException;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketService;

@Tags({"WebSocket", "publish", "send"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerSerially
@CapabilityDescription("Sends messages to a WebSocket remote endpoint" +
        " using a WebSocket session that is established by either ListenWebSocket or ConnectWebSocket." +
        " This processor can be configured to send a message to a specific peer by providing a WebSocket session id," +
        " or broadcasting to all connected peers, see description of 'WebSocket Session Id' for detail.")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_WS_CS_ID, description = "WebSocket Controller Service id."),
        @WritesAttribute(attribute = ATTR_WS_SESSION_ID, description = "Established WebSocket session id."),
        @WritesAttribute(attribute = ATTR_WS_ENDPOINT_ID, description = "WebSocket endpoint id."),
        @WritesAttribute(attribute = ATTR_WS_MESSAGE_TYPE, description = "TEXT or BINARY."),
        @WritesAttribute(attribute = ATTR_WS_LOCAL_ADDRESS, description = "The address of sending peer."),
        @WritesAttribute(attribute = ATTR_WS_REMOTE_ADDRESS, description = "The address of receiving peer. " +
                "If the message is sent to more than one peers (i.e. broadcast) then only the last peer address is captured."),
        @WritesAttribute(attribute = ATTR_WS_FAILURE_DETAIL, description = "Detail of the failure. " +
                "If the message fails with more than one peers (i.e. broadcast) then only the last peer address is captured."),
        @WritesAttribute(attribute = ATTR_WS_BROADCAST_SUCCEEDED, description = "The number of messages sent successfully. Only available when a specific session id is not provided."),
        @WritesAttribute(attribute = ATTR_WS_BROADCAST_FAILED, description = "The number of messages failed to be sent. Only available when a specific session id is not provided.")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutWebSocket extends AbstractProcessor {

    public static final PropertyDescriptor PROP_WS_SESSION_ID = new PropertyDescriptor.Builder()
            .name("websocket-session-id")
            .displayName("WebSocket Session Id")
            .description("A NiFi Expression to retrieve the session id. If not specified, a message will be " +
                    "sent (i.e. broadcast) to all connected WebSocket peers for the specified endpoint of the WebSocket controller service. " +
                    "Under the condition where some peers can be sent to and others cannot, the FlowFile will be transferred to success " +
                    "and if 'Fork Failed Broadcast Sessions' is enabled, for each failed peer a forked copy of the FlowFile " +
                    "with ${" + ATTR_WS_SESSION_ID + "} set (to the peer's session id) is routed to failure. " +
                    "So that if failure is routed back into this processor it can be retried (as a non-broadcast message).")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + ATTR_WS_SESSION_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ID = new PropertyDescriptor.Builder()
            .name("websocket-controller-service-id")
            .displayName("WebSocket ControllerService Id")
            .description("A NiFi Expression to retrieve one from incoming FlowFile , or a specific WebSocket ControllerService ID.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + ATTR_WS_CS_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ENDPOINT = new PropertyDescriptor.Builder()
            .name("websocket-endpoint-id")
            .displayName("WebSocket Endpoint Id")
            .description("A NiFi Expression to retrieve one from incoming FlowFile, or a specific 'endpoint id' of a WebSocket ControllerService. " +
                    "An 'endpoint id' is managed differently by different WebSocket ControllerServices to group one or more WebSocket sessions. " +
                    "'WebSocket Client Id' for ConnectWebSocket, and 'Server URL Path' for ListenWebSocket respectively. ")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + ATTR_WS_ENDPOINT_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("websocket-message-type")
            .displayName("WebSocket Message Type")
            .description("The type of message content: TEXT or BINARY")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(WebSocketMessage.Type.TEXT.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_FORK_FAILED_BROADCAST_SESSIONS = new PropertyDescriptor.Builder()
            .name("fork-failed-broadcast-sessions")
            .displayName("Fork Failed Broadcast Sessions")
            .description("Whether to create forked copies of incoming FlowFile with individual WebSocket Session ID." +
                    " Useful when specific error handling or retry flow is needed per failed session.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS = new PropertyDescriptor.Builder()
            .name("enable-detailed-failure-relationships")
            .displayName("Enable Detailed Failure Relationships")
            .description("If enabled, detailed failure relationships: 'no connected sessions' and 'communication failure' are activated in addition to general 'failure' relationship.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles sent successfully to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles failed to be sent to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_NO_CONNECTED_SESSIONS = new Relationship.Builder()
            .name("no connected sessions")
            .description("FlowFiles intended to be broadcast to any connected sessions," +
                    " but no connected session is available at that time, are transferred to this relationship.")
            .build();
    public static final Relationship REL_COMMUNICATION_FAILURE = new Relationship.Builder()
            .name("communication failure")
            .description("FlowFiles failed to be sent to the destination due to communication failure are transferred to this relationship." +
                    " Connecting this relationship back to PutWebSocket may recover from a temporal communication error.")
            .build();

    public static final String NO_CONNECTED_WEB_SOCKET_SESSIONS = "No connected WebSocket Sessions";

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;
    private static final Set<Relationship> detailedRelationships;

    static{
        final List<PropertyDescriptor> innerDescriptorsList = new ArrayList<>();
        innerDescriptorsList.add(PROP_WS_SESSION_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ENDPOINT);
        innerDescriptorsList.add(PROP_WS_MESSAGE_TYPE);
        innerDescriptorsList.add(PROP_FORK_FAILED_BROADCAST_SESSIONS);
        innerDescriptorsList.add(PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);

        final Set<Relationship> detailedRels = new HashSet<>(rels);
        detailedRels.add(REL_COMMUNICATION_FAILURE);
        detailedRels.add(REL_NO_CONNECTED_SESSIONS);
        detailedRelationships = Collections.unmodifiableSet(detailedRels);
    }

    /**
     * Contains information per session ID.
     */
    private class SessionIdInfo {
        String sessionId;
        String localAddress;
        String remoteAddress;
        Exception e = null;
    }

    private volatile boolean enableDetailedFailureRelationships = false;

    @Override
    public Set<Relationship> getRelationships() {
        return enableDetailedFailureRelationships ? detailedRelationships : relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS.equals(descriptor)) {
            enableDetailedFailureRelationships = Boolean.valueOf(newValue);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) throws ProcessException {
        final FlowFile flowfile = processSession.get();
        if (flowfile == null) {
            return;
        }

        final String specifiedSessionId = context.getProperty(PROP_WS_SESSION_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceId = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceEndpoint = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ENDPOINT)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String messageTypeStr = context.getProperty(PROP_WS_MESSAGE_TYPE)
                .evaluateAttributeExpressions(flowfile).getValue();
        final WebSocketMessage.Type messageType = WebSocketMessage.Type.valueOf(messageTypeStr);

        if (StringUtils.isEmpty(webSocketServiceId)
                || StringUtils.isEmpty(webSocketServiceEndpoint)) {
            transferToFailure(processSession, flowfile, "Required WebSocket attribute was not found.", null);
            return;
        }

        final ControllerService controllerService = context.getControllerServiceLookup().getControllerService(webSocketServiceId);
        if (controllerService == null) {
            transferToFailure(processSession, flowfile, "WebSocket ControllerService was not found.", null);
            return;
        } else if (!(controllerService instanceof WebSocketService)) {
            transferToFailure(processSession, flowfile, "The ControllerService found was not a WebSocket ControllerService but a "
                    + controllerService.getClass().getName(), null);
            return;
        }

        final WebSocketService webSocketService = (WebSocketService)controllerService;
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        final long startSending = System.currentTimeMillis();

        final AtomicReference<String> transitUri = new AtomicReference<>();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(ATTR_WS_CS_ID, webSocketService.getIdentifier());

        boolean isEmptySessionId = StringUtils.isEmpty(specifiedSessionId);
        if (!isEmptySessionId) {
            attrs.put(ATTR_WS_SESSION_ID, specifiedSessionId);
        }

        attrs.put(ATTR_WS_ENDPOINT_ID, webSocketServiceEndpoint);
        attrs.put(ATTR_WS_MESSAGE_TYPE, messageTypeStr);

        processSession.read(flowfile, in -> {
            StreamUtils.fillBuffer(in, messageContent, true);
        });


        final List<String> sessionIds = new ArrayList<>();
        if (isEmptySessionId) {
            try {
                // If session id is not specified, broadcast message to all connected peers.
                final Set<String> currentSessionIds = webSocketService.getSessionIds(webSocketServiceEndpoint);
                if (currentSessionIds.isEmpty()) {
                    getLogger().error("Failed to obtain list of SessionIds via WebSocket, no Sessions connected");
                    transferToFailure(processSession, flowfile, NO_CONNECTED_WEB_SOCKET_SESSIONS, null);
                    return;
                }
                sessionIds.addAll(currentSessionIds);

            } catch (WebSocketConfigurationException e) {
                // WebSocketConfigurationException: If the corresponding WebSocketGatewayProcessor has been stopped.
                getLogger().error("Failed to obtain list of SessionIds via WebSocket due to " + e, e);
                transferToFailure(processSession, flowfile, e.toString(), e);
                return;
            }
        } else {
            sessionIds.add(specifiedSessionId);
        }

        // Only if there are more than 1 session Ids to send.
        // Even if session id is not provided, if there is only one session id, then it's not broadcasting.
        final boolean shouldBroadcast = sessionIds.size() > 1;

        final List<SessionIdInfo> succeededSessionIds = new ArrayList<>();
        final List<SessionIdInfo> failedSessionIds = new ArrayList<>();
        SessionIdInfo lastFailedSessionId = null;

        for (String sessionId : sessionIds) {
            final SessionIdInfo sessionIdInfo = new SessionIdInfo();
            sessionIdInfo.sessionId = sessionId;
            try {
                webSocketService.sendMessage(webSocketServiceEndpoint, sessionId, sender -> {
                    switch (messageType) {
                        case TEXT:
                            sender.sendString(new String(messageContent, CHARSET_NAME));
                            break;
                        case BINARY:
                            sender.sendBinary(ByteBuffer.wrap(messageContent));
                            break;
                    }

                    final String localAddress = sender.getLocalAddress().toString();
                    final String remoteAddress = sender.getRemoteAddress().toString();
                    sessionIdInfo.localAddress = localAddress;
                    sessionIdInfo.remoteAddress = remoteAddress;

                    // When there are multiple peers to send, we keep the only last peer's information
                    attrs.put(ATTR_WS_LOCAL_ADDRESS, localAddress);
                    attrs.put(ATTR_WS_REMOTE_ADDRESS, remoteAddress);
                    transitUri.set(sender.getTransitUri());
                });
                succeededSessionIds.add(sessionIdInfo);

            } catch (WebSocketConfigurationException | SessionNotFoundException | IOException e) {
                // WebSocketConfigurationException: If the corresponding WebSocketGatewayProcessor has been stopped.
                // IllegalStateException: Session is already closed or not found.
                // IOException: other IO error.
                getLogger().error("Failed to send message (sessionId '" + sessionId + "') via WebSocket due to " + e, e);
                sessionIdInfo.e = e;
                failedSessionIds.add(sessionIdInfo);
                lastFailedSessionId = sessionIdInfo;
            }
        }

        if (isEmptySessionId) {
            // If a session id is not specified, user would like to know how many messages are sent or failed.
            attrs.put(ATTR_WS_BROADCAST_SUCCEEDED, String.valueOf(succeededSessionIds.size()));
            attrs.put(ATTR_WS_BROADCAST_FAILED, String.valueOf(failedSessionIds.size()));
        }

        // FORK every failed broadcast message; adding the session id attribute so that it can be used as a non-broadcast message in subsequent flow
        final boolean forkFailedBroadcastSessions = shouldBroadcast && context.getProperty(PROP_FORK_FAILED_BROADCAST_SESSIONS).asBoolean();
        if (forkFailedBroadcastSessions) {
            Map<String, String> parSessionIdAttrs = new HashMap<>();
            for (SessionIdInfo sessionIdInfoEntry : failedSessionIds) {
                FlowFile forkFlowFile = processSession.create(flowfile);
                forkFlowFile = processSession.putAllAttributes(forkFlowFile, attrs);
                parSessionIdAttrs.put(ATTR_WS_SESSION_ID, sessionIdInfoEntry.sessionId);
                parSessionIdAttrs.put(ATTR_WS_LOCAL_ADDRESS, sessionIdInfoEntry.localAddress);
                parSessionIdAttrs.put(ATTR_WS_REMOTE_ADDRESS, sessionIdInfoEntry.remoteAddress);
                forkFlowFile = processSession.putAllAttributes(forkFlowFile, parSessionIdAttrs);
                transferToFailure(processSession, forkFlowFile, sessionIdInfoEntry.e.toString(), sessionIdInfoEntry.e);
            }
        }

        if (succeededSessionIds.size() > 0) {
            // SUCCESS sending to at least one destination
            final FlowFile updatedFlowFile = processSession.putAllAttributes(flowfile, attrs);
            final long transmissionMillis = System.currentTimeMillis() - startSending;
            processSession.getProvenanceReporter().send(updatedFlowFile, transitUri.get(), transmissionMillis);
            processSession.transfer(updatedFlowFile, REL_SUCCESS);

        } else if (lastFailedSessionId != null) {
            // Every session has failed to send the message.
            if (forkFailedBroadcastSessions) {
                // When all broadcast messages are failed, just remove the original FlowFile, as failed messages are forked and sent to failure.
                processSession.remove(flowfile);
            } else {
                // If not creating forked FlowFiles, send the original FlowFile to failure.
                final FlowFile failedOriginalFlowFile = processSession.putAllAttributes(flowfile, attrs);
                transferToFailure(processSession, failedOriginalFlowFile, lastFailedSessionId.e.toString(), lastFailedSessionId.e);
            }
        }
    }

    private FlowFile transferToFailure(final ProcessSession processSession, FlowFile flowfile, final String failureDetail, final Exception e) {
        flowfile = processSession.putAttribute(flowfile, ATTR_WS_FAILURE_DETAIL, failureDetail);
        if (enableDetailedFailureRelationships) {
            if (NO_CONNECTED_WEB_SOCKET_SESSIONS.equals(failureDetail)) {
                processSession.transfer(flowfile, REL_NO_CONNECTED_SESSIONS);
            } else if (e instanceof IOException) {
                processSession.transfer(flowfile, REL_COMMUNICATION_FAILURE);
            } else {
                processSession.transfer(flowfile, REL_FAILURE);
            }
        } else {
            processSession.transfer(flowfile, REL_FAILURE);
        }
        return flowfile;
    }

}
