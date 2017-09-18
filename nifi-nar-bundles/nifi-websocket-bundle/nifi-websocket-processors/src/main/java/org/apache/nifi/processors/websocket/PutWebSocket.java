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
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_BROADCAST;
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
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketService;

@Tags({"WebSocket", "publish", "send"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerSerially
@CapabilityDescription("Sends messages to a WebSocket remote endpoint" +
        " using a WebSocket session that is established by either ListenWebSocket or ConnectWebSocket.")
@ReadsAttributes({
        @ReadsAttribute(attribute = ATTR_WS_BROADCAST, description = "Broadcast related. Defined when the flowfile was forked due to a send failure to the WebSocket session id. " +
            "Used to control success behavior; on success, this causes the flowfile to be dropped. Since this is a forked flowfile, the parent " +
            "flowfile already transfered to success (because at least one client received the broadcast message). Transfering any forked flowfile to " +
            "success would effectivly duplicate success for the same data."),
})
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_WS_CS_ID, description = "WebSocket Controller Service id."),
        @WritesAttribute(attribute = ATTR_WS_SESSION_ID, description = "Established WebSocket session id."),
        @WritesAttribute(attribute = ATTR_WS_ENDPOINT_ID, description = "WebSocket endpoint id."),
        @WritesAttribute(attribute = ATTR_WS_MESSAGE_TYPE, description = "TEXT or BINARY."),
        @WritesAttribute(attribute = ATTR_WS_LOCAL_ADDRESS, description = "WebSocket server address. If the message is sent to more than one client (e.g. broadcast) " +
                "then it will be a comma separated list of addresses; where any address that could not be sent to on the first try will end with an '*'. " +
                "This maintains an audit trail of addresses; because if the client's message is actually sent to the client later, that (forked) flowfile is dropped " +
                "(it does not transfer to success). Only the parent flowfile transfers to success."),
        @WritesAttribute(attribute = ATTR_WS_REMOTE_ADDRESS, description = "WebSocket client address. If the message is sent to more than one client (e.g. broadcast) " +
                "then it will be a comma separated list of addresses; where any address that could not be sent to on the first try will end with an '*'. " +
                "This maintains an audit trail of addresses; because if the client's message is actually sent to the client later, that (forked) flowfile is dropped " +
                "(it does not transfer to success). Only the parent flowfile transfers to success."),
        @WritesAttribute(attribute = ATTR_WS_FAILURE_DETAIL, description = "Detail of the failure."),
        @WritesAttribute(attribute = ATTR_WS_BROADCAST, description = "Broadcast related. Defined when the flowfile was forked due to a send failure to the WebSocket session id. " +
                "Used to control success behavior; on success, this causes the flowfile to be dropped. Since this is a forked flowfile, the parent " +
                "flowfile already transfered to success (because at least one client received the broadcast message). Transfering any forked flowfile to " +
                "success would effectivly duplicate success for the same data."),
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutWebSocket extends AbstractProcessor {

    public static final PropertyDescriptor PROP_WS_SESSION_ID = new PropertyDescriptor.Builder()
            .name("websocket-session-id")
            .displayName("WebSocket Session Id")
            .description("A NiFi Expression to retrieve the session id. If not specified, a message will be " +
                    "sent (e.g. broadcast) to all connected WebSocket clients for the WebSocket controller service id and endpoint. " +
                    "Under the condition where some clients can be sent to and others cannot, the flowfile will transfer to success " +
                    "and each failed client will get a forked copy of the flowfile with ${" + ATTR_WS_SESSION_ID + "} set (to the client's session id) and " +
                    "${" + ATTR_WS_BROADCAST + "} defined. Then it is routed to failure; such that if failure is routed back " +
                    "into this processor it can be retried (as a non-broadcast message). For this reason, using a value other than the default can " +
                    "interfere with forked message processing. To handle this potential issue, any message that does not have a session id but " +
                    "has ${" + ATTR_WS_BROADCAST + "} defined will be dropped (so that it is not broadcast to all clients). " +
                    "If this processor is being used in a broadcast capacity and flowfiles come from a processor that adds the ${" + ATTR_WS_SESSION_ID + "} " +
                    "attribute (ex. ListenWebSocket), the ${" + ATTR_WS_SESSION_ID + "} attribute needs to be removed from the flowfile before it gets to this " +
                    "processor. Another option is to get creative with Expression Language referencing ${" + ATTR_WS_SESSION_ID + "} and ${" + ATTR_WS_BROADCAST + "}.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + ATTR_WS_SESSION_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ID = new PropertyDescriptor.Builder()
            .name("websocket-controller-service-id")
            .displayName("WebSocket ControllerService Id")
            .description("A NiFi Expression to retrieve the id of a WebSocket ControllerService. " +
                    "Setting this value to the Id of a ConnectWebSocket processor allows this processor to send messages via the ConnectWebSocket's " +
                    "WebSocket controller service id and endpoint; as well as the session id of the client it is connected to (at the time the message " +
                    "is sent). Under this configuration, do not set this processor's WebSocket Session Id (this allows the session id to be obtained from " +
                    "the referenced ConnectWebSocket processor). Similarly, if the flowfile comes from a processor that writes this value to an attribute " +
                    "(ex. ListenWebSocket) it can be used; and, if this processor's WebSocket Session Id is not set then this processor will send the " +
                    "message to every client connected to it (at the time the message is sent). The creation of forked copies of the flowfile " +
                    "can only occur when more than one client is in the list of clients (to send to)." +
                    "See also WebSocket Endpoint Id.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + ATTR_WS_CS_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ENDPOINT = new PropertyDescriptor.Builder()
            .name("websocket-endpoint-id")
            .displayName("WebSocket Endpoint Id")
            .description("A NiFi Expression to retrieve the endpoint id of a WebSocket ControllerService. " +
                    "Setting this value to a ConnectWebSocket processor's WebSocket Endpoint Id value allows this processor to send messages via the " +
                    "ConnectWebSocket's connection. See also WebSocket ControllerService Id.")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_SESSION_UNKNOWN = new Relationship.Builder()
            .name("session unknown")
            .description("FlowFiles that failed to send to the destination because the session id does not exist are transferred to this relationship.")
            .build();

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static{
        final List<PropertyDescriptor> innerDescriptorsList = new ArrayList<>();
        innerDescriptorsList.add(PROP_WS_SESSION_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ENDPOINT);
        innerDescriptorsList.add(PROP_WS_MESSAGE_TYPE);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> innerRelationshipsSet = new HashSet<>();
        innerRelationshipsSet.add(REL_SUCCESS);
        innerRelationshipsSet.add(REL_FAILURE);
        innerRelationshipsSet.add(REL_SESSION_UNKNOWN);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @SuppressWarnings("unused")
    private class sessionIdInfo {
        String sessionId;
        long startSending;
        long stopSending;
        final AtomicReference<String> transitUri = new AtomicReference<>();
        final Map<String, String> attrs = new HashMap<>();
        boolean success = false;
        Exception e = null;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) throws ProcessException {
        final FlowFile flowfile = processSession.get();
        if (flowfile == null) {
            return;
        }

        final List<sessionIdInfo> sessionIdInfoList = new ArrayList<sessionIdInfo>();

        final String sessionIdProperty = context.getProperty(PROP_WS_SESSION_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceId = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceEndpoint = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ENDPOINT)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String messageTypeStr = context.getProperty(PROP_WS_MESSAGE_TYPE)
                .evaluateAttributeExpressions(flowfile).getValue();
        final WebSocketMessage.Type messageType = WebSocketMessage.Type.valueOf(messageTypeStr);
        final boolean flowfileIsBroadcast = (null != flowfile.getAttribute(ATTR_WS_BROADCAST));

        if (StringUtils.isEmpty(sessionIdProperty)) {
            getLogger().debug("Specific SessionID not specified. Message will be broadcast to all connected clients.");
        }

        if (StringUtils.isEmpty(webSocketServiceId)
                || StringUtils.isEmpty(webSocketServiceEndpoint)) {
            transferToFailure(processSession, flowfile, "Required WebSocket attribute was not found.");
            return;
        }

        final ControllerService controllerService = context.getControllerServiceLookup().getControllerService(webSocketServiceId);
        if (controllerService == null) {
            transferToFailure(processSession, flowfile, "WebSocket ControllerService was not found.");
            return;
        } else if (!(controllerService instanceof WebSocketService)) {
            transferToFailure(processSession, flowfile, "The ControllerService found was not a WebSocket ControllerService but a "
                    + controllerService.getClass().getName());
            return;
        }

        final WebSocketService webSocketService = (WebSocketService)controllerService;
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        final long startSending = System.currentTimeMillis();

        processSession.read(flowfile, in -> {
            StreamUtils.fillBuffer(in, messageContent, true);
        });

        final boolean broadcastMessage = StringUtils.isEmpty(sessionIdProperty);

        if (broadcastMessage && flowfileIsBroadcast) {
            // message is a broadcast (no websocket.session.id) *and* id'd as a FORKed message (websocket.broadcast) - which isn't good.
            //  Sending it on would cause a duplicate, so just drop it.
            final FlowFile updatedFlowFile = processSession.penalize(processSession.putAttribute(flowfile, ATTR_WS_FAILURE_DETAIL,
                                                                        "No session id; message was FORKed from a broadcast message"));
            processSession.remove(updatedFlowFile);    // terminate relationship
            return;
        }

        final List<String> sessionIds = new ArrayList<String>();
        final HashSet<String> currentSessionIds = new HashSet<String>();
        WebSocketConfigurationException currentSessionIdsEx = null;
        try {
            currentSessionIds.addAll(webSocketService.getSessionIds(webSocketServiceEndpoint));
        } catch (WebSocketConfigurationException e) {
            // WebSocketConfigurationException: If the corresponding WebSocketGatewayProcessor has been stopped.
            currentSessionIdsEx = e;
        }

        if (broadcastMessage) {
            if (currentSessionIdsEx != null) {
                getLogger().error("Failed to obtain list of SessionIds via WebSocket due to " + currentSessionIdsEx, currentSessionIdsEx);
                transferToFailure(processSession, flowfile, currentSessionIdsEx.toString());
                return;
            }
            else if (currentSessionIds.isEmpty()) {
                getLogger().error("Failed to obtain list of SessionIds via WebSocket due to no Sessions connected");
                transferToFailure(processSession, flowfile, "No connected WebSocket Sessions");
                return;
            }
            sessionIds.addAll(currentSessionIds);
        }
        else {
            if (currentSessionIds.contains(sessionIdProperty)) {
                sessionIds.add(sessionIdProperty);
            }
            else {
                final FlowFile updatedFlowFile = processSession.penalize(processSession.putAttribute(flowfile, ATTR_WS_FAILURE_DETAIL, "No such session id"));
                if( flowfileIsBroadcast ) {
                    // flowfile was created earlier by the below FORK; don't need it anymore. Sending it to SESSION UNKNOWN would cause a duplicate.
                    processSession.remove(updatedFlowFile);    // terminate relationship
                }
                else {
                    processSession.transfer(updatedFlowFile, REL_SESSION_UNKNOWN);
                }
                return;
            }
        }

        final HashSet<String> ws_local_addresses = new HashSet<String>();
        final HashSet<String> ws_remote_addresses = new HashSet<String>();
        Integer sessionCount = 0, successCount = 0, failureCount = 0;
        for (String sessionId : sessionIds) {
            sessionCount ++;
            boolean sendSuccess = false;
            final sessionIdInfo sessionIdInfoEntry = new sessionIdInfo();
            sessionIdInfoEntry.sessionId = sessionId;
            sessionIdInfoEntry.attrs.put(ATTR_WS_CS_ID, webSocketService.getIdentifier());
            sessionIdInfoEntry.attrs.put(ATTR_WS_ENDPOINT_ID, webSocketServiceEndpoint);
            sessionIdInfoEntry.attrs.put(ATTR_WS_MESSAGE_TYPE, messageTypeStr);
            if (!broadcastMessage) {
                sessionIdInfoEntry.attrs.put(ATTR_WS_SESSION_ID, sessionId);
            }
            sessionIdInfoEntry.startSending = System.currentTimeMillis();
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
                    sessionIdInfoEntry.attrs.put(ATTR_WS_LOCAL_ADDRESS, sender.getLocalAddress().toString());
                    sessionIdInfoEntry.attrs.put(ATTR_WS_REMOTE_ADDRESS, sender.getRemoteAddress().toString());
                    sessionIdInfoEntry.transitUri.set(sender.getTransitUri());
                });
                sendSuccess = true;
            } catch (WebSocketConfigurationException|IllegalStateException|IOException e) {
                // WebSocketConfigurationException: If the corresponding WebSocketGatewayProcessor has been stopped.
                // IllegalStateException: Session is already closed or not found.
                // IOException: other IO error.
                sessionIdInfoEntry.e = e;
                getLogger().error("Failed to send message (sessionId '" + sessionId + "') via WebSocket due to " + e, e);
            }
            if (sendSuccess) {
                successCount ++;
                sessionIdInfoEntry.success = true;
            }
            else {
                failureCount ++;
            }
            String address = sessionIdInfoEntry.attrs.get(ATTR_WS_LOCAL_ADDRESS);
            if (address != null)
                ws_local_addresses.add(address + (sendSuccess ? "" : "*"));
            address = sessionIdInfoEntry.attrs.get(ATTR_WS_REMOTE_ADDRESS);
            if (address != null)
                ws_remote_addresses.add(address + (sendSuccess ? "" : "*"));

            sessionIdInfoEntry.stopSending = System.currentTimeMillis();
            sessionIdInfoList.add(sessionIdInfoEntry);
        }
        final long transmissionMillis = System.currentTimeMillis() - startSending;

        if (sessionCount > 1 && failureCount > 0) {
            // FORK every failed broadcast (sessionCount >1) message; adding the session id attribute to make it a non-broadcast message
            for (sessionIdInfo sessionIdInfoEntry : sessionIdInfoList) {
                if (sessionIdInfoEntry.success)
                    continue;
                FlowFile forkFlowFile = processSession.create(flowfile);
                forkFlowFile = processSession.putAllAttributes(forkFlowFile, sessionIdInfoEntry.attrs);
                forkFlowFile = processSession.putAttribute(forkFlowFile, ATTR_WS_SESSION_ID, sessionIdInfoEntry.sessionId);
                forkFlowFile = processSession.putAttribute(forkFlowFile, ATTR_WS_BROADCAST, "");  // so we know it was a FORK; used later on
                transferToFailure(processSession, forkFlowFile, sessionIdInfoEntry.e.toString());
            }
        }
        // decide fate of the flowfile
        final sessionIdInfo sessionIdInfoEntry = sessionIdInfoList.get(0);  // use info from the first (and maybe only) entry
        if (successCount > 0) {     // SUCCESS sending to at least one destination
            if (sessionIds.size() > 1) {    // when applicable, retain list of local and remote address that were sent to successfully
                String addresses;
                if (!ws_local_addresses.isEmpty()) {
                    addresses = ws_local_addresses.toString();
                    sessionIdInfoEntry.attrs.put(ATTR_WS_LOCAL_ADDRESS, addresses.substring(1, addresses.length()-1));
                }
                if (!ws_remote_addresses.isEmpty()) {
                    addresses = ws_remote_addresses.toString();
                    sessionIdInfoEntry.attrs.put(ATTR_WS_REMOTE_ADDRESS, addresses.substring(1, addresses.length()-1));
                }
            }
            final FlowFile updatedFlowFile = processSession.putAllAttributes(flowfile, sessionIdInfoEntry.attrs);
            processSession.getProvenanceReporter().send(updatedFlowFile, sessionIdInfoEntry.transitUri.get(), transmissionMillis);
            if( flowfileIsBroadcast ) {
                // flowfile was created earlier by the above FORK; don't need it anymore. Sending it to SUCCESS would cause a duplicate.
                processSession.remove(updatedFlowFile);    // terminate relationship
            }
            else {
                processSession.transfer(updatedFlowFile, REL_SUCCESS);
            }
        }
        else {  // FAILURE sending to at least one destination
            transferToFailure(processSession, flowfile, sessionIdInfoEntry.e.toString());
        }
    }

    private FlowFile transferToFailure(final ProcessSession processSession, FlowFile flowfile, final String value) {
        flowfile = processSession.putAttribute(flowfile, ATTR_WS_FAILURE_DETAIL, value);
        processSession.transfer(flowfile, REL_FAILURE);
        return flowfile;
    }

}
