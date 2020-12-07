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
package org.apache.nifi.processors.splunk;

import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dto.splunk.SendRawDataResponse;
import org.apache.nifi.dto.splunk.SendRawDataSuccessResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"splunk", "logs", "http"})
@CapabilityDescription("Sends events to the specified Splunk server over HTTP or HTTPS. Supports HEC Index Acknowledgement.")
public class PutSplunkHTTP extends SplunkAPICall {
    private static final String ENDPOINT = "/services/collector/raw";

    static final PropertyDescriptor SOURCE = new PropertyDescriptor.Builder()
            .name("source")
            .displayName("Source")
            .description("User-defined event source. Sets a default for all events when unspecified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor.Builder()
            .name("source-type")
            .displayName("Source Type")
            .description("User-defined event sourcetype. Sets a default for all events when unspecified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("host")
            .displayName("Host")
            .description("Specify with the host query string parameter. Sets a default for all events when unspecified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("index")
            .displayName("Index")
            .description("Index name. Specify with the index query string parameter. Sets a default for all events when unspecified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("The name of the character set.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(Charset.defaultCharset().name())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("content-type")
            .displayName("Content Type")
            .description(
                    "The media type of the event sent to Splunk. " +
                    "If not set, \"mime.type\" flow file attribute will be used. " +
                    "In case of neither of them is specified, this information will not be sent to the server.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are sent out this relationship.")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            RELATIONSHIP_SUCCESS,
            RELATIONSHIP_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> result = new ArrayList<>(super.getSupportedPropertyDescriptors());
        result.add(SOURCE);
        result.add(SOURCE_TYPE);
        result.add(HOST);
        result.add(INDEX);
        result.add(CONTENT_TYPE);
        result.add(CHARSET);
        return result;
    }

    private volatile String endpoint;
    private volatile String contentType;
    private volatile String charset;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        if (context.getProperty(CONTENT_TYPE).isSet()) {
            contentType = context.getProperty(CONTENT_TYPE).evaluateAttributeExpressions().getValue();
        }

        charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();

        final Map<String, String> queryParameters = new HashMap<>();

        if (context.getProperty(SOURCE_TYPE).isSet()) {
            queryParameters.put("sourcetype", context.getProperty(SOURCE_TYPE).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(SOURCE).isSet()) {
            queryParameters.put("source", context.getProperty(SOURCE).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(HOST).isSet()) {
            queryParameters.put("host", context.getProperty(HOST).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(INDEX).isSet()) {
            queryParameters.put("index", context.getProperty(INDEX).evaluateAttributeExpressions().getValue());
        }

        if (queryParameters.isEmpty()) {
            endpoint = ENDPOINT;
        } else {
            endpoint = ENDPOINT + '?' + queryParameters.entrySet().stream().map(e -> e.getKey() + '=' + e.getValue()).collect(Collectors.joining("&"));
        }
    }

    @OnUnscheduled
    public void onUnscheduled() {
        super.onUnscheduled();
        contentType = null;
        charset = null;
        endpoint = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile incomingFlowFile = session.get();
        FlowFile outgoingFlowFile = incomingFlowFile;
        boolean success = false;

        try {
            final RequestMessage requestMessage = createRequestMessage(session, incomingFlowFile);
            final ResponseMessage responseMessage = call(endpoint, requestMessage);
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, "splunk_status_code", String.valueOf(responseMessage.getStatus()));

            switch (responseMessage.getStatus()) {
                case 200:
                    final SendRawDataSuccessResponse successResponse = extractResult(responseMessage.getContent(), SendRawDataSuccessResponse.class);

                    if (successResponse.getCode() == 0) {
                        outgoingFlowFile = enrichFlowFile(session, outgoingFlowFile, successResponse.getAckId());
                        success = true;
                    } else {
                        outgoingFlowFile = session.putAttribute(outgoingFlowFile, "splunk_response_code", String.valueOf(successResponse.getCode()));
                        getLogger().error("Putting data into Splunk was not successful: (" + successResponse.getCode() + ") " + successResponse.getText());
                    }

                    break;
                case 503 : // HEC is unhealthy, queues are full
                    context.yield();
                default:
                    final SendRawDataResponse response = extractResult(responseMessage.getContent(), SendRawDataResponse.class);
                    getLogger().error("Putting data into Splunk was not successful: " + response.getText());
            }
        } catch (final Exception e) {
            getLogger().error("Error during communication with Splunk: " + e.getMessage(), e);
        } finally {
            session.transfer(outgoingFlowFile, success ? RELATIONSHIP_SUCCESS : RELATIONSHIP_FAILURE);
        }
    }

    private RequestMessage createRequestMessage(final ProcessSession session, final FlowFile flowFile) {
        final RequestMessage requestMessage = new RequestMessage("POST");
        final String flowFileContentType = Optional.ofNullable(contentType).orElse(flowFile.getAttribute("mime.type"));

        if (flowFileContentType != null) {
            requestMessage.getHeader().put("Content-Type", flowFileContentType);
        }

        requestMessage.setContent(extractTextMessageBody(flowFile, session, charset));
        return requestMessage;
    }

    private String extractTextMessageBody(final FlowFile flowFile, final ProcessSession session, final String charset) {
        final StringWriter writer = new StringWriter();
        session.read(flowFile, in -> IOUtils.copy(in, writer, Charset.forName(charset)));
        return writer.toString();
    }

    private FlowFile enrichFlowFile(final ProcessSession session, final FlowFile flowFile, final int ackId) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ackIdAttributeName, String.valueOf(ackId));
        attributes.put(insertedAtAttributeName, String.valueOf(System.currentTimeMillis()));
        return session.putAllAttributes(flowFile, attributes);
    }
}
