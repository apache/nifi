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
package org.apache.nifi.processors.standard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.util.StopWatch;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"http", "https", "response", "egress", "web service"})
@CapabilityDescription("Sends an HTTP Response to the Requestor that generated a FlowFile. This Processor is designed to be used in conjunction with "
        + "the HandleHttpRequest in order to create a web service.")
@DynamicProperty(name = "An HTTP header name", value = "An HTTP header value",
                    description = "These HTTPHeaders are set in the HTTP Response",
                    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@ReadsAttributes({
    @ReadsAttribute(attribute = HTTPUtils.HTTP_CONTEXT_ID, description = "The value of this attribute is used to lookup the HTTP Response so that the "
        + "proper message can be sent back to the requestor. If this attribute is missing, the FlowFile will be routed to 'failure.'"),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_REQUEST_URI, description = "Value of the URI requested by the client. Used for provenance event."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_REMOTE_HOST, description = "IP address of the client. Used for provenance event."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_LOCAL_NAME, description = "IP address/hostname of the server. Used for provenance event."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_PORT, description = "Listening port of the server. Used for provenance event."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_SSL_CERT, description = "SSL distinguished name (if any). Used for provenance event.")})
@SeeAlso(value = {HandleHttpRequest.class}, classNames = {"org.apache.nifi.http.StandardHttpContextMap", "org.apache.nifi.ssl.StandardSSLContextService"})
public class HandleHttpResponse extends AbstractProcessor {

    public static final Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+");

    public static final PropertyDescriptor STATUS_CODE = new PropertyDescriptor.Builder()
            .name("HTTP Status Code")
            .description("The HTTP Status Code to use when responding to the HTTP Request. See Section 10 of RFC 2616 for more information.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor HTTP_CONTEXT_MAP = new PropertyDescriptor.Builder()
            .name("HTTP Context Map")
            .description("The HTTP Context Map Controller Service to use for caching the HTTP Request Information")
            .required(true)
            .identifiesControllerService(HttpContextMap.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles will be routed to this Relationship after the response has been successfully sent to the requestor")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this Relationship if the Processor is unable to respond to the requestor. This may happen, "
                    + "for instance, if the connection times out or if NiFi is restarted before responding to the HTTP Request.")
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(STATUS_CODE);
        properties.add(HTTP_CONTEXT_MAP);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value to send for the '" + propertyDescriptorName + "' HTTP Header")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        final String contextIdentifier = flowFile.getAttribute(HTTPUtils.HTTP_CONTEXT_ID);
        if (contextIdentifier == null) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().warn("Failed to respond to HTTP request for {} because FlowFile did not have an '" + HTTPUtils.HTTP_CONTEXT_ID + "' attribute",
                    new Object[]{flowFile});
            return;
        }

        final String statusCodeValue = context.getProperty(STATUS_CODE).evaluateAttributeExpressions(flowFile).getValue();
        if (!isNumber(statusCodeValue)) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Failed to respond to HTTP request for {} because status code was '{}', which is not a valid number", new Object[]{flowFile, statusCodeValue});
            return;
        }

        final HttpContextMap contextMap = context.getProperty(HTTP_CONTEXT_MAP).asControllerService(HttpContextMap.class);
        final HttpServletResponse response = contextMap.getResponse(contextIdentifier);
        if (response == null) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Failed to respond to HTTP request for {} because FlowFile had an '{}' attribute of {} but could not find an HTTP Response Object for this identifier",
                    new Object[]{flowFile, HTTPUtils.HTTP_CONTEXT_ID, contextIdentifier});
            return;
        }

        final int statusCode = Integer.parseInt(statusCodeValue);
        response.setStatus(statusCode);

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                final String headerName = descriptor.getName();
                final String headerValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();

                if (!headerValue.trim().isEmpty()) {
                    response.setHeader(headerName, headerValue);
                }
            }
        }

        try {
            session.exportTo(flowFile, response.getOutputStream());
            response.flushBuffer();
        } catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Failed to respond to HTTP request for {} due to {}", new Object[]{flowFile, e});
            contextMap.complete(contextIdentifier);
            return;
        } catch (final Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Failed to respond to HTTP request for {} due to {}", new Object[]{flowFile, e});
            return;
        }

        try {
            contextMap.complete(contextIdentifier);
        } catch (final IllegalStateException ise) {
            getLogger().error("Failed to complete HTTP Transaction for {} due to {}", new Object[]{flowFile, ise});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.getProvenanceReporter().send(flowFile, HTTPUtils.getURI(flowFile.getAttributes()), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);
        getLogger().info("Successfully responded to HTTP Request for {} with status code {}", new Object[]{flowFile, statusCode});
    }

    private static boolean isNumber(final String value) {
        if (value.length() == 0) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }
}
