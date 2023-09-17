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
package org.apache.nifi.processors.aws.wag;

import com.amazonaws.http.AmazonHttpClient;
import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayClient;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayException;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayRequest;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayResponse;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"Amazon", "AWS", "Client", "Gateway-API", "Rest", "http", "https"})
@CapabilityDescription("Client for AWS Gateway API endpoint")
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type of the flowfiles"),
    @WritesAttribute(attribute = "aws.gateway.api.status.code", description = "The status code that is returned"),
    @WritesAttribute(attribute = "aws.gateway.api.status.message", description = "The status message that is returned"),
    @WritesAttribute(attribute = "aws.gateway.api.response.body", description = "In the instance where the status code received is not a success (2xx)"),
    @WritesAttribute(attribute = "aws.gateway.api.resource", description = "The request resource"),
    @WritesAttribute(attribute = "aws.gateway.api.tx.id", description = "The transaction ID that is returned after reading the response"),
    @WritesAttribute(attribute = "aws.gateway.api.java.exception.class", description = "The Java exception class raised when the processor fails"),
    @WritesAttribute(attribute = "aws.gateway.api.java.exception.message", description = "The Java exception message raised when the processor fails"),})
@DynamicProperty(name = "Header Name", value = "Attribute Expression Language", supportsExpressionLanguage = true, description =
    "Send request header "
        + "with a key matching the Dynamic Property Key and a value created by evaluating the Attribute Expression Language set in the value "
        + "of the Dynamic Property.")
public class InvokeAWSGatewayApi extends AbstractAWSGatewayApiProcessor {

    private static final Set<String> IDEMPOTENT_METHODS = new HashSet<>(Arrays.asList("GET", "HEAD", "OPTIONS"));

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays
            .asList(
                    PROP_METHOD,
                    PROP_AWS_GATEWAY_API_REGION,
                    ACCESS_KEY,
                    SECRET_KEY,
                    CREDENTIALS_FILE,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
                    TIMEOUT,
                    PROP_RESOURCE_NAME,
                    PROP_AWS_GATEWAY_API_ENDPOINT,
                    PROP_AWS_API_KEY,
                    PROP_ATTRIBUTES_TO_SEND,
                    PROP_PUT_OUTPUT_IN_ATTRIBUTE,
                    PROP_CONTENT_TYPE,
                    PROP_SEND_BODY,
                    PROP_OUTPUT_RESPONSE_REGARDLESS,
                    PROP_PENALIZE_NO_RETRY,
                    PROXY_HOST,
                    PROXY_HOST_PORT,
                    PROXY_USERNAME,
                    PROXY_PASSWORD,
                    PROP_QUERY_PARAMS,
                    PROP_PUT_ATTRIBUTE_MAX_LENGTH,
                    PROP_ADD_HEADERS_TO_REQUEST,
                    PROXY_CONFIGURATION_SERVICE));


    public static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
            .name(REL_SUCCESS_REQ_NAME)
            .description("The original FlowFile will be routed upon success (2xx status codes). It will have new  "
                    + "attributes detailing the success of the request.")
            .build();

    public static final Relationship REL_RESPONSE = new Relationship.Builder()
            .name(REL_RESPONSE_NAME)
            .description("A Response FlowFile will be routed upon success (2xx status codes). If the 'Output Response "
                    + "Regardless' property is true then the response will be sent to this relationship regardless of "
                    + "the status code received.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name(REL_RETRY_NAME)
            .description("The original FlowFile will be routed on any status code that can be retried "
                    + "(5xx status codes). It will have new attributes detailing the request.")
            .build();

    public static final Relationship REL_NO_RETRY = new Relationship.Builder()
            .name(REL_NO_RETRY_NAME)
            .description("The original FlowFile will be routed on any status code that should NOT be retried "
                    + "(1xx, 3xx, 4xx status codes).  It will have new attributes detailing the request.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name(REL_FAILURE_NAME)
            .description("The original FlowFile will be routed on any type of connection failure, timeout or general "
                    + "exception. It will have new attributes detailing the request.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(
        Arrays.asList(REL_SUCCESS_REQ, REL_RESPONSE, REL_RETRY, REL_NO_RETRY, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    public InvokeAWSGatewayApi() {
        super();
    }

    public InvokeAWSGatewayApi(AmazonHttpClient client) {
        super(client);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        FlowFile requestFlowFile = session.get();

        // Checking to see if the property to put the body of the response in an attribute was set
        boolean putToAttribute = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).isSet();
        if (requestFlowFile == null) {
            String request = context.getProperty(PROP_METHOD).evaluateAttributeExpressions()
                                    .getValue().toUpperCase();
            if ("POST".equals(request) || "PUT".equals(request) || "PATCH".equals(request)) {
                return;
            } else if (putToAttribute) {
                requestFlowFile = session.create();
            }
        }

        // Every request/response cycle has a unique transaction id which will be stored as a flowfile attribute.
        final UUID txId = UUID.randomUUID();
        FlowFile responseFlowFile = null;

        try {
            final int maxAttributeSize = context.getProperty(PROP_PUT_ATTRIBUTE_MAX_LENGTH)
                                                .asInteger();

            final String resourceName = context.getProperty(PROP_RESOURCE_NAME).getValue();

            final GenericApiGatewayClient client = getClient(context);

            final long startNanos = System.nanoTime();
            final Map<String, String> attributes = requestFlowFile == null ? Collections.emptyMap() : requestFlowFile.getAttributes();
            final GatewayResponse gatewayResponse = invokeGateway(client, context, session, requestFlowFile, attributes, logger);

            final GenericApiGatewayResponse response = gatewayResponse.response;
            final GenericApiGatewayException exception = gatewayResponse.exception;
            final int statusCode = gatewayResponse.statusCode;

            final String endpoint = context.getProperty(PROP_AWS_GATEWAY_API_ENDPOINT).getValue();
            final boolean outputRegardless = context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS)
                                              .asBoolean();

            boolean outputBodyToResponseContent = (isSuccess(statusCode) && !putToAttribute || outputRegardless);
            boolean outputBodyToRequestAttribute = (!isSuccess(statusCode) || putToAttribute) && requestFlowFile != null;
            boolean bodyExists = response != null && response.getBody() != null;

            final String statusExplanation;
            if (exception != null) {
                statusExplanation = EnglishReasonPhraseCatalog.INSTANCE.getReason(statusCode, null);
            } else {
                statusExplanation = response.getHttpResponse().getStatusText();
            }

            // Create a map of the status attributes that are always written to the request and response FlowFiles
            final Map<String, String> statusAttributes = new HashMap<>();
            statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
            statusAttributes.put(STATUS_MESSAGE, statusExplanation);
            statusAttributes.put(ENDPOINT_ATTR, client.getEndpointPrefix());
            statusAttributes.put(RESOURCE_NAME_ATTR, resourceName);
            statusAttributes.put(TRANSACTION_ID, txId.toString());

            if (outputBodyToResponseContent) {
                /*
                 * If successful and putting to response flowfile, store the response body as the flowfile payload
                 * we include additional flowfile attributes including the response headers and the status codes.
                 */

                // clone the flowfile to capture the response
                if (requestFlowFile != null) {
                    responseFlowFile = session.create(requestFlowFile);
                    // write attributes to request flowfile
                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
                    // If the property to add the response headers to the request flowfile is true then add them
                    if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean()) {
                        // write the response headers as attributes
                        // this will overwrite any existing flowfile attributes
                        requestFlowFile = session.putAllAttributes(requestFlowFile, convertAttributesFromHeaders(response));
                    }
                } else {
                    responseFlowFile = session.create();
                }

                // write attributes to response flowfile
                responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);

                // write the response headers as attributes
                // this will overwrite any existing flowfile attributes
                if (response != null) {
                    responseFlowFile = session
                        .putAllAttributes(responseFlowFile, convertAttributesFromHeaders(response));
                } else {
                    responseFlowFile = session
                        .putAllAttributes(responseFlowFile, exception.getHttpHeaders());
                }
                // transfer the message body to the payload
                // can potentially be null in edge cases
                if (bodyExists) {
                    final String contentType = response.getHttpResponse().getHeaders()
                                                       .get("Content-Type");
                    if (!(contentType == null) && !contentType.trim().isEmpty()) {
                        responseFlowFile = session
                            .putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(),
                                          contentType.trim());
                    }

                    responseFlowFile = session
                        .importFrom(new ByteArrayInputStream(response.getBody().getBytes()),
                                    responseFlowFile);

                    // emit provenance event
                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    if (requestFlowFile != null) {
                        session.getProvenanceReporter().fetch(responseFlowFile, endpoint, millis);
                    } else {
                        session.getProvenanceReporter().receive(responseFlowFile, endpoint, millis);
                    }
                } else if (exception != null) {
                    final String contentType = "application/json";
                    responseFlowFile = session
                        .putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(),
                                      contentType.trim());

                    responseFlowFile = session
                        .importFrom(new ByteArrayInputStream(exception.getRawResponse()),
                                    responseFlowFile);

                    // emit provenance event
                    final long millis = TimeUnit.NANOSECONDS
                        .toMillis(System.nanoTime() - startNanos);
                    if (requestFlowFile != null) {
                        session.getProvenanceReporter().fetch(responseFlowFile, endpoint, millis);
                    } else {
                        session.getProvenanceReporter().receive(responseFlowFile, endpoint, millis);
                    }
                }
            }
            // if not successful and request flowfile is not null, store the response body into a flowfile attribute
            if (outputBodyToRequestAttribute) {
                String attributeKey = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE)
                                             .evaluateAttributeExpressions(requestFlowFile)
                                             .getValue();
                if (attributeKey == null) {
                    attributeKey = RESPONSE_BODY;
                }
                final byte[] outputBuffer;
                int size = 0;
                outputBuffer = new byte[maxAttributeSize];
                if (bodyExists) {
                    size = StreamUtils
                        .fillBuffer(new ByteArrayInputStream(response.getBody().getBytes()),
                                    outputBuffer, false);
                } else if (exception != null && exception.getRawResponse() != null
                    && exception.getRawResponse().length > 0) {
                    size = StreamUtils
                        .fillBuffer(new ByteArrayInputStream(exception.getRawResponse()),
                                    outputBuffer, false);
                }

                if (size > 0) {
                    String bodyString = new String(outputBuffer, 0, size, "UTF-8");
                    requestFlowFile = session
                        .putAttribute(requestFlowFile, attributeKey, bodyString);
                }

                requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().modifyAttributes(requestFlowFile, String
                        .format("The %s has been added. The value of which is the body of a http call to %s%s. It took %s millis,", attributeKey, endpoint, resourceName, millis));
            }

            route(requestFlowFile, responseFlowFile, session, context, statusCode,
                  getRelationships());
        } catch (final Exception e) {
            // penalize or yield
            if (requestFlowFile != null) {
                logger.error("Routing to {} due to exception: {}", REL_FAILURE.getName(), e, e);
                requestFlowFile = session.penalize(requestFlowFile);
                requestFlowFile = session
                    .putAttribute(requestFlowFile, EXCEPTION_CLASS, e.getClass().getName());
                requestFlowFile = session
                    .putAttribute(requestFlowFile, EXCEPTION_MESSAGE, e.getMessage());
                // transfer original to failure
                session.transfer(requestFlowFile,
                                 getRelationshipForName(REL_FAILURE_NAME, getRelationships()));
            } else {
                logger.error("Yielding processor due to exception encountered as a source processor: {}", e);
                context.yield();
            }

            // cleanup response flowfile, if applicable
            try {
                if (responseFlowFile != null) {
                    session.remove(responseFlowFile);
                }
            } catch (final Exception e1) {
                logger.error("Could not cleanup response flowfile due to exception: {}", e1, e1);
            }
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(super.verify(context, verificationLogger, attributes));

        final String method = context.getProperty(PROP_METHOD).getValue();

        if (!IDEMPOTENT_METHODS.contains(method)) {
            return results;
        }

        final String endpoint = context.getProperty(PROP_AWS_GATEWAY_API_ENDPOINT).getValue();
        final String resource = context.getProperty(PROP_RESOURCE_NAME).getValue();
        try {
            final GenericApiGatewayClient client = getClient(context);

            final GatewayResponse gatewayResponse = invokeGateway(client, context, null, null, attributes, verificationLogger);

            final String explanation;
            if (gatewayResponse.exception != null) {
                final String statusExplanation = EnglishReasonPhraseCatalog.INSTANCE.getReason(gatewayResponse.statusCode, null);
                explanation = String.format("Successfully invoked AWS Gateway API [%s %s/%s] with blank request body, receiving error response [%s] with status code [%s]",
                        method, endpoint, resource, statusExplanation, gatewayResponse.statusCode);
            } else {
                final String statusExplanation = gatewayResponse.response.getHttpResponse().getStatusText();
                explanation = String.format("Successfully invoked AWS Gateway API [%s %s%/s] with blank request body, receiving success response [%s] with status code [%s]",
                        method, endpoint, resource, statusExplanation, gatewayResponse.statusCode);
            }
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.SUCCESSFUL)
                    .verificationStepName("Invoke AWS Gateway API")
                    .explanation(explanation)
                    .build());

        } catch (final Exception e) {
            verificationLogger.error("Failed to invoke AWS Gateway API " + endpoint, e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.FAILED)
                    .verificationStepName("Invoke AWS Gateway API")
                    .explanation(String.format("Failed to invoke AWS Gateway API [%s %s/%s]: %s", method, endpoint, resource, e.getMessage()))
                    .build());
        }

        return results;
    }

    private GatewayResponse invokeGateway(final GenericApiGatewayClient client, final ProcessContext context, final ProcessSession session,
                                          final FlowFile requestFlowFile, final Map<String, String> attributes, final ComponentLog logger) {
        final String resourceName = context.getProperty(PROP_RESOURCE_NAME).getValue();

        final GenericApiGatewayRequest request = configureRequest(context, session, resourceName, requestFlowFile, attributes);

        logRequest(logger, client.getEndpoint(), request);
        GenericApiGatewayResponse response = null;
        GenericApiGatewayException exception = null;
        try {
            response = client.execute(request);
            logResponse(logger, response);
        } catch (final GenericApiGatewayException gag) {
            // ERROR response codes may come back as exceptions, 404 for example
            exception = gag;
        }

        final int statusCode;
        if (exception != null) {
            statusCode = exception.getStatusCode();
        } else {
            statusCode = response.getHttpResponse().getStatusCode();
        }

        if (statusCode == 0) {
            throw new IllegalStateException(
                    "Status code unknown, connection hasn't been attempted.");
        }
        return new GatewayResponse(response, exception, statusCode);
    }

    private class GatewayResponse {
        private final GenericApiGatewayResponse response;
        private final GenericApiGatewayException exception;
        private final int statusCode;

        private GatewayResponse(final GenericApiGatewayResponse response, final GenericApiGatewayException exception, final int statusCode) {
            this.response = response;
            this.exception = exception;
            this.statusCode = statusCode;
        }
    }
}
