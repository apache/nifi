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

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayClient;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayClientBuilder;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayRequest;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayRequestBuilder;
import org.apache.nifi.processors.aws.wag.client.GenericApiGatewayResponse;

/**
 * This class is the base class for invoking aws gateway api endpoints
 */
public abstract class AbstractAWSGatewayApiProcessor extends
                                                     AbstractAWSCredentialsProviderProcessor<GenericApiGatewayClient> {

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();
    private volatile Pattern regexAttributesToSend = null;
    private volatile AmazonHttpClient providedClient = null;

    public final static String STATUS_CODE = "aws.gateway.api.status.code";
    public final static String STATUS_MESSAGE = "aws.gateway.api.status.message";
    public final static String RESPONSE_BODY = "aws.gateway.api.response.body";
    public final static String RESOURCE_NAME_ATTR = "aws.gateway.api.resource";
    public final static String ENDPOINT_ATTR = "aws.gateway.api.endpoint";
    public final static String TRANSACTION_ID = "aws.gateway.api.tx.id";
    public final static String EXCEPTION_CLASS = "aws.gateway.api.java.exception.class";
    public final static String EXCEPTION_MESSAGE = "aws.gateway.api.java.exception.message";

    protected static final String REL_RESPONSE_NAME = "Response";
    protected static final String REL_SUCCESS_REQ_NAME = "Original";
    protected static final String REL_RETRY_NAME = "Retry";
    protected static final String REL_NO_RETRY_NAME = "No Retry";
    protected static final String REL_FAILURE_NAME = "Failure";
    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    public AbstractAWSGatewayApiProcessor() {
    }

    public AbstractAWSGatewayApiProcessor(AmazonHttpClient client) {
        providedClient = client;
    }


    // Set of flowfile attributes which we generally always ignore during
    // processing, including when converting http headers, copying attributes, etc.
    // This set includes our strings defined above as well as some standard flowfile
    // attributes.
    public static final Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(
        Arrays.asList(STATUS_CODE, STATUS_MESSAGE, RESOURCE_NAME_ATTR, TRANSACTION_ID, "uuid",
                      "filename", "path")));

    public static final PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
            .name("aws-gateway-http-method")
            .displayName("HTTP Method")
            .description(
                "HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)."
                    + "Methods other than POST, PUT and PATCH will be sent without a message body.")
            .required(true)
            .defaultValue("GET")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators
                    .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_AWS_API_KEY = new PropertyDescriptor.Builder()
            .name("aws-gateway-api-key")
            .displayName("Amazon Gateway Api Key")
            .description("The API Key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_AWS_GATEWAY_API_ENDPOINT = new PropertyDescriptor.Builder()
            .name("aws-gateway-api-endpoint")
            .displayName("Amazon Gateway Api Endpoint")
            .description("The Api Endpoint")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    // we use our own region, because the way the base sets the region after the client is created
    // resets the endpoint and breaks everything
    public static final PropertyDescriptor PROP_AWS_GATEWAY_API_REGION = new PropertyDescriptor.Builder()
            .name("aws-gateway-region")
            .displayName("Amazon Region")
            .required(true)
            .allowableValues(AbstractAWSProcessor.getAvailableRegions())
            .defaultValue(AbstractAWSProcessor.createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    public static final PropertyDescriptor PROP_RESOURCE_NAME = new PropertyDescriptor.Builder()
            .name("aws-gateway-resource")
            .displayName("Amazon Gateway Api ResourceName")
            .description("The Name of the Gateway API Resource")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor PROP_QUERY_PARAMS = new PropertyDescriptor.Builder()
            .name("aws-gateway-query-parameters")
            .displayName("Query Parameters")
            .description(
                "The query parameters for this request in the form of Name=Value separated by &")
            .displayName("Query Parameters")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators
                    .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_ATTRIBUTES_TO_SEND = new PropertyDescriptor.Builder()
            .name("aws-gateway-attributes-to-send")
            .displayName("Attributes to Send")
            .description(
                "Regular expression that defines which attributes to send as HTTP headers in the request. "
                    + "If not defined, no attributes are sent as headers. Also any dynamic properties set will be sent as headers. "
                    + "The dynamic property key will be the header key and the dynamic property value will be interpreted as expression "
                    + "language will be the header value.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PUT_OUTPUT_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("aws-gateway-put-response-body-in-attribute")
            .displayName("Put Response Body In Attribute")
            .description(
                "If set, the response body received back will be put into an attribute of the original FlowFile instead of a separate "
                    + "FlowFile. The attribute key to put to is determined by evaluating value of this property. ")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_RESPONSE_REGARDLESS = new PropertyDescriptor.Builder()
            .name("aws-gateway-always-output-response")
            .displayName("Always Output Response")
            .description(
                "Will force a response FlowFile to be generated and routed to the 'Response' relationship regardless of what the server status code received is "
                    + "or if the processor is configured to put the server response body in the request attribute. In the later configuration a request FlowFile with the "
                    + "response body in the attribute and a typical response FlowFile will be emitted to their respective relationships.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_PENALIZE_NO_RETRY = new PropertyDescriptor.Builder()
            .name("aws-gateway-penalize-no-retry")
            .displayName("Penalize on \"No Retry\"")
            .description("Enabling this property will penalize FlowFiles that are routed to the \"No Retry\" " +
                    "relationship.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("aws-gateway-max-attribute-length")
            .displayName("Max Length To Put In Attribute")
            .description(
                "If routing the response body to an attribute of the original (by setting the \"Put response body in attribute\" "
                    + "property or by receiving an error status code), the number of characters put to the attribute value will be at "
                    + "most this amount. This is important because attributes are held in memory and large attributes will quickly "
                    + "cause out of memory issues. If the output goes longer than this value, it will be truncated to fit. "
                    + "Consider making this smaller if able.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();

    public static final PropertyDescriptor PROP_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("aws-gateway-content-type")
            .displayName("Content-Type")
            .description(
                "The Content-Type to specify for when content is being transmitted through a PUT, POST or PATCH. "
                    + "In the case of an empty value after evaluating an expression language expression, Content-Type defaults to "
                    + DEFAULT_CONTENT_TYPE).required(true).expressionLanguageSupported(true)
            .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
            .addValidator(StandardValidators
                    .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_SEND_BODY = new PropertyDescriptor.Builder()
            .name("aws-gateway-send-message-body")
            .displayName("Send Message Body")
            .description("If true, sends the HTTP message body on POST/PUT/PATCH requests (default).  If false, "
                    + "suppresses the message body and content-type header for these requests.")
            .defaultValue("true")
            .allowableValues("true", "false")
            .required(false)
            .build();

    public static final PropertyDescriptor PROP_ADD_HEADERS_TO_REQUEST = new PropertyDescriptor.Builder()
            .name("aws-gateway-add-response-headers-request")
            .displayName("Add Response Headers To Request")
            .description("Enabling this property saves all the response "
                    + "headers to the original request. This may be when the response headers are needed "
                + "but a response is not generated due to the status code received.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("aws-gateway-connection-timeout")
            .displayName("Connection Timeout")
            .description("Max wait time for connection to remote service.")
            .required(false)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("aws-gateway-read-timeout")
            .displayName("Read Timeout")
            .description("Max wait time for response from remote service.")
            .required(false)
            .defaultValue("50 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
        String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                                                                     AttributeExpression.ResultType.STRING,
                                                                     true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }


    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                   final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        } else {
            // compile the attributes-to-send filter pattern
            if (PROP_ATTRIBUTES_TO_SEND.getName().equalsIgnoreCase(descriptor.getName())) {
                if (newValue == null || newValue.isEmpty()) {
                    regexAttributesToSend = null;
                } else {
                    final String trimmedValue = StringUtils.trimToEmpty(newValue);
                    regexAttributesToSend = Pattern.compile(trimmedValue);
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(
        final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(3);
        results.addAll(super.customValidate(validationContext));
        final boolean querySet = validationContext.getProperty(PROP_QUERY_PARAMS).isSet();

        if (querySet) {
            String input = validationContext.getProperty(PROP_QUERY_PARAMS).getValue();
            // if we have expressions, we don't do further validation
            if (!(validationContext.isExpressionLanguageSupported(PROP_QUERY_PARAMS.getName())
                && validationContext.isExpressionLanguagePresent(input))) {

                try {
                    final String evaluatedInput = validationContext.newPropertyValue(input)
                                                                   .evaluateAttributeExpressions()
                                                                   .getValue();
                    // user is not expected to encode, that will be done by the aws client
                    // but we may need to when validating
                    final String encodedInput = URLEncoder.encode(evaluatedInput, "UTF-8");
                    final String url = String.format("http://www.foo.com?%s", encodedInput);
                    new URL(url);
                    results.add(new ValidationResult.Builder().subject(PROP_QUERY_PARAMS.getName())
                                                              .input(input)
                                                              .explanation("Valid URL params")
                                                              .valid(true).build());
                } catch (final Exception e) {
                    results.add(new ValidationResult.Builder().subject(PROP_QUERY_PARAMS.getName())
                                                              .input(input).explanation(
                            "Not a valid set of URL params").valid(false).build());
                }
            }
        }
        String method = trimToEmpty(validationContext.getProperty(PROP_METHOD).getValue())
            .toUpperCase();

        // if there are expressions do not validate
        if (!(validationContext.isExpressionLanguageSupported(PROP_METHOD.getName())
                && validationContext.isExpressionLanguagePresent(method))) {
            try {
                HttpMethodName methodName = HttpMethodName.fromValue(method);
            } catch (IllegalArgumentException e) {
                results.add(new ValidationResult.Builder().subject(PROP_METHOD.getName()).input(method)
                        .explanation("Unsupported METHOD")
                        .valid(false).build());
            }
        }

        return results;
    }

    @Override
    protected GenericApiGatewayClient createClient(ProcessContext context,
                                                   AWSCredentialsProvider awsCredentialsProvider,
                                                   ClientConfiguration clientConfiguration) {

        GenericApiGatewayClientBuilder builder = new GenericApiGatewayClientBuilder()
            .withCredentials(awsCredentialsProvider).withClientConfiguration(clientConfiguration)
            .withEndpoint(context.getProperty(PROP_AWS_GATEWAY_API_ENDPOINT).getValue()).withRegion(
                Region.getRegion(
                    Regions.fromName(context.getProperty(PROP_AWS_GATEWAY_API_REGION).getValue())));
        if (context.getProperty(PROP_AWS_API_KEY).isSet()) {
            builder = builder.withApiKey(context.getProperty(PROP_AWS_API_KEY).evaluateAttributeExpressions().getValue());
        }
        if (providedClient != null) {
            builder = builder.withHttpClient(providedClient);
        }
        return builder.build();
    }

    @Override
    @Deprecated
    protected GenericApiGatewayClient createClient(final ProcessContext context,
                                                   final AWSCredentials credentials,
                                                   final ClientConfiguration clientConfiguration) {
        return createClient(context, new AWSStaticCredentialsProvider(credentials),
                            clientConfiguration);
    }

    protected GenericApiGatewayRequest configureRequest(final ProcessContext context,
                                                        final ProcessSession session,
                                                        final String resourcePath,
                                                        final FlowFile requestFlowFile) {
        String method = trimToEmpty(
                context.getProperty(PROP_METHOD).evaluateAttributeExpressions(requestFlowFile)
                        .getValue()).toUpperCase();
        HttpMethodName methodName = HttpMethodName.fromValue(method);
        return configureRequest(context, session, resourcePath,requestFlowFile, methodName);
    }

    protected GenericApiGatewayRequest configureRequest(final ProcessContext context,
                                                        final ProcessSession session,
                                                        final String resourcePath,
                                                        final FlowFile requestFlowFile,
                                                        final HttpMethodName methodName) {

        GenericApiGatewayRequestBuilder builder = new GenericApiGatewayRequestBuilder()
            .withResourcePath(resourcePath);
        final Map<String, List<String>> parameters = getParameters(context);
        builder = builder.withParameters(parameters);

        InputStream requestBody = null;
        switch (methodName) {
            case GET:
                builder = builder.withHttpMethod(HttpMethodName.GET);
                break;
            case POST:
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                builder = builder.withHttpMethod(HttpMethodName.POST).withBody(requestBody);
                break;
            case PUT:
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                builder = builder.withHttpMethod(HttpMethodName.PUT).withBody(requestBody);
                break;
            case PATCH:
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                builder = builder.withHttpMethod(HttpMethodName.PATCH).withBody(requestBody);
                break;
            case HEAD:
                builder = builder.withHttpMethod(HttpMethodName.HEAD);
                break;
            case DELETE:
                builder = builder.withHttpMethod(HttpMethodName.DELETE);
                break;
            case OPTIONS:
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                builder = builder.withHttpMethod(HttpMethodName.OPTIONS).withBody(requestBody);
                break;
        }

        builder = setHeaderProperties(context, builder, methodName, requestFlowFile);
        return builder.build();
    }

    protected InputStream getRequestBodyToSend(final ProcessSession session,
                                               final ProcessContext context,
                                               final FlowFile requestFlowFile) {

        if (context.getProperty(PROP_SEND_BODY).asBoolean() && requestFlowFile != null) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            session.exportTo(requestFlowFile, outputStream);
            return new ByteArrayInputStream(outputStream.toByteArray());

        } else {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    protected GenericApiGatewayRequestBuilder setHeaderProperties(final ProcessContext context,
                                                                  GenericApiGatewayRequestBuilder requestBuilder,
                                                                  HttpMethodName methodName,
                                                                  final FlowFile requestFlowFile) {

        Map<String, String> headers = new HashMap<>();
        for (String headerKey : dynamicPropertyNames) {
            String headerValue = context.getProperty(headerKey)
                                        .evaluateAttributeExpressions(requestFlowFile).getValue();
            headers.put(headerKey, headerValue);
        }

        // iterate through the flowfile attributes, adding any attribute that
        // matches the attributes-to-send pattern. if the pattern is not set
        // (it's an optional property), ignore that attribute entirely
        if (regexAttributesToSend != null && requestFlowFile != null) {
            Map<String, String> attributes = requestFlowFile.getAttributes();
            Matcher m = regexAttributesToSend.matcher("");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String headerKey = trimToEmpty(entry.getKey());

                // don't include any of the ignored attributes
                if (IGNORED_ATTRIBUTES.contains(headerKey)) {
                    continue;
                }

                // check if our attribute key matches the pattern
                // if so, include in the request as a header
                m.reset(headerKey);
                if (m.matches()) {
                    String headerVal = trimToEmpty(entry.getValue());
                    headers.put(headerKey, headerVal);
                }
            }
        }

        String contentType = context.getProperty(PROP_CONTENT_TYPE)
                                    .evaluateAttributeExpressions(requestFlowFile).getValue();
        boolean sendBody = context.getProperty(PROP_SEND_BODY).asBoolean();
        contentType = StringUtils.isBlank(contentType) ? DEFAULT_CONTENT_TYPE : contentType;
        if (methodName == HttpMethodName.PUT || methodName == HttpMethodName.POST
            || methodName == HttpMethodName.PATCH) {
            if (sendBody) {
                headers.put("Content-Type", contentType);
            }
        } else {
            headers.put("Content-Type", contentType);
        }

        if (!headers.isEmpty()) {
            requestBuilder = requestBuilder.withHeaders(headers);
        }

        return requestBuilder;
    }

    /**
     * Returns a map of Query Parameter Name to Values
     *
     * @param context ProcessContext
     * @return Map of names and values
     */
    protected Map<String, List<String>> getParameters(ProcessContext context) {

        if (!context.getProperty(PROP_QUERY_PARAMS).isSet()) {
            return new HashMap<>();
        }
        final String queryString = context.getProperty(PROP_QUERY_PARAMS)
                                          .evaluateAttributeExpressions().getValue();
        List<NameValuePair> params = URLEncodedUtils
            .parse(queryString, Charsets.toCharset("UTF-8"));

        if (params.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, List<String>> map = new HashMap<>();

        for (NameValuePair nvp : params) {
            if (!map.containsKey(nvp.getName())) {
                map.put(nvp.getName(), new ArrayList<>());
            }
            map.get(nvp.getName()).add(nvp.getValue());
        }
        return map;
    }

    /**
     * Returns a Map of flowfile attributes from the response http headers. Multivalue headers are naively converted to comma separated strings.
     */
    protected Map<String, String> convertAttributesFromHeaders(
        GenericApiGatewayResponse responseHttp) {
        // create a new hashmap to store the values from the connection
        Map<String, String> map = new HashMap<>();
        responseHttp.getHttpResponse().getHeaders().entrySet().forEach((entry) -> {

            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null) {
                return;
            }

            // we ignore any headers with no actual values (rare)
            if (StringUtils.isBlank(value)) {
                return;
            }

            // put the csv into the map
            map.put(key, value);
        });

        return map;
    }


    protected Relationship getRelationshipForName(String name, Set<Relationship> relationships) {
        for (Relationship relationship : relationships) {
            if (relationship.getName().equals(name)) {
                return relationship;
            }
        }
        throw new IllegalStateException("Unknown relationship " + name);
    }

    protected void route(FlowFile request, FlowFile response, ProcessSession session,
                         ProcessContext context, int statusCode, Set<Relationship> relationships) {
        // check if we should yield the processor
        if (!isSuccess(statusCode) && request == null) {
            context.yield();
        }

        // If the property to output the response flowfile regardless of status code is set then transfer it
        boolean responseSent = false;
        if (context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean()) {
            session.transfer(response, getRelationshipForName(REL_RESPONSE_NAME, relationships));
            responseSent = true;
        }

        // transfer to the correct relationship
        // 2xx -> SUCCESS
        if (isSuccess(statusCode)) {
            // we have two flowfiles to transfer
            if (request != null) {
                session
                    .transfer(request, getRelationshipForName(REL_SUCCESS_REQ_NAME, relationships));
            }
            if (response != null && !responseSent) {
                session
                    .transfer(response, getRelationshipForName(REL_RESPONSE_NAME, relationships));
            }

            // 5xx -> RETRY
        } else if (statusCode / 100 == 5) {
            if (request != null) {
                request = session.penalize(request);
                session.transfer(request, getRelationshipForName(REL_RETRY_NAME, relationships));
            }

            // 1xx, 3xx, 4xx -> NO RETRY
        } else {
            if (request != null) {
                if (context.getProperty(PROP_PENALIZE_NO_RETRY).asBoolean()) {
                    request = session.penalize(request);
                }
                session.transfer(request, getRelationshipForName(REL_NO_RETRY_NAME, relationships));
            }
        }

    }

    protected boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    protected void logRequest(ComponentLog logger, URI endpoint, GenericApiGatewayRequest request) {
        try {
            logger.debug("\nRequest to remote service:\n\t{}\t{}\t\n{}",
                new Object[]{endpoint.toURL().toExternalForm(), request.getHttpMethod(), getLogString(request.getHeaders())});
        } catch (MalformedURLException e) {
            logger.debug(e.getMessage());
        }
    }

    protected void logResponse(ComponentLog logger, GenericApiGatewayResponse response) {
        try {
            logger.debug("\nResponse from remote service:\n\t{}\n{}",
                    new Object[]{response.getHttpResponse().getHttpRequest().getURI().toURL().toExternalForm(), getLogString(response.getHttpResponse().getHeaders())});
        } catch (MalformedURLException e) {
            logger.debug(e.getMessage());
        }
    }

    protected String getLogString(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        if(map != null && map.size() > 0) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String value = entry.getValue();
                sb.append("\t");
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(value);
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
