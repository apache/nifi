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

package org.apache.nifi.processors.zendesk;

import static com.fasterxml.jackson.core.JsonEncoding.UTF8;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_FORBIDDEN;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_LONG_VALIDATOR;
import static org.apache.nifi.processors.zendesk.GetZendesk.RECORD_COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.web.client.api.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(INPUT_FORBIDDEN)
@DefaultSettings(yieldDuration = "20 sec")
@Tags({"zendesk"})
@CapabilityDescription("Incrementally fetches data from Zendesk API.")
@Stateful(scopes = CLUSTER, description = "Paging cursor for Zendesk API is stored. Cursor is updated after each successful request.")
@WritesAttributes({
    @WritesAttribute(attribute = RECORD_COUNT_ATTRIBUTE_NAME, description = "The number of records fetched by the processor.")})
public class GetZendesk extends AbstractProcessor {

    static final int HTTP_TOO_MANY_REQUESTS = 429;
    static final String RECORD_COUNT_ATTRIBUTE_NAME = "record.count";

    static final String REL_SUCCESS_NAME = "success";
    static final String WEB_CLIENT_SERVICE_PROVIDER_NAME = "web-client-service-provider";
    static final String ZENDESK_SUBDOMAIN_NAME = "zendesk-subdomain";
    static final String ZENDESK_USER_NAME = "zendesk-user";
    static final String ZENDESK_AUTHENTICATION_TYPE_NAME = "zendesk-authentication-type-name";
    static final String ZENDESK_AUTHENTICATION_CREDENTIAL_NAME = "zendesk-authentication-value-name";
    static final String ZENDESK_EXPORT_METHOD_NAME = "zendesk-export-method";
    static final String ZENDESK_RESOURCE_NAME = "zendesk-resource";
    static final String ZENDESK_QUERY_START_TIMESTAMP_NAME = "zendesk-query-start-timestamp";

    private static final String HTTPS = "https";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String BASIC_AUTH_PREFIX = "Basic ";
    private static final String ZENDESK_HOST_TEMPLATE = "%s.zendesk.com";

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name(REL_SUCCESS_NAME)
        .description("For FlowFiles created as a result of a successful HTTP request.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = singleton(REL_SUCCESS);

    private static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
        .name(WEB_CLIENT_SERVICE_PROVIDER_NAME)
        .displayName("Web Client Service Provider")
        .description("Controller service for HTTP client operations.")
        .identifiesControllerService(WebClientServiceProvider.class)
        .required(true)
        .build();

    private static final PropertyDescriptor ZENDESK_SUBDOMAIN = new PropertyDescriptor.Builder()
        .name(ZENDESK_SUBDOMAIN_NAME)
        .displayName("Zendesk Subdomain Name")
        .description("Name of the Zendesk subdomain.")
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(NON_BLANK_VALIDATOR)
        .build();

    private static final PropertyDescriptor ZENDESK_USER = new PropertyDescriptor.Builder()
        .name(ZENDESK_USER_NAME)
        .displayName("Zendesk User Name")
        .description("Login user to Zendesk subdomain.")
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(NON_BLANK_VALIDATOR)
        .build();

    private static final PropertyDescriptor ZENDESK_AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
        .name(ZENDESK_AUTHENTICATION_TYPE_NAME)
        .displayName("Zendesk Authentication Type")
        .description("Type of authentication to Zendesk API.")
        .required(true)
        .allowableValues(ZendeskAuthenticationType.class)
        .build();

    private static final PropertyDescriptor ZENDESK_AUTHENTICATION_CREDENTIAL = new PropertyDescriptor.Builder()
        .name(ZENDESK_AUTHENTICATION_CREDENTIAL_NAME)
        .displayName("Zendesk Authentication Credential")
        .description("Password or authentication token for Zendesk login user.")
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .sensitive(true)
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .build();

    private static final PropertyDescriptor ZENDESK_EXPORT_METHOD = new PropertyDescriptor.Builder()
        .name(ZENDESK_EXPORT_METHOD_NAME)
        .displayName("Zendesk Export Method")
        .description("Method for incremental export.")
        .required(true)
        .allowableValues(ZendeskExportMethod.class)
        .build();

    private static final PropertyDescriptor ZENDESK_RESOURCE = new PropertyDescriptor.Builder()
        .name(ZENDESK_RESOURCE_NAME)
        .displayName("Zendesk Resource")
        .description("The particular Zendesk resource which is meant to be exported.")
        .required(true)
        .allowableValues(ZendeskResource.class)
        .build();

    private static final PropertyDescriptor ZENDESK_QUERY_START_TIMESTAMP = new PropertyDescriptor.Builder()
        .name(ZENDESK_QUERY_START_TIMESTAMP_NAME)
        .displayName("Zendesk Query Start Timestamp")
        .description("Initial timestamp to query Zendesk API from in Unix timestamp seconds format.")
        .addValidator(POSITIVE_LONG_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Stream.of(
        WEB_CLIENT_SERVICE_PROVIDER,
        ZENDESK_SUBDOMAIN,
        ZENDESK_USER,
        ZENDESK_AUTHENTICATION_TYPE,
        ZENDESK_AUTHENTICATION_CREDENTIAL,
        ZENDESK_EXPORT_METHOD,
        ZENDESK_RESOURCE,
        ZENDESK_QUERY_START_TIMESTAMP
    ).collect(collectingAndThen(toList(), Collections::unmodifiableList));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

    private volatile WebClientServiceProvider webClientServiceProvider;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(1);

        ZendeskExportMethod exportMethod = ZendeskExportMethod.forName(validationContext.getProperty(ZENDESK_EXPORT_METHOD).getValue());
        ZendeskResource zendeskResource = ZendeskResource.forName(validationContext.getProperty(ZENDESK_RESOURCE).getValue());
        if (!zendeskResource.supportsExportMethod(exportMethod)) {
            results.add(new ValidationResult.Builder()
                .subject(ZENDESK_EXPORT_METHOD_NAME)
                .valid(false)
                .explanation("Not supported export method for resource.")
                .build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        ZendeskResource zendeskResource = ZendeskResource.forName(context.getProperty(ZENDESK_RESOURCE).getValue());
        ZendeskExportMethod exportMethod = ZendeskExportMethod.forName(context.getProperty(ZENDESK_EXPORT_METHOD).getValue());

        URI uri = createUri(context, zendeskResource, exportMethod);
        HttpResponseEntity response = performQuery(context, uri);

        if (response.statusCode() == OK.getCode()) {
            AtomicInteger resultCount = new AtomicInteger(0);
            FlowFile createdFlowFile = session.write(
                session.create(),
                httpResponseParser(context, response, zendeskResource, exportMethod, resultCount));
            int recordCount = resultCount.get();
            if (recordCount > 0) {
                FlowFile updatedFlowFile = session.putAttribute(createdFlowFile, RECORD_COUNT_ATTRIBUTE_NAME, Integer.toString(recordCount));
                session.getProvenanceReporter().receive(updatedFlowFile, uri.toString());
                session.transfer(updatedFlowFile, REL_SUCCESS);
            } else {
                session.remove(createdFlowFile);
            }
        } else if (response.statusCode() == HTTP_TOO_MANY_REQUESTS) {
            getLogger().error("Rate limit exceeded for uri={}, yielding before retrying request.", uri);
            context.yield();
        } else {
            getLogger().error("HTTP {} error for uri={} with response={}, yielding before retrying request.", response.statusCode(), uri, responseBodyToString(context, response));
            context.yield();
        }
    }

    private URI createUri(ProcessContext context, ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) {
        String subDomain = context.getProperty(ZENDESK_SUBDOMAIN).evaluateAttributeExpressions().getValue();
        String resourcePath = zendeskResource.apiPath(exportMethod);
        HttpUriBuilder uriBuilder = uriBuilder(subDomain, resourcePath);

        String cursor = getCursorState(context, zendeskResource, exportMethod);
        if (cursor == null) {
            String queryStartTimestamp = context.getProperty(ZENDESK_QUERY_START_TIMESTAMP).evaluateAttributeExpressions().getValue();
            uriBuilder.addQueryParameter(exportMethod.getInitialCursorQueryParameterName(), queryStartTimestamp);
        } else {
            uriBuilder.addQueryParameter(exportMethod.getCursorQueryParameterName(), cursor);
        }
        return uriBuilder.build();
    }

    HttpUriBuilder uriBuilder(String subDomain, String resourcePath) {
        return webClientServiceProvider.getHttpUriBuilder()
            .scheme(HTTPS)
            .host(format(ZENDESK_HOST_TEMPLATE, subDomain))
            .encodedPath(resourcePath);
    }

    private String getCursorState(ProcessContext context, ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) {
        try {
            return context.getStateManager().getState(CLUSTER).get(zendeskResource.getValue() + exportMethod.getValue());
        } catch (IOException e) {
            throw new ProcessException("Failed to retrieve cursor state", e);
        }
    }

    private HttpResponseEntity performQuery(ProcessContext context, URI uri) {
        String userName = context.getProperty(ZENDESK_USER).evaluateAttributeExpressions().getValue();
        ZendeskAuthenticationType authenticationType = ZendeskAuthenticationType.forName(context.getProperty(ZENDESK_AUTHENTICATION_TYPE).getValue());
        String authenticationCredential = context.getProperty(ZENDESK_AUTHENTICATION_CREDENTIAL).evaluateAttributeExpressions().getValue();

        return webClientServiceProvider.getWebClientService()
            .get()
            .uri(uri)
            .header(AUTHORIZATION_HEADER_NAME, basicAuthHeaderValue(authenticationType.enrichUserName(userName), authenticationCredential))
            .retrieve();
    }

    private String basicAuthHeaderValue(String user, String credential) {
        String userWithPassword = user + ":" + credential;
        return BASIC_AUTH_PREFIX + getEncoder().encodeToString(userWithPassword.getBytes());
    }

    private OutputStreamCallback httpResponseParser(ProcessContext context, HttpResponseEntity response,
                                                    ZendeskResource zendeskResource, ZendeskExportMethod exportMethod,
                                                    AtomicInteger resultCount) {
        return out -> {
            try (JsonParser parser = JSON_FACTORY.createParser(response.body());
                 JsonGenerator generator = JSON_FACTORY.createGenerator(out, UTF8)) {
                while (parser.nextToken() != null) {
                    if (parser.getCurrentToken() == FIELD_NAME) {
                        String fieldName = parser.getCurrentName();
                        parser.nextToken();
                        if (zendeskResource.getResponseFieldName().equals(fieldName)) {
                            int numberOfExtractedRecords = extractZendeskResourceData(parser, generator);
                            resultCount.addAndGet(numberOfExtractedRecords);
                        }
                        if (exportMethod.getCursorJsonFieldName().equals(fieldName) && parser.currentToken() != VALUE_NULL) {
                            updateCursorState(context, zendeskResource, exportMethod, parser.getText());
                        }
                    }
                }
            }
        };
    }

    private int extractZendeskResourceData(JsonParser parser, JsonGenerator generator) throws IOException {
        ArrayNode zendeskItems = OBJECT_MAPPER.readTree(parser);
        if (zendeskItems.size() > 0) {
            generator.writeStartArray();
            for (JsonNode zendeskItem : zendeskItems) {
                generator.writeTree(zendeskItem);
            }
            generator.writeEndArray();
        }
        return zendeskItems.size();
    }

    private void updateCursorState(ProcessContext context, ZendeskResource zendeskResource, ZendeskExportMethod exportMethod, String cursor) {
        try {
            context.getStateManager().setState(singletonMap(zendeskResource.getValue() + exportMethod.getValue(), cursor), CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cursor state", e);
        }
    }

    private String responseBodyToString(ProcessContext context, HttpResponseEntity response) {
        try {
            return IOUtils.toString(response.body(), UTF_8);
        } catch (IOException e) {
            context.yield();
            throw new UncheckedIOException("Reading response body has failed", e);
        }
    }
}
