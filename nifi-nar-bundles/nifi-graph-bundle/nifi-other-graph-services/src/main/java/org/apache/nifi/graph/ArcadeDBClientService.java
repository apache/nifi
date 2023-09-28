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
package org.apache.nifi.graph;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.regex.Pattern;

@CapabilityDescription("A client service that connects to ArcadeDB database.")
@Tags({"graph", "database", "arcadedb",})
public class ArcadeDBClientService extends AbstractControllerService implements GraphClientService {

    public static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("API URL")
            .description("HTTP API URL including a scheme of http or https, as well as a hostname or IP address with optional port and path elements, for example 'http://localhost:2480/api/v1'")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations.")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    public static final PropertyDescriptor REQUEST_USERNAME = new PropertyDescriptor.Builder()
            .name("basic-authentication-username")
            .displayName("Request Username")
            .description("The username provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
            .build();

    public static final PropertyDescriptor REQUEST_PASSWORD = new PropertyDescriptor.Builder()
            .name("basic-authentication-password")
            .displayName("Request Password")
            .description("The password provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
            .build();
    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("Database name")
            .description("The name of the database the query should be invoked on.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_LANGUAGE = new PropertyDescriptor.Builder()
            .name("query-language")
            .displayName("Query language")
            .description("Query language to use with ArcadeDB.")
            .required(true)
            .defaultValue("gremlin")
            .allowableValues("sql", "cypher", "gremlin")
            .build();

    private static final String NOT_SUPPORTED = "NOT_SUPPORTED";
    private static final String RESULT_TOKEN = "result";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private WebClientServiceProvider webClientServiceProvider;
    private URI uri;
    private String apiUrl;
    private String databaseName;
    private String userName;
    private String password;
    private String language;
    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Arrays.asList(
            API_URL,
            WEB_CLIENT_SERVICE_PROVIDER,
            REQUEST_USERNAME,
            REQUEST_PASSWORD,
            DATABASE_NAME,
            QUERY_LANGUAGE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        apiUrl = context.getProperty(API_URL).evaluateAttributeExpressions().getValue();
        databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        userName = context.getProperty(REQUEST_USERNAME).getValue();
        password = context.getProperty(REQUEST_PASSWORD).getValue();
        language = context.getProperty(QUERY_LANGUAGE).getValue();
        uri = getUri();
    }

    @Override
    public Map<String, String> executeQuery(final String query, final Map<String, Object> parameters, final GraphQueryResultCallback handler) {
        getLogger().info("Executing Query:\n" + query);
        final ArcadeDbRequestBody body = new ArcadeDbRequestBody(language, query, parameters);
        final HttpResponseEntity httpResponseEntity = getHttpResponseEntity(body);

        final int responseStatusCode = httpResponseEntity.statusCode();
        // TODO be more specific with error codes? Improve response message?
        if (responseStatusCode < 200 || responseStatusCode >= 300) {
            String response = "";
            try {
                response = IOUtils.toString(httpResponseEntity.body(), StandardCharsets.UTF_8);
                getLogger().error("Error returned in response: " + response);
            } catch (IOException ioe) {
                getLogger().error("Error reading body of response", ioe);
            }
            throw new ProcessException("Query execution failed with status code " + responseStatusCode + " and error message " + response);
        }

        try (final JsonParser jsonParser = MAPPER.getFactory().createParser(httpResponseEntity.body())) {
            long count = 0;
            JsonToken jsonToken = jsonParser.nextToken();
            while ( jsonToken != null && jsonToken != JsonToken.END_OBJECT) {
                final String token = jsonParser.getCurrentName();
                if (RESULT_TOKEN.equals(token)) {
                    jsonParser.nextToken();
                    if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                        jsonParser.nextToken();
                        while (jsonParser.currentToken() != JsonToken.END_ARRAY) {
                            final String result = jsonParser.readValueAsTree().toString();
                            jsonParser.nextToken();
                            handler.process(new HashMap<String, Object>() {{
                                put(RESULT_TOKEN, result);
                            }}, jsonParser.currentToken() != JsonToken.END_ARRAY);
                            count++;
                        }
                    }
                }
                jsonToken = jsonParser.nextToken();
            }

            final Map<String, String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
            resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
            resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
            resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (IOException e) {
            throw new ProcessException("Failed to process request " + body.getCommand(), e);
        }
    }

    private HttpResponseEntity getHttpResponseEntity(final ArcadeDbRequestBody body) {
        final String valueToEncode = String.format("%s:%s", userName, password);
        final String credential = "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());

        try (InputStream inputStream = new ByteArrayInputStream(MAPPER.writeValueAsBytes(body))) {
            return webClientServiceProvider.getWebClientService()
                    .post()
                    .uri(uri)
                    .header("Authorization", credential)
                    .header("Content-Type", "application/json")
                    .body(inputStream, OptionalLong.of(inputStream.available()))
                    .retrieve();

        } catch (IOException e) {
            throw new ProcessException("Failed to execute query " + body.getCommand(), e);
        }
    }

    private URI getUri() {
        try {
            return new URI(normalizeURL(apiUrl + "/command/" + databaseName));
        } catch (URISyntaxException e) {
            throw new ProcessException("Invalid url", e);
        }
    }

    @Override
    public String getTransitUrl() {
        return uri.toString();
    }

    @Override
    public List<GraphQuery> buildQueryFromNodes(final List<Map<String, Object>> nodeList, final Map<String, Object> parameters) {
        // Build queries from event list
        if (GraphClientService.GREMLIN.equals(language)) {
            return new GremlinQueryFromNodesBuilder().getQueries(nodeList);
        } else if (GraphClientService.SQL.equals(language)) {
            return new SqlQueryFromNodesBuilder().getQueries(nodeList);
        } else if (GraphClientService.CYPHER.equals(language)) {
            return new CypherQueryFromNodesBuilder().getQueries(nodeList);
        }
        return Collections.emptyList();
    }

    private String normalizeURL(final String url) {
        return url.replaceAll("(?<!http:|https:)/+/", "/");
    }

    private static class ArcadeDbRequestBody {
        private final String language;
        private final String command;
        private final Map<String, Object> params;

        public ArcadeDbRequestBody(final String language, final String command, final Map<String, Object> params) {
            this.language = language;
            this.command = command;
            this.params = params;
        }

        public String getLanguage() {
            return language;
        }

        public String getCommand() {
            return command;
        }

        public Map<String, Object> getParams() {
            return params;
        }
    }
}
