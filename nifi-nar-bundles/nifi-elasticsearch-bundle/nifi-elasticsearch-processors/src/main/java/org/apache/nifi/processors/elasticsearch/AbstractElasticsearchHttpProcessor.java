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
package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Route;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A base class for Elasticsearch processors that use the HTTP API
 */
public abstract class AbstractElasticsearchHttpProcessor extends AbstractElasticsearchProcessor {

    static final String FIELD_INCLUDE_QUERY_PARAM = "_source_include";
    static final String QUERY_QUERY_PARAM = "q";
    static final String SORT_QUERY_PARAM = "sort";
    static final String SIZE_QUERY_PARAM = "size";


    public static final PropertyDescriptor ES_URL = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-url")
            .displayName("Elasticsearch URL")
            .description("Elasticsearch URL which will be connected to, including scheme (http, e.g.), host, and port. The default port for the REST API is 9200.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-host")
            .displayName("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-port")
            .displayName("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("proxy-username")
            .displayName("Proxy Username")
            .description("Proxy Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-password")
            .displayName("Proxy Password")
            .description("Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-connect-timeout")
            .displayName("Connection Timeout")
            .description("Max wait time for the connection to the Elasticsearch REST API.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-response-timeout")
            .displayName("Response Timeout")
            .description("Max wait time for a response from the Elasticsearch REST API.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);
    static final List<PropertyDescriptor> COMMON_PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ES_URL);
        properties.add(PROP_SSL_CONTEXT_SERVICE);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(CONNECT_TIMEOUT);
        properties.add(RESPONSE_TIMEOUT);
        properties.add(PROXY_CONFIGURATION_SERVICE);
        properties.add(PROXY_HOST);
        properties.add(PROXY_PORT);
        properties.add(PROXY_USERNAME);
        properties.add(PROXY_PASSWORD);

        COMMON_PROPERTY_DESCRIPTORS = Collections.unmodifiableList(properties);
    }

    @Override
    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();

        // Add a proxy if set
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
            final Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();
            if (proxyHost != null && proxyPort != null) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                componentProxyConfig.setProxyUserName(context.getProperty(PROXY_USERNAME).evaluateAttributeExpressions().getValue());
                componentProxyConfig.setProxyUserPassword(context.getProperty(PROXY_PASSWORD).evaluateAttributeExpressions().getValue());
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (!Proxy.Type.DIRECT.equals(proxyConfig.getProxyType())) {
            final Proxy proxy = proxyConfig.createProxy();
            okHttpClient.proxy(proxy);

            if (proxyConfig.hasCredential()){
                okHttpClient.proxyAuthenticator(new Authenticator() {
                    @Override
                    public Request authenticate(Route route, Response response) throws IOException {
                        final String credential=Credentials.basic(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    }
                });
            }
        }


        // Set timeouts
        okHttpClient.connectTimeout((context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClient.readTimeout(context.getProperty(RESPONSE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);

        final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createSSLContext(SSLContextService.ClientAuth.NONE);

        // check if the ssl context is set and add the factory if so
        if (sslContext != null) {
            okHttpClient.sslSocketFactory(sslContext.getSocketFactory());
        }

        okHttpClientAtomicReference.set(okHttpClient.build());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        if(validationContext.getProperty(PROXY_HOST).isSet() != validationContext.getProperty(PROXY_PORT).isSet()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .explanation("Proxy Host and Proxy Port must be both set or empty")
                    .subject("Proxy server configuration")
                    .build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, results, PROXY_SPECS);

        return results;
    }

    protected OkHttpClient getClient() {
        return okHttpClientAtomicReference.get();
    }

    protected boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    protected Response sendRequestToElasticsearch(OkHttpClient client, URL url, String username, String password, String verb, RequestBody body) throws IOException {

        final ComponentLog log = getLogger();
        Request.Builder requestBuilder = new Request.Builder()
                .url(url);
        if ("get".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.get();
        } else if ("put".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.put(body);
        } else if ("post".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.post(body);
        } else {
            throw new IllegalArgumentException("Elasticsearch REST API verb not supported by this processor: " + verb);
        }

        if(!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            String credential = Credentials.basic(username, password);
            requestBuilder = requestBuilder.header("Authorization", credential);
        }
        Request httpRequest = requestBuilder.build();
        log.debug("Sending Elasticsearch request to {}", new Object[]{url});

        Response responseHttp = client.newCall(httpRequest).execute();

        // store the status code and message
        int statusCode = responseHttp.code();

        if (statusCode == 0) {
            throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
        }

        log.debug("Received response from Elasticsearch with status code {}", new Object[]{statusCode});

        return responseHttp;
    }

    protected JsonNode parseJsonResponse(InputStream in) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(in);
    }

    protected void buildBulkCommand(StringBuilder sb, String index, String docType, String indexOp, String id, String jsonString) {
        if (indexOp.equalsIgnoreCase("index")) {
            sb.append("{\"index\": { \"_index\": \"");
            sb.append(StringEscapeUtils.escapeJson(index));
            sb.append("\", \"_type\": \"");
            sb.append(StringEscapeUtils.escapeJson(docType));
            sb.append("\"");
            if (!StringUtils.isEmpty(id)) {
                sb.append(", \"_id\": \"");
                sb.append(StringEscapeUtils.escapeJson(id));
                sb.append("\"");
            }
            sb.append("}}\n");
            sb.append(jsonString);
            sb.append("\n");
        } else if (indexOp.equalsIgnoreCase("upsert") || indexOp.equalsIgnoreCase("update")) {
            sb.append("{\"update\": { \"_index\": \"");
            sb.append(StringEscapeUtils.escapeJson(index));
            sb.append("\", \"_type\": \"");
            sb.append(StringEscapeUtils.escapeJson(docType));
            sb.append("\", \"_id\": \"");
            sb.append(StringEscapeUtils.escapeJson(id));
            sb.append("\" }\n");
            sb.append("{\"doc\": ");
            sb.append(jsonString);
            sb.append(", \"doc_as_upsert\": ");
            sb.append(indexOp.equalsIgnoreCase("upsert"));
            sb.append(" }\n");
        } else if (indexOp.equalsIgnoreCase("delete")) {
            sb.append("{\"delete\": { \"_index\": \"");
            sb.append(StringEscapeUtils.escapeJson(index));
            sb.append("\", \"_type\": \"");
            sb.append(StringEscapeUtils.escapeJson(docType));
            sb.append("\", \"_id\": \"");
            sb.append(StringEscapeUtils.escapeJson(id));
            sb.append("\" }\n");
        }
    }
}
