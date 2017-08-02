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

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
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
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-host")
            .displayName("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-port")
            .displayName("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-connect-timeout")
            .displayName("Connection Timeout")
            .description("Max wait time for the connection to the Elasticsearch REST API.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-response-timeout")
            .displayName("Response Timeout")
            .description("Max wait time for a response from the Elasticsearch REST API.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();

        // Add a proxy if set
        final String proxyHost = context.getProperty(PROXY_HOST).getValue();
        final Integer proxyPort = context.getProperty(PROXY_PORT).asInteger();
        if (proxyHost != null && proxyPort != null) {
            final Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
            okHttpClient.proxy(proxy);
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
                    .build());
        }
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
}
