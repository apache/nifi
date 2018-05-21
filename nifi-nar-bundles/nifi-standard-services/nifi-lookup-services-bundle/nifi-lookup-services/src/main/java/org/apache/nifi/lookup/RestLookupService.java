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

package org.apache.nifi.lookup;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({ "rest", "lookup", "json", "xml", "http" })
@CapabilityDescription("Use a REST service to enrich records.")
public class RestLookupService extends AbstractControllerService implements LookupService<Record> {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("rest-lookup-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for loading the payload and handling it as a record set.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .identifiesControllerService(RecordReaderFactory.class)
        .addValidator(Validator.VALID)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_PATH = new PropertyDescriptor.Builder()
        .name("rest-lookup-record-path")
        .displayName("Record Path")
        .description("An optional record path that can be used to define where in a record to get the real data to merge " +
                "into the record set to be enriched. See documentation for examples of when this might be useful.")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("rest-lookup-ssl-context-service")
        .displayName("SSL Context Service")
        .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                + "connections.")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    static final String ENDPOINT_KEY = "endpoint";
    static final String MIME_TYPE_KEY = "mime.type";
    static final String BODY_KEY = "request.body";
    static final String METHOD_KEY = "request.method";

    static final List<PropertyDescriptor> DESCRIPTORS;

    static {
        DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            RECORD_PATH,
            SSL_CONTEXT_SERVICE,
            PROXY_CONFIGURATION_SERVICE
        ));
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private ProxyConfigurationService proxyConfigurationService;
    private RecordReaderFactory readerFactory;
    private RecordPath recordPath;
    private OkHttpClient client;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        proxyConfigurationService = context.getProperty(PROXY_CONFIGURATION_SERVICE)
                .asControllerService(ProxyConfigurationService.class);

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (proxyConfigurationService != null) {
            setProxy(builder);
        }

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createSSLContext(SSLContextService.ClientAuth.WANT);
        if (sslService != null) {
            builder.sslSocketFactory(sslContext.getSocketFactory());
        }

        client = builder.build();

        String path = context.getProperty(RECORD_PATH).isSet() ? context.getProperty(RECORD_PATH).getValue() : null;
        if (!StringUtils.isBlank(path)) {
            recordPath = RecordPath.compile(path);
        }

        getHeaders(context);
    }

    private Map<String, String> headers;
    private void getHeaders(ConfigurationContext context) {
        headers = new HashMap<>();
        for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.getName().startsWith("header.")) {
                headers.put(
                    descriptor.getDisplayName(),
                    context.getProperty(descriptor).evaluateAttributeExpressions().getValue()
                );
            }
        }
    }

    private void setProxy(OkHttpClient.Builder builder) {
        ProxyConfiguration config = proxyConfigurationService.getConfiguration();
        if (!config.getProxyType().equals(Proxy.Type.DIRECT)) {
            final Proxy proxy = config.createProxy();
            builder.proxy(proxy);

            if (config.hasCredential()){
                builder.proxyAuthenticator((route, response) -> {
                    final String credential= Credentials.basic(config.getProxyUserName(), config.getProxyUserPassword());
                    return response.request().newBuilder()
                            .header("Proxy-Authorization", credential)
                            .build();
                });
            }
        }
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        final String endpoint = (String)coordinates.get(ENDPOINT_KEY);
        final String mimeType = (String)coordinates.get(MIME_TYPE_KEY);
        final String method   = (String)coordinates.get(METHOD_KEY);
        final String body     = (String)coordinates.get(BODY_KEY);

        if (StringUtils.isBlank(body) && (method.equals("post") || method.equals("put"))) {
            throw new LookupFailureException(
                String.format("Used HTTP verb %s without specifying the %s key to provide a payload.", method, BODY_KEY)
            );
        }

        Request request = buildRequest(mimeType, method, body, endpoint);
        try {
            Response response = executeRequest(request);
            InputStream is = response.body().byteStream();

            Record record = handleResponse(is, coordinates);

            return Optional.of(record);
        } catch (MalformedRecordException | SchemaNotFoundException | IOException e) {
            getLogger().error("Could not execute lookup.", e);
            throw new LookupFailureException(e);
        }
    }

    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith("header")) {
            String header = propertyDescriptorName.substring(propertyDescriptorName.indexOf(".") + 1, propertyDescriptorName.length());
            return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(header)
                .addValidator(Validator.VALID)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
        }

        return null;
    }

    protected Response executeRequest(Request request) throws IOException {
        return client.newCall(request).execute();
    }

    private Record handleResponse(InputStream is, Map<String, Object> coordinates) throws SchemaNotFoundException, MalformedRecordException, IOException {
        Map<String, String> variables = coordinates.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toString()
            ));
        RecordReader reader = readerFactory.createRecordReader(variables, is, getLogger());
        Record record = reader.nextRecord();

        if (recordPath != null) {
            Optional<FieldValue> fv = recordPath.evaluate(record).getSelectedFields().findFirst();
            if (fv.isPresent()) {
                FieldValue fieldValue = fv.get();
                RecordSchema schema = new SimpleRecordSchema(Arrays.asList(fieldValue.getField()));
                String[] parts = recordPath.getPath().split("/");
                String last = parts[parts.length - 1];

                Record temp;
                Object value = fieldValue.getValue();
                if (value instanceof Record) {
                    temp = (Record)value;
                } else if (value instanceof Map) {
                    temp = new MapRecord(schema, (Map<String, Object>)value);
                } else {
                    temp = new MapRecord(schema, new HashMap<String, Object>(){{
                        put(last, value);
                    }});
                }

                record = temp;
            }
        }

        reader.close();

        return record;
    }

    private Request buildRequest(final String mimeType, final String method, final String body, final String endpoint) {
        final MediaType mt = MediaType.parse(mimeType);
        RequestBody requestBody = null;
        if (body != null) {
            requestBody = RequestBody.create(mt, body);
        }
        Request.Builder request = new Request.Builder()
                .url(endpoint);
        switch(method.toLowerCase()) {
            case "delete":
                request = body != null ? request.delete(requestBody) : request.delete();
                break;
            case "get":
                request = request.get();
                break;
            case "post":
                request = request.post(requestBody);
                break;
            case "put":
                request = request.put(requestBody);
                break;
        }

        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                request = request.addHeader(header.getKey(), header.getValue());
            }
        }

        return request.build();
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return new HashSet<String>(){{
            add(ENDPOINT_KEY);
            add(MIME_TYPE_KEY);
            add(METHOD_KEY);
        }};
    }
}
