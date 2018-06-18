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

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.validation.RecordPathValidator;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@Tags({ "rest", "lookup", "json", "xml", "http" })
@CapabilityDescription("Use a REST service to look up values.")
@DynamicProperties({
    @DynamicProperty(name = "*", value = "*", description = "All dynamic properties are added as HTTP headers with the name " +
            "as the header name and the value as the header value.")
})
public class RestLookupService extends AbstractControllerService implements RecordLookupService {
    static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
        .name("rest-lookup-url")
        .displayName("URL")
        .description("The URL for the REST endpoint. Expression language is evaluated against the lookup key/value pairs, " +
                "not flowfile attributes.")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("rest-lookup-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for loading the payload and handling it as a record set.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_PATH = new PropertyDescriptor.Builder()
        .name("rest-lookup-record-path")
        .displayName("Record Path")
        .description("An optional record path that can be used to define where in a record to get the real data to merge " +
                "into the record set to be enriched. See documentation for examples of when this might be useful.")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new RecordPathValidator())
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
    public static final PropertyDescriptor PROP_BASIC_AUTH_USERNAME = new PropertyDescriptor.Builder()
        .name("rest-lookup-basic-auth-username")
        .displayName("Basic Authentication Username")
        .description("The username to be used by the client to authenticate against the Remote URL.  Cannot include control characters (0-31), ':', or DEL (127).")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
        .build();

    public static final PropertyDescriptor PROP_BASIC_AUTH_PASSWORD = new PropertyDescriptor.Builder()
        .name("rest-lookup-basic-auth-password")
        .displayName("Basic Authentication Password")
        .description("The password to be used by the client to authenticate against the Remote URL.")
        .required(false)
        .sensitive(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
        .build();
    public static final PropertyDescriptor PROP_DIGEST_AUTH = new PropertyDescriptor.Builder()
        .name("rest-lookup-digest-auth")
        .displayName("Use Digest Authentication")
        .description("Whether to communicate with the website using Digest Authentication. 'Basic Authentication Username' and 'Basic Authentication Password' are used "
                + "for authentication.")
        .required(false)
        .defaultValue("false")
        .allowableValues("true", "false")
        .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    static final String MIME_TYPE_KEY = "mime.type";
    static final String BODY_KEY = "request.body";
    static final String METHOD_KEY = "request.method";

    static final List<PropertyDescriptor> DESCRIPTORS;
    static final Set<String> KEYS;

    static final List VALID_VERBS = Arrays.asList("delete", "get", "post", "put");

    static {
        DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            URL,
            RECORD_READER,
            RECORD_PATH,
            SSL_CONTEXT_SERVICE,
            PROXY_CONFIGURATION_SERVICE,
            PROP_BASIC_AUTH_USERNAME,
            PROP_BASIC_AUTH_PASSWORD,
            PROP_DIGEST_AUTH
        ));
        KEYS = Collections.emptySet();
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile ProxyConfigurationService proxyConfigurationService;
    private volatile RecordReaderFactory readerFactory;
    private volatile RecordPath recordPath;
    private volatile OkHttpClient client;
    private volatile Map<String, String> headers;
    private volatile PropertyValue urlTemplate;
    private volatile String basicUser;
    private volatile String basicPass;
    private volatile boolean isDigest;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        proxyConfigurationService = context.getProperty(PROXY_CONFIGURATION_SERVICE)
                .asControllerService(ProxyConfigurationService.class);

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        setAuthenticator(builder, context);

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

        buildHeaders(context);

        urlTemplate = context.getProperty(URL);
    }

    @OnDisabled
    public void onDisabled() {
        this.recordPath = null;
        this.urlTemplate = null;
    }

    private void buildHeaders(ConfigurationContext context) {
        headers = new HashMap<>();
        for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
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
        return lookup(coordinates, null);
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        final String endpoint = determineEndpoint(coordinates);
        final String mimeType = (String)coordinates.get(MIME_TYPE_KEY);
        final String method   = ((String)coordinates.getOrDefault(METHOD_KEY, "get")).trim().toLowerCase();
        final String body     = (String)coordinates.get(BODY_KEY);

        validateVerb(method);

        if (StringUtils.isBlank(body)) {
            if (method.equals("post") || method.equals("put")) {
                throw new LookupFailureException(
                        String.format("Used HTTP verb %s without specifying the %s key to provide a payload.", method, BODY_KEY)
                );
            }
        } else {
            if (StringUtils.isBlank(mimeType)) {
                throw new LookupFailureException(
                        String.format("Request body is specified without its %s.", MIME_TYPE_KEY)
                );
            }
        }

        Request request = buildRequest(mimeType, method, body, endpoint);
        try {
            Response response = executeRequest(request);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Response code {} was returned for coordinate {}",
                        new Object[]{response.code(), coordinates});
            }

            final ResponseBody responseBody = response.body();
            if (responseBody == null) {
                return Optional.empty();
            }

            InputStream is = responseBody.byteStream();
            Record record = handleResponse(is, context);

            return Optional.ofNullable(record);
        } catch (Exception e) {
            getLogger().error("Could not execute lookup.", e);
            throw new LookupFailureException(e);
        }
    }

    protected void validateVerb(String method) throws LookupFailureException {
        if (!VALID_VERBS.contains(method)) {
            throw new LookupFailureException(String.format("%s is not a supported HTTP verb.", method));
        }
    }

    protected String determineEndpoint(Map<String, Object> coordinates) {
        Map<String, String> converted = coordinates.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toString()
            ));
        return urlTemplate.evaluateAttributeExpressions(converted).getValue();
    }

    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .displayName(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    }

    protected Response executeRequest(Request request) throws IOException {
        return client.newCall(request).execute();
    }

    private Record handleResponse(InputStream is, Map<String, String> context) throws SchemaNotFoundException, MalformedRecordException, IOException {

        try (RecordReader reader = readerFactory.createRecordReader(context, is, getLogger())) {

            Record record = reader.nextRecord();

            if (recordPath != null) {
                Optional<FieldValue> fv = recordPath.evaluate(record).getSelectedFields().findFirst();
                if (fv.isPresent()) {
                    FieldValue fieldValue = fv.get();
                    RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(fieldValue.getField()));

                    Record temp;
                    Object value = fieldValue.getValue();
                    if (value instanceof Record) {
                        temp = (Record) value;
                    } else if (value instanceof Map) {
                        temp = new MapRecord(schema, (Map<String, Object>) value);
                    } else {
                        Map<String, Object> val = new HashMap<>();
                        val.put(fieldValue.getField().getFieldName(), value);
                        temp = new MapRecord(schema, val);
                    }

                    record = temp;
                } else {
                    record = null;
                }
            }

            return record;
        } catch (Exception ex) {
            is.close();
            throw ex;
        }
    }

    private Request buildRequest(final String mimeType, final String method, final String body, final String endpoint) {
        RequestBody requestBody = null;
        if (body != null) {
            final MediaType mt = MediaType.parse(mimeType);
            requestBody = RequestBody.create(mt, body);
        }
        Request.Builder request = new Request.Builder()
                .url(endpoint);
        switch(method) {
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

        if (!basicUser.isEmpty() && !isDigest) {
            String credential = Credentials.basic(basicUser, basicPass);
            request = request.header("Authorization", credential);
        }

        return request.build();
    }

    private void setAuthenticator(OkHttpClient.Builder okHttpClientBuilder, ConfigurationContext context) {
        final String authUser = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).evaluateAttributeExpressions().getValue());
        this.basicUser = authUser;


        isDigest = context.getProperty(PROP_DIGEST_AUTH).asBoolean();
        final String authPass = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_PASSWORD).evaluateAttributeExpressions().getValue());
        this.basicPass = authPass;
        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && isDigest) {

            /*
             * OkHttp doesn't have built-in Digest Auth Support. A ticket for adding it is here[1] but they authors decided instead to rely on a 3rd party lib.
             *
             * [1] https://github.com/square/okhttp/issues/205#issuecomment-154047052
             */
            final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(authUser, authPass);
            final DigestAuthenticator digestAuthenticator = new DigestAuthenticator(credentials);

            okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
            okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return KEYS;
    }
}
