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

package org.apache.nifi.minifi.bootstrap.configuration.ingestors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;
import static java.nio.ByteBuffer.wrap;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeCoordinator.NOTIFIER_INGESTORS_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.differentiators.WholeConfigDifferentiator.WHOLE_CONFIG_KEY;
import static org.eclipse.jetty.http.HttpScheme.HTTP;
import static org.eclipse.jetty.http.HttpScheme.HTTPS;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.Differentiator;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.WholeConfigDifferentiator;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PullHttpChangeIngestor extends AbstractPullChangeIngestor {

    public static final String PULL_HTTP_BASE_KEY = NOTIFIER_INGESTORS_KEY + ".pull.http";
    public static final String PULL_HTTP_POLLING_PERIOD_KEY = PULL_HTTP_BASE_KEY + ".period.ms";
    public static final String PORT_KEY = PULL_HTTP_BASE_KEY + ".port";
    public static final String HOST_KEY = PULL_HTTP_BASE_KEY + ".hostname";
    public static final String PATH_KEY = PULL_HTTP_BASE_KEY + ".path";
    public static final String QUERY_KEY = PULL_HTTP_BASE_KEY + ".query";
    public static final String PROXY_HOST_KEY = PULL_HTTP_BASE_KEY + ".proxy.hostname";
    public static final String PROXY_PORT_KEY = PULL_HTTP_BASE_KEY + ".proxy.port";
    public static final String PROXY_USERNAME = PULL_HTTP_BASE_KEY + ".proxy.username";
    public static final String PROXY_PASSWORD = PULL_HTTP_BASE_KEY + ".proxy.password";
    public static final String TRUSTSTORE_LOCATION_KEY = PULL_HTTP_BASE_KEY + ".truststore.location";
    public static final String TRUSTSTORE_PASSWORD_KEY = PULL_HTTP_BASE_KEY + ".truststore.password";
    public static final String TRUSTSTORE_TYPE_KEY = PULL_HTTP_BASE_KEY + ".truststore.type";
    public static final String KEYSTORE_LOCATION_KEY = PULL_HTTP_BASE_KEY + ".keystore.location";
    public static final String KEYSTORE_PASSWORD_KEY = PULL_HTTP_BASE_KEY + ".keystore.password";
    public static final String KEYSTORE_TYPE_KEY = PULL_HTTP_BASE_KEY + ".keystore.type";
    public static final String CONNECT_TIMEOUT_KEY = PULL_HTTP_BASE_KEY + ".connect.timeout.ms";
    public static final String READ_TIMEOUT_KEY = PULL_HTTP_BASE_KEY + ".read.timeout.ms";
    public static final String DIFFERENTIATOR_KEY = PULL_HTTP_BASE_KEY + ".differentiator";
    public static final String USE_ETAG_KEY = PULL_HTTP_BASE_KEY + ".use.etag";
    public static final String OVERRIDE_SECURITY = PULL_HTTP_BASE_KEY + ".override.security";
    public static final String HTTP_HEADERS = PULL_HTTP_BASE_KEY + ".headers";

    protected static final String DEFAULT_CONNECT_TIMEOUT_MS = "5000";
    protected static final String DEFAULT_READ_TIMEOUT_MS = "15000";
    protected static final String DEFAULT_PATH = "/";
    private static final Logger logger = LoggerFactory.getLogger(PullHttpChangeIngestor.class);

    private static final Map<String, Supplier<Differentiator<ByteBuffer>>> DIFFERENTIATOR_CONSTRUCTOR_MAP = Map.of(
        WHOLE_CONFIG_KEY, WholeConfigDifferentiator::getByteBufferDifferentiator
    );
    private static final int NOT_MODIFIED_STATUS_CODE = 304;
    private static final String DOUBLE_QUOTES = "\"";
    private static final String ETAG_HEADER = "ETag";
    private static final String PROXY_AUTHORIZATION_HEADER = "Proxy-Authorization";
    private static final int BAD_REQUEST_STATUS_CODE = 400;
    private static final String IF_NONE_MATCH_HEADER_KEY = "If-None-Match";
    private static final String HTTP_HEADERS_SEPARATOR = ",";
    private static final String HTTP_HEADER_KEY_VALUE_SEPARATOR = ":";

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final AtomicReference<Integer> portReference = new AtomicReference<>();
    private final AtomicReference<String> hostReference = new AtomicReference<>();
    private final AtomicReference<String> pathReference = new AtomicReference<>();
    private final AtomicReference<String> queryReference = new AtomicReference<>();
    private final AtomicReference<Map<String, String>> httpHeadersReference = new AtomicReference<>();

    private volatile Differentiator<ByteBuffer> differentiator;
    private volatile String connectionScheme;
    private volatile String lastEtag = "";
    private volatile boolean useEtag = false;

    @Override
    public void initialize(BootstrapProperties properties, ConfigurationFileHolder configurationFileHolder, ConfigurationChangeNotifier configurationChangeNotifier) {
        super.initialize(properties, configurationFileHolder, configurationChangeNotifier);

        pollingPeriodMS.set(Integer.parseInt(properties.getProperty(PULL_HTTP_POLLING_PERIOD_KEY, DEFAULT_POLLING_PERIOD_MILLISECONDS)));
        if (pollingPeriodMS.get() < 1) {
            throw new IllegalArgumentException("Property, " + PULL_HTTP_POLLING_PERIOD_KEY + ", for the polling period ms must be set with a positive integer");
        }

        String host = ofNullable(properties.getProperty(HOST_KEY))
            .filter(StringUtils::isNotBlank)
            .orElseThrow(() -> new IllegalArgumentException("Property, " + HOST_KEY + ", for the hostname to pull configurations from must be specified"));
        String path = properties.getProperty(PATH_KEY, DEFAULT_PATH);
        String query = properties.getProperty(QUERY_KEY, EMPTY);
        Map<String, String> httpHeaders = ofNullable(properties.getProperty(HTTP_HEADERS))
            .filter(StringUtils::isNotBlank)
            .map(headers -> headers.split(HTTP_HEADERS_SEPARATOR))
            .stream()
            .flatMap(Arrays::stream)
            .map(String::trim)
            .map(header -> header.split(HTTP_HEADER_KEY_VALUE_SEPARATOR))
            .filter(split -> split.length == 2)
            .collect(toMap(split -> ofNullable(split[0]).map(String::trim).orElse(EMPTY), split -> ofNullable(split[1]).map(String::trim).orElse(EMPTY)));
        logger.debug("Configured HTTP headers: {}", httpHeaders);

        ofNullable(properties.getProperty(PORT_KEY))
            .map(Integer::parseInt)
            .ifPresentOrElse(
                portReference::set,
                () -> {
                    throw new IllegalArgumentException("Property, " + PORT_KEY + ", for the hostname to pull configurations from must be specified");
                });
        hostReference.set(host);
        pathReference.set(path);
        queryReference.set(query);
        httpHeadersReference.set(httpHeaders);
        useEtag = parseBoolean(properties.getProperty(USE_ETAG_KEY, FALSE.toString()));

        httpClientReference.set(null);

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
            .connectTimeout(parseLong(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT_MS)), MILLISECONDS)
            .readTimeout(parseLong(properties.getProperty(READ_TIMEOUT_KEY, DEFAULT_READ_TIMEOUT_MS)), MILLISECONDS)
            .followRedirects(true);

        String proxyHost = properties.getProperty(PROXY_HOST_KEY);
        if (isNotBlank(proxyHost)) {
            ofNullable(properties.getProperty(PROXY_PORT_KEY))
                .filter(StringUtils::isNotBlank)
                .map(Integer::parseInt)
                .map(port -> new InetSocketAddress(proxyHost, port))
                .map(inetSocketAddress -> new Proxy(Proxy.Type.HTTP, inetSocketAddress))
                .ifPresentOrElse(
                    okHttpClientBuilder::proxy,
                    () -> {
                        throw new IllegalArgumentException("Proxy port required if proxy specified");
                    });

            ofNullable(properties.getProperty(PROXY_USERNAME))
                .filter(StringUtils::isNotBlank)
                .ifPresent(proxyUserName ->
                    ofNullable(properties.getProperty(PROXY_PASSWORD))
                        .map(proxyPassword -> Credentials.basic(proxyUserName, proxyPassword))
                        .map(credentials -> (Authenticator) (route, response) -> response.request().newBuilder().addHeader(PROXY_AUTHORIZATION_HEADER, credentials).build())
                        .ifPresentOrElse(
                            okHttpClientBuilder::proxyAuthenticator,
                            () -> {
                                throw new IllegalArgumentException("Must specify proxy password with proxy username");
                            }));
        }

        if (properties.containsKey(KEYSTORE_LOCATION_KEY)) {
            connectionScheme = HTTPS.toString();
            setSslSocketFactory(okHttpClientBuilder, properties);
        } else {
            connectionScheme = HTTP.toString();
        }

        httpClientReference.set(okHttpClientBuilder.build());
        differentiator = ofNullable(properties.getProperty(DIFFERENTIATOR_KEY))
            .filter(not(String::isBlank))
            .map(differentiator -> ofNullable(DIFFERENTIATOR_CONSTRUCTOR_MAP.get(differentiator))
                .map(Supplier::get)
                .orElseThrow(unableToFindDifferentiatorExceptionSupplier(differentiator)))
            .orElseGet(WholeConfigDifferentiator::getByteBufferDifferentiator);
        differentiator.initialize(configurationFileHolder);
    }

    @Override
    public void run() {
        logger.debug("Attempting to pull new config");
        HttpUrl.Builder builder = new HttpUrl.Builder()
            .host(hostReference.get())
            .port(portReference.get())
            .encodedPath(pathReference.get());
        ofNullable(queryReference.get())
            .filter(StringUtils::isNotBlank)
            .ifPresent(builder::encodedQuery);
        HttpUrl url = builder.scheme(connectionScheme).build();

        Request.Builder requestBuilder = new Request.Builder().get().url(url);
        if (useEtag) {
            requestBuilder.addHeader(IF_NONE_MATCH_HEADER_KEY, lastEtag);
        }
        httpHeadersReference.get().forEach(requestBuilder::addHeader);

        Request request = requestBuilder.build();
        logger.debug("Sending request: {}", request);

        try (Response response = httpClientReference.get().newCall(request).execute()) {
            logger.debug("Response received: {}", response);
            int code = response.code();
            if (code == NOT_MODIFIED_STATUS_CODE) {
                return;
            }
            if (code >= BAD_REQUEST_STATUS_CODE) {
                throw new IOException("Got response code " + code + " while trying to pull configuration: " + response.body().string());
            }

            ResponseBody body = response.body();
            if (body == null) {
                logger.warn("No body returned when pulling a new configuration");
                return;
            }

            ByteBuffer newFlowConfig = wrap(body.bytes()).duplicate();
            if (differentiator.isNew(newFlowConfig)) {
                logger.debug("New change received, notifying listener");
                configurationChangeNotifier.notifyListeners(newFlowConfig);
                logger.debug("Listeners notified");
            } else {
                logger.debug("Pulled config same as currently running");
            }

            if (useEtag) {
                lastEtag = Stream.of(DOUBLE_QUOTES, response.header(ETAG_HEADER).trim(), DOUBLE_QUOTES).collect(joining());
            }
        } catch (Exception e) {
            logger.warn("Hit an exception while trying to pull", e);
        }
    }

    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder, BootstrapProperties properties) {
        String keystorePass = properties.getProperty(KEYSTORE_PASSWORD_KEY);
        KeyStore keyStore = buildKeyStore(properties, KEYSTORE_LOCATION_KEY, KEYSTORE_PASSWORD_KEY, KEYSTORE_TYPE_KEY);
        KeyStore truststore = buildKeyStore(properties, TRUSTSTORE_LOCATION_KEY, TRUSTSTORE_PASSWORD_KEY, TRUSTSTORE_TYPE_KEY);

        SSLSocketFactory sslSocketFactory = new StandardSslContextBuilder()
            .keyStore(keyStore)
            .keyPassword(keystorePass.toCharArray())
            .trustStore(truststore)
            .build()
            .getSocketFactory();
        X509TrustManager trustManager = new StandardTrustManagerBuilder().trustStore(truststore).build();

        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, trustManager);
    }

    private KeyStore buildKeyStore(BootstrapProperties properties, String locationKey, String passKey, String typeKey) {
        String keystoreLocation = ofNullable(properties.getProperty(locationKey))
            .filter(StringUtils::isNotBlank)
            .orElseThrow(() -> new IllegalArgumentException(locationKey + " is null or empty"));
        String keystorePass = ofNullable(properties.getProperty(passKey))
            .filter(StringUtils::isNotBlank)
            .orElseThrow(() -> new IllegalArgumentException(passKey + " is null or empty"));
        String keystoreType = ofNullable(properties.getProperty(typeKey))
            .filter(StringUtils::isNotBlank)
            .orElseThrow(() -> new IllegalArgumentException(typeKey + " is null or empty"));

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            return new StandardKeyStoreBuilder()
                .type(keystoreType)
                .inputStream(keyStoreStream)
                .password(keystorePass.toCharArray())
                .build();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create keyStore", e);
        }
    }

    private Supplier<IllegalArgumentException> unableToFindDifferentiatorExceptionSupplier(String differentiator) {
        return () -> new IllegalArgumentException("Property, " + DIFFERENTIATOR_KEY + ", has value " + differentiator
            + " which does not correspond to any in the FileChangeIngestor Map:" + DIFFERENTIATOR_CONSTRUCTOR_MAP.keySet());
    }

    // Methods exposed only for enable testing
    void setDifferentiator(Differentiator<ByteBuffer> differentiator) {
        this.differentiator = differentiator;
    }

    void setLastEtag(String lastEtag) {
        this.lastEtag = lastEtag;
    }

    void setUseEtag(boolean useEtag) {
        this.useEtag = useEtag;
    }
}
