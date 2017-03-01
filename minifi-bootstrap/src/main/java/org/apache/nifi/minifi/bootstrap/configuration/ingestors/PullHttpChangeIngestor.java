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

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.WholeConfigDifferentiator;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.interfaces.Differentiator;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeCoordinator.NOTIFIER_INGESTORS_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.differentiators.WholeConfigDifferentiator.WHOLE_CONFIG_KEY;


public class PullHttpChangeIngestor extends AbstractPullChangeIngestor {

    private static final int NOT_MODIFIED_STATUS_CODE = 304;
    private static final Map<String, Supplier<Differentiator<ByteBuffer>>> DIFFERENTIATOR_CONSTRUCTOR_MAP;

    static {
        HashMap<String, Supplier<Differentiator<ByteBuffer>>> tempMap = new HashMap<>();
        tempMap.put(WHOLE_CONFIG_KEY, WholeConfigDifferentiator::getByteBufferDifferentiator);

        DIFFERENTIATOR_CONSTRUCTOR_MAP = Collections.unmodifiableMap(tempMap);
    }

    private static final String DEFAULT_CONNECT_TIMEOUT_MS = "5000";
    private static final String DEFAULT_READ_TIMEOUT_MS = "15000";

    private static final String PULL_HTTP_BASE_KEY = NOTIFIER_INGESTORS_KEY + ".pull.http";
    public static final String PULL_HTTP_POLLING_PERIOD_KEY = PULL_HTTP_BASE_KEY + ".period.ms";
    public static final String PORT_KEY = PULL_HTTP_BASE_KEY + ".port";
    public static final String HOST_KEY = PULL_HTTP_BASE_KEY + ".hostname";
    public static final String PATH_KEY = PULL_HTTP_BASE_KEY + ".path";
    public static final String QUERY_KEY = PULL_HTTP_BASE_KEY + ".query";
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

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final AtomicReference<Integer> portReference = new AtomicReference<>();
    private final AtomicReference<String> hostReference = new AtomicReference<>();
    private final AtomicReference<String> pathReference = new AtomicReference<>();
    private final AtomicReference<String> queryReference = new AtomicReference<>();
    private volatile Differentiator<ByteBuffer> differentiator;
    private volatile String connectionScheme;
    private volatile String lastEtag = "";
    private volatile boolean useEtag = false;

    public PullHttpChangeIngestor() {
        logger = LoggerFactory.getLogger(PullHttpChangeIngestor.class);
    }

    @Override
    public void initialize(Properties properties, ConfigurationFileHolder configurationFileHolder, ConfigurationChangeNotifier configurationChangeNotifier) {
        super.initialize(properties, configurationFileHolder, configurationChangeNotifier);

        pollingPeriodMS.set(Integer.parseInt(properties.getProperty(PULL_HTTP_POLLING_PERIOD_KEY, DEFAULT_POLLING_PERIOD)));
        if (pollingPeriodMS.get() < 1) {
            throw new IllegalArgumentException("Property, " + PULL_HTTP_POLLING_PERIOD_KEY + ", for the polling period ms must be set with a positive integer.");
        }

        final String host = properties.getProperty(HOST_KEY);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Property, " + HOST_KEY + ", for the hostname to pull configurations from must be specified.");
        }

        final String path = properties.getProperty(PATH_KEY, "/");
        final String query = properties.getProperty(QUERY_KEY, "");

        final String portString = (String) properties.get(PORT_KEY);
        final Integer port;
        if (portString == null) {
            throw new IllegalArgumentException("Property, " + PORT_KEY + ", for the hostname to pull configurations from must be specified.");
        } else {
            port = Integer.parseInt(portString);
        }

        portReference.set(port);
        hostReference.set(host);
        pathReference.set(path);
        queryReference.set(query);

        final String useEtagString = (String) properties.getOrDefault(USE_ETAG_KEY, "false");
        if ("true".equalsIgnoreCase(useEtagString) || "false".equalsIgnoreCase(useEtagString)){
            useEtag = Boolean.parseBoolean(useEtagString);
        } else {
            throw new IllegalArgumentException("Property, " + USE_ETAG_KEY + ", to specify whether to use the ETag header, must either be a value boolean value (\"true\" or \"false\") or left to " +
                    "the default value of \"false\". It is set to \"" + useEtagString + "\".");
        }

        httpClientReference.set(null);

        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        // Set timeouts
        okHttpClientBuilder.connectTimeout(Long.parseLong(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT_MS)), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(Long.parseLong(properties.getProperty(READ_TIMEOUT_KEY, DEFAULT_READ_TIMEOUT_MS)), TimeUnit.MILLISECONDS);

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(true);

        // check if the ssl path is set and add the factory if so
        if (properties.containsKey(KEYSTORE_LOCATION_KEY)) {
            try {
                setSslSocketFactory(okHttpClientBuilder, properties);
                connectionScheme = "https";
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            connectionScheme = "http";
        }

        httpClientReference.set(okHttpClientBuilder.build());
        final String differentiatorName = properties.getProperty(DIFFERENTIATOR_KEY);

        if (differentiatorName != null && !differentiatorName.isEmpty()) {
            Supplier<Differentiator<ByteBuffer>> differentiatorSupplier = DIFFERENTIATOR_CONSTRUCTOR_MAP.get(differentiatorName);
            if (differentiatorSupplier == null) {
                throw new IllegalArgumentException("Property, " + DIFFERENTIATOR_KEY + ", has value " + differentiatorName + " which does not " +
                        "correspond to any in the PullHttpChangeIngestor Map:" + DIFFERENTIATOR_CONSTRUCTOR_MAP.keySet());
            }
            differentiator = differentiatorSupplier.get();
        } else {
            differentiator = WholeConfigDifferentiator.getByteBufferDifferentiator();
        }
        differentiator.initialize(properties, configurationFileHolder);
    }


    @Override
    public void run() {
        try {
            logger.debug("Attempting to pull new config");
            HttpUrl.Builder builder = new HttpUrl.Builder()
                    .host(hostReference.get())
                    .port(portReference.get())
                    .encodedPath(pathReference.get());
            String query = queryReference.get();
            if (!StringUtil.isNullOrEmpty(query)) {
                builder = builder.encodedQuery(query);
            }
            final HttpUrl url = builder
                    .scheme(connectionScheme)
                    .build();


            final Request.Builder requestBuilder = new Request.Builder()
                    .get()
                    .url(url);

            if (useEtag) {
                requestBuilder.addHeader("If-None-Match", lastEtag);
            }

            final Request request = requestBuilder.build();

            final OkHttpClient httpClient = httpClientReference.get();

            final Call call = httpClient.newCall(request);
            final Response response = call.execute();

            logger.debug("Response received: {}", response.toString());

            if (response.code() == NOT_MODIFIED_STATUS_CODE) {
                return;
            }

            ResponseBody body = response.body();
            if (body == null) {
                logger.warn("No body returned when pulling a new configuration");
                return;
            }

            ByteBuffer bodyByteBuffer = ByteBuffer.wrap(body.bytes());

            if (differentiator.isNew(bodyByteBuffer)) {
                logger.debug("New change, notifying listener");

                ByteBuffer readOnlyNewConfig = bodyByteBuffer.asReadOnlyBuffer();

                configurationChangeNotifier.notifyListeners(readOnlyNewConfig);
                logger.debug("Listeners notified");
            } else {
                logger.debug("Pulled config same as currently running.");
            }

            if (useEtag) {
                lastEtag = (new StringBuilder("\""))
                        .append(response.header("ETag").trim())
                        .append("\"").toString();
            }
        } catch (Exception e) {
            logger.warn("Hit an exception while trying to pull", e);
        }
    }

    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder, Properties properties) throws Exception {
        final String keystoreLocation = properties.getProperty(KEYSTORE_LOCATION_KEY);
        final String keystorePass = properties.getProperty(KEYSTORE_PASSWORD_KEY);
        final String keystoreType = properties.getProperty(KEYSTORE_TYPE_KEY);

        assertKeystorePropertiesSet(keystoreLocation, keystorePass, keystoreType);

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = properties.getProperty(TRUSTSTORE_LOCATION_KEY);
        final String truststorePass = properties.getProperty(TRUSTSTORE_PASSWORD_KEY);
        final String truststoreType = properties.getProperty(TRUSTSTORE_TYPE_KEY);
        assertTruststorePropertiesSet(truststoreLocation, truststorePass, truststoreType);

        KeyStore truststore = KeyStore.getInstance(truststoreType);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        truststore.load(new FileInputStream(truststoreLocation), truststorePass.toCharArray());
        trustManagerFactory.init(truststore);

        final X509TrustManager x509TrustManager;
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers[0] != null) {
            x509TrustManager = (X509TrustManager) trustManagers[0];
        } else {
            throw new IllegalStateException("List of trust managers is null");
        }

        SSLContext tempSslContext;
        try {
            tempSslContext = SSLContext.getInstance("TLS");
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Unable to use 'TLS' for the PullHttpChangeIngestor due to NoSuchAlgorithmException. Will attempt to use the default algorithm.", e);
            tempSslContext = SSLContext.getDefault();
        }

        final SSLContext sslContext = tempSslContext;
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);
    }

    private void assertKeystorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException(KEYSTORE_LOCATION_KEY + " is null or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException(KEYSTORE_LOCATION_KEY + " is set but " + KEYSTORE_PASSWORD_KEY + " is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException(KEYSTORE_LOCATION_KEY + " is set but " + KEYSTORE_TYPE_KEY + " is not (or is empty). If the location is set, the type must also be.");
        }
    }

    private void assertTruststorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException(TRUSTSTORE_LOCATION_KEY + " is not set or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException(TRUSTSTORE_LOCATION_KEY + " is set but " + TRUSTSTORE_PASSWORD_KEY + " is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException(TRUSTSTORE_LOCATION_KEY + " is set but " + TRUSTSTORE_TYPE_KEY + " is not (or is empty). If the location is set, the type must also be.");
        }
    }

    protected void setDifferentiator(Differentiator<ByteBuffer> differentiator) {
        this.differentiator = differentiator;
    }

    public void setLastEtag(String lastEtag) {
        this.lastEtag = lastEtag;
    }

    public void setUseEtag(boolean useEtag) {
        this.useEtag = useEtag;
    }
}
