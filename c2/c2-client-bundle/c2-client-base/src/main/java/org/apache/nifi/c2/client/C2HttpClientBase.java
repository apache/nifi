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
package org.apache.nifi.c2.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.c2.client.api.C2Properties;
import org.apache.nifi.c2.client.api.FlowUpdateInfo;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.c2.client.api.C2Properties.C2_PROPERTY_BASE;

public abstract class C2HttpClientBase extends C2ClientBase {

    private static final Logger logger = LoggerFactory.getLogger(C2HttpClientBase.class);

    public static final String TRUSTSTORE_LOCATION_KEY = C2_PROPERTY_BASE + ".truststore.location";
    public static final String TRUSTSTORE_PASSWORD_KEY = C2_PROPERTY_BASE + ".truststore.password";
    public static final String TRUSTSTORE_TYPE_KEY = C2_PROPERTY_BASE + ".truststore.type";
    public static final String KEYSTORE_LOCATION_KEY = C2_PROPERTY_BASE + ".keystore.location";
    public static final String KEYSTORE_PASSWORD_KEY = C2_PROPERTY_BASE + ".keystore.password";
    public static final String KEYSTORE_TYPE_KEY = C2_PROPERTY_BASE + ".keystore.type";

    protected AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final C2Properties c2Properties;
    private final String c2Url;
    private final String c2AckUrl;

    public C2HttpClientBase(Properties properties) {
        super();
        this.c2Properties = new C2Properties(properties);
        this.c2Url = c2Properties.getRestUrl();
        this.c2AckUrl = c2Properties.getRestAckUrl();
        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(true);

        // check if the ssl path is set and add the factory if so
        if (StringUtils.isNotBlank(c2Properties.getKeystore())) {
            try {
                setSslSocketFactory(okHttpClientBuilder, properties);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        httpClientReference.set(okHttpClientBuilder.build());
    }

    @Override
    protected C2HeartbeatResponse sendHeartbeat(final String heartbeatString) {
        final RequestBody requestBody = RequestBody.create(heartbeatString, MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON));
        logger.debug("Performing request to {}", c2Url);
        final Request.Builder requestBuilder = new Request.Builder()
                .post(requestBody)
                .url(c2Url);

        try {
            final Response heartbeatResponse = httpClientReference.get().newCall(requestBuilder.build()).execute();
            int statusCode = heartbeatResponse.code();
            final String responseBody = heartbeatResponse.body().string();
            logger.debug("Received heartbeat response (Status={}) {}", statusCode, responseBody);
            ObjectMapper objMapper = new ObjectMapper();
            // Create C2HeartbeatResponse, populate it, and return it
            C2HeartbeatResponse c2HeartbeatResponse = objMapper.readValue(responseBody, C2HeartbeatResponse.class);

            List<C2Operation> requestedOperations = c2HeartbeatResponse.getRequestedOperations();

            return c2HeartbeatResponse;
        } catch (ConnectException ce) {
            logger.error("Connection error while sending heartbeat", ce);
            return null;
        } catch (IOException e) {
            logger.error("Could not transmit", e);
            throw new RuntimeException(e);
        }
    }

    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder, Properties properties) throws Exception {
        final String keystoreLocation = properties.getProperty(C2Properties.KEYSTORE_LOCATION_KEY);
        final String keystoreType = properties.getProperty(C2Properties.KEYSTORE_TYPE_KEY);
        final String keystorePass = properties.getProperty(C2Properties.KEYSTORE_PASSWORD_KEY);

        assertKeystorePropertiesSet(keystoreLocation, keystorePass, keystoreType);

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = properties.getProperty(C2Properties.TRUSTSTORE_LOCATION_KEY);
        final String truststorePass = properties.getProperty(C2Properties.TRUSTSTORE_PASSWORD_KEY);
        final String truststoreType = properties.getProperty(C2Properties.TRUSTSTORE_TYPE_KEY);
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

    @Override
    public ByteBuffer retrieveUpdateContent(FlowUpdateInfo flowUpdateInfo) {
        final Request.Builder requestBuilder = new Request.Builder()
                .get()
                .url(flowUpdateInfo.getFlowUpdateUrl());
        final Request request = requestBuilder.build();

        ResponseBody body;
        try (final Response response = httpClientReference.get().newCall(request).execute()) {
            logger.debug("Response received: {}", response);

            int code = response.code();
            if (code >= 400) {
                throw new IOException("Got response code " + code + " while trying to pull configuration: " + response.body().string());
            }

            body = response.body();

            if (body == null) {
                logger.warn("No body returned when pulling a new configuration");
                return null;
            }

            final ByteBuffer bodyByteBuffer = ByteBuffer.wrap(body.bytes());
            return bodyByteBuffer;

        } catch (Exception e) {
            logger.warn("Hit an exception while trying to pull", e);
            return null;
        }
    }

    @Override
    public void acknowledgeOperation(C2OperationAck operationAck) {
        logger.info("Performing acknowledgement request to {} for operation {}", c2AckUrl, operationAck.getOperationId());
        final ObjectMapper jacksonObjectMapper = new ObjectMapper();
        jacksonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            final String operationAckBody = jacksonObjectMapper.writeValueAsString(operationAck);

            final RequestBody requestBody = RequestBody.create(operationAckBody, MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON));
            final Request.Builder requestBuilder = new Request.Builder()
                    .post(requestBody)
                    .url(c2AckUrl);
            final Response heartbeatResponse = httpClientReference.get().newCall(requestBuilder.build()).execute();
            if (!heartbeatResponse.isSuccessful()) {
                logger.warn("Acknowledgement was not successful.");
            }
            logger.trace("Status on acknowledgement was {}", heartbeatResponse.code());
        } catch (Exception e) {
            logger.error("Could not transmit ack to c2 server", e);
        }
    }
}
