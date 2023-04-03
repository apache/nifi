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
package org.apache.nifi.c2.client.http;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OkHttpClientProvider {

    private static final Logger logger = LoggerFactory.getLogger(OkHttpClientProvider.class);

    private final C2ClientConfig clientConfig;

    public OkHttpClientProvider(C2ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public OkHttpClient okHttpClient() {
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        // Configure request and response logging
        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(logger::debug);
        logging.setLevel(HttpLoggingInterceptor.Level.BASIC);
        okHttpClientBuilder.addInterceptor(logging);

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(true);
        okHttpClientBuilder.connectionPool(new ConnectionPool(clientConfig.getMaxIdleConnections(), clientConfig.getKeepAliveDuration(), TimeUnit.MILLISECONDS));

        // Timeouts
        okHttpClientBuilder.connectTimeout(clientConfig.getConnectTimeout(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(clientConfig.getReadTimeout(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.callTimeout(clientConfig.getCallTimeout(), TimeUnit.MILLISECONDS);

        // check if the ssl path is set and add the factory if so
        if (StringUtils.isNotBlank(clientConfig.getKeystoreFilename())) {
            try {
                setSslSocketFactory(okHttpClientBuilder);
            } catch (Exception e) {
                throw new IllegalStateException("OkHttp TLS configuration failed", e);
            }
        }

        return okHttpClientBuilder.build();
    }

    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder) throws Exception {
        String keystoreLocation = clientConfig.getKeystoreFilename();
        String keystoreType = clientConfig.getKeystoreType();
        String keystorePass = clientConfig.getKeystorePass();
        assertKeystorePropertiesSet(keystoreLocation, keystorePass, keystoreType);

        KeyStore keyStore;
        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore = new StandardKeyStoreBuilder()
                .type(keystoreType)
                .inputStream(keyStoreStream)
                .password(keystorePass.toCharArray())
                .build();
        }

        String truststoreLocation = clientConfig.getTruststoreFilename();
        String truststorePass = clientConfig.getTruststorePass();
        String truststoreType = clientConfig.getTruststoreType();
        assertTruststorePropertiesSet(truststoreLocation, truststorePass, truststoreType);

        KeyStore truststore;
        try (FileInputStream trustStoreStream = new FileInputStream(truststoreLocation)) {
            truststore = new StandardKeyStoreBuilder()
                .type(truststoreType)
                .inputStream(trustStoreStream)
                .password(truststorePass.toCharArray())
                .build();
        }

        X509TrustManager trustManager = new StandardTrustManagerBuilder().trustStore(truststore).build();
        SSLContext sslContext = new StandardSslContextBuilder()
            .keyStore(keyStore)
            .keyPassword(keystorePass.toCharArray())
            .trustStore(truststore)
            .build();
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, trustManager);
    }

    private void assertKeystorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException(clientConfig.getKeystoreFilename() + " is null or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("The client's keystore filename is set but its password is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("The client's keystore filename is set but its type is not (or is empty). If the location is set, the type must also be.");
        }
    }

    private void assertTruststorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is not set or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is set but its password is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is set but its type is not (or is empty). If the location is set, the type must also be.");
        }
    }
}
