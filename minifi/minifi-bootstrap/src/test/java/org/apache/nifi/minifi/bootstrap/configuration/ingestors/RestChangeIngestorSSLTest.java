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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ListenerHandleResult;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.RestChangeIngestorCommonTest;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Properties;

import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_BASE_KEY;
import static org.mockito.Mockito.when;

public class RestChangeIngestorSSLTest extends RestChangeIngestorCommonTest {

    @BeforeAll
    public static void setUpHttps() throws IOException, InterruptedException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().trustStoreType("JKS").build();

        Properties properties = new Properties();
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_LOCATION_KEY, tlsConfiguration.getTruststorePath());
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_PASSWORD_KEY, tlsConfiguration.getTruststorePassword());
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_TYPE_KEY, tlsConfiguration.getTruststoreType().getType());
        properties.setProperty(RestChangeIngestor.KEYSTORE_LOCATION_KEY, tlsConfiguration.getKeystorePath());
        properties.setProperty(RestChangeIngestor.KEYSTORE_PASSWORD_KEY, tlsConfiguration.getKeystorePassword());
        properties.setProperty(RestChangeIngestor.KEYSTORE_TYPE_KEY, tlsConfiguration.getKeystoreType().getType());
        properties.setProperty(RestChangeIngestor.NEED_CLIENT_AUTH_KEY, "false");
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        properties.put(PULL_HTTP_BASE_KEY + ".override.core", "true");

        restChangeIngestor = new RestChangeIngestor();

        testNotifier = Mockito.mock(ConfigurationChangeNotifier.class);
        ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        when(testListener.getDescriptor()).thenReturn("MockChangeListener");
        when(testNotifier.notifyListeners(Mockito.any())).thenReturn(Collections.singleton(new ListenerHandleResult(testListener)));

        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);
        when(configurationFileHolder.getConfigFileReference()).thenReturn(new AtomicReference<>(ByteBuffer.wrap(new byte[0])));

        restChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        restChangeIngestor.setDifferentiator(mockDifferentiator);
        restChangeIngestor.start();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        final KeyStore keyStore;
        try (final FileInputStream keyStoreStream = new FileInputStream(tlsConfiguration.getKeystorePath())) {
            keyStore = new StandardKeyStoreBuilder()
                    .type(tlsConfiguration.getKeystoreType().getType())
                    .inputStream(keyStoreStream)
                    .password(tlsConfiguration.getKeystorePassword().toCharArray())
                    .build();
        }

        final KeyStore truststore;
        try (final FileInputStream trustStoreStream = new FileInputStream(tlsConfiguration.getTruststorePath())) {
            truststore = new StandardKeyStoreBuilder()
                    .type(tlsConfiguration.getTruststoreType().getType())
                    .inputStream(trustStoreStream)
                    .password(tlsConfiguration.getTruststorePassword().toCharArray())
                    .build();
        }

        final X509TrustManager trustManager = new StandardTrustManagerBuilder().trustStore(truststore).build();
        final SSLContext sslContext = new StandardSslContextBuilder()
                .keyStore(keyStore)
                .keyPassword(tlsConfiguration.getKeyPassword().toCharArray())
                .trustStore(truststore)
                .build();
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        clientBuilder.sslSocketFactory(sslSocketFactory, trustManager);

        Thread.sleep(1000);
        url = restChangeIngestor.getURI().toURL().toString();
        client = clientBuilder.build();
    }

    @AfterAll
    public static void stop() throws Exception {
        restChangeIngestor.close();
        client = null;
    }
}
