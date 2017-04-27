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


import okhttp3.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ListenerHandleResult;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.RestChangeIngestorCommonTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Properties;

import static org.mockito.Mockito.when;


public class RestChangeIngestorSSLTest extends RestChangeIngestorCommonTest {


    @BeforeClass
    public static void setUpHttps() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, UnrecoverableKeyException, KeyManagementException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_LOCATION_KEY, "./src/test/resources/localhost-ts.jks");
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_PASSWORD_KEY, "localtest");
        properties.setProperty(RestChangeIngestor.TRUSTSTORE_TYPE_KEY, "JKS");
        properties.setProperty(RestChangeIngestor.KEYSTORE_LOCATION_KEY, "./src/test/resources/localhost-ks.jks");
        properties.setProperty(RestChangeIngestor.KEYSTORE_PASSWORD_KEY, "localtest");
        properties.setProperty(RestChangeIngestor.KEYSTORE_TYPE_KEY, "JKS");
        properties.setProperty(RestChangeIngestor.NEED_CLIENT_AUTH_KEY, "false");

        restChangeIngestor = new RestChangeIngestor();

        testNotifier = Mockito.mock(ConfigurationChangeNotifier.class);
        ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        when(testListener.getDescriptor()).thenReturn("MockChangeListener");
        when(testNotifier.notifyListeners(Mockito.any())).thenReturn(Collections.singleton(new ListenerHandleResult(testListener)));

        restChangeIngestor.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), testNotifier);
        restChangeIngestor.setDifferentiator(mockDifferentiator);
        restChangeIngestor.start();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        final String keystoreLocation = "./src/test/resources/localhost-ks.jks";
        final String keystorePass = "localtest";
        final String keystoreType = "JKS";

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = "./src/test/resources/localhost-ts.jks";
        final String truststorePass = "localtest";
        final String truststoreType = "JKS";

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
            tempSslContext = SSLContext.getDefault();
        }

        final SSLContext sslContext = tempSslContext;
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        clientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);

        Thread.sleep(1000);
        url = restChangeIngestor.getURI().toURL().toString();
        client = clientBuilder.build();
    }

    @AfterClass
    public static void stop() throws Exception {
        restChangeIngestor.close();
        client = null;
    }

    private static KeyStore readKeyStore(String path) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore ks = KeyStore.getInstance("jks");

        char[] password = "localtest".toCharArray();

        java.io.FileInputStream fis = null;
        try {
            fis = new java.io.FileInputStream(path);
            ks.load(fis, password);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return ks;
    }
}
