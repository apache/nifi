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

package org.apache.nifi.oauth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.KeyManager;
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
import java.util.Map;

public class Util {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * This code as taken from the InvokeHttp processor from Apache NiFi 1.10-SNAPSHOT found here:
     *
     * https://github.com/apache/nifi/blob/1cadc722229ad50cf569ee107eaeeb95dc216ea2/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/InvokeHTTP.java
     */
    public static void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder, SSLContextService sslService, SSLContext sslContext, boolean setAsSocketFactory)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        // initialize the KeyManager array to null and we will overwrite later if a keystore is loaded
        KeyManager[] keyManagers = null;

        // we will only initialize the keystore if properties have been supplied by the SSLContextService
        if (sslService.isKeyStoreConfigured()) {
            final String keystoreLocation = sslService.getKeyStoreFile();
            final String keystorePass = sslService.getKeyStorePassword();
            final String keystoreType = sslService.getKeyStoreType();

            // prepare the keystore
            final KeyStore keyStore = KeyStore.getInstance(keystoreType);

            try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
                keyStore.load(keyStoreStream, keystorePass.toCharArray());
            }

            keyManagerFactory.init(keyStore, keystorePass.toCharArray());
            keyManagers = keyManagerFactory.getKeyManagers();
        }

        // we will only initialize the truststure if properties have been supplied by the SSLContextService
        if (sslService.isTrustStoreConfigured()) {
            // load truststore
            final String truststoreLocation = sslService.getTrustStoreFile();
            final String truststorePass = sslService.getTrustStorePassword();
            final String truststoreType = sslService.getTrustStoreType();

            KeyStore truststore = KeyStore.getInstance(truststoreType);
            truststore.load(new FileInputStream(truststoreLocation), truststorePass.toCharArray());
            trustManagerFactory.init(truststore);
        }

         /*
            TrustManagerFactory.getTrustManagers returns a trust manager for each type of trust material. Since we are getting a trust manager factory that uses "X509"
            as it's trust management algorithm, we are able to grab the first (and thus the most preferred) and use it as our x509 Trust Manager
            https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/TrustManagerFactory.html#getTrustManagers--
         */
        final X509TrustManager x509TrustManager;
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers[0] != null) {
            x509TrustManager = (X509TrustManager) trustManagers[0];
        } else {
            throw new IllegalStateException("List of trust managers is null");
        }

        // if keystore properties were not supplied, the keyManagers array will be null
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);
        if (setAsSocketFactory) {
            okHttpClientBuilder.socketFactory(sslSocketFactory);
        }
    }

    public static final String KEY_ACCESS_TOKEN = "access_token";
    public static final String KEY_REFRESH_TOKEN = "refresh_token";
    public static final String KEY_EXPIRES = "expires_in";
    public static final String KEY_TOKEN_TYPE = "token_type";
    public static final String KEY_SCOPE = "scope";

    public static AccessToken parseTokenResponse(String rawResponse) {
        try {
            Map<String, Object> parsed = MAPPER.readValue(rawResponse, Map.class);
            String accessToken = (String)parsed.get(KEY_ACCESS_TOKEN);
            String refreshToken = (String)parsed.get(KEY_REFRESH_TOKEN);
            Integer expires = (Integer)parsed.get(KEY_EXPIRES);
            String tokenType = (String)parsed.get(KEY_TOKEN_TYPE);
            String scope = (String)parsed.get(KEY_SCOPE);

            if (StringUtils.isEmpty(accessToken)) {
                throw new Exception(String.format("Missing value for %s", KEY_ACCESS_TOKEN));
            }

            if (StringUtils.isEmpty(tokenType)) {
                throw new Exception(String.format("Missing value for %s", KEY_TOKEN_TYPE));
            }

            return new AccessToken(accessToken, refreshToken, tokenType, expires, scope);
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }
}
