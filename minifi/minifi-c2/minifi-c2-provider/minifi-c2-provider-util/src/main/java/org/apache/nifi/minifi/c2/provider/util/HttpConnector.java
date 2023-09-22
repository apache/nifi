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

package org.apache.nifi.minifi.c2.provider.util;

import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE_TYPE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEY_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE_TYPE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.properties.C2Properties;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpConnector {

    private static final Logger logger = LoggerFactory.getLogger(HttpConnector.class);
    private static final String HTTPS = "https:";
    private static final String C2_SERVER_HOME = System.getenv("C2_SERVER_HOME");

    private final String baseUrl;
    private final SSLContext sslContext;
    private final Proxy proxy;
    private final String proxyAuthorization;

    public HttpConnector(String baseUrl) throws InvalidParameterException, GeneralSecurityException, IOException {
        this(baseUrl, null, 0);
    }

    public HttpConnector(String baseUrl, String proxyHost, int proxyPort) throws InvalidParameterException, GeneralSecurityException, IOException {
        this(baseUrl, proxyHost, proxyPort, null, null);
    }

    public HttpConnector(String baseUrl, String proxyHost, int proxyPort, String proxyUsername, String proxyPassword) throws InvalidParameterException {
        if (baseUrl.startsWith(HTTPS)) {
            sslContext = buildSSLContext();
        } else {
            sslContext = null;
        }
        this.baseUrl = baseUrl;
        if (proxyHost != null && !proxyHost.isEmpty()) {
            if (proxyPort == 0) {
                throw new InvalidParameterException("Must specify proxy port with proxy host");
            }
            proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        } else {
            proxy = null;
        }

        if (proxyUsername != null && !proxyUsername.isEmpty()) {
            if (proxy == null) {
                throw new InvalidParameterException("Cannot specify proxy username without proxy host.");
            }
            if (proxyPassword == null) {
                throw new InvalidParameterException("Must specify proxy password with proxy username.");
            }
            proxyAuthorization = "Basic " + Base64.getEncoder().encodeToString((proxyHost + ":" + proxyPassword).getBytes(StandardCharsets.UTF_8));
        } else {
            proxyAuthorization = null;
        }
    }

    private SSLContext buildSSLContext() {
        C2Properties properties = C2Properties.getInstance();
        KeyStore keyStore;
        KeyStore truststore;

        Path c2ServerHome = Paths.get(C2_SERVER_HOME);
        File keyStoreFile = c2ServerHome.resolve(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE)).toFile();
        logger.debug("Loading Key Store [{}]", keyStoreFile.getPath());
        try (FileInputStream keyStoreStream = new FileInputStream(keyStoreFile)) {
            keyStore = new StandardKeyStoreBuilder()
                .type(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_TYPE))
                .inputStream(keyStoreStream)
                .password(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_PASSWD).toCharArray())
                .build();
        } catch (IOException ioe) {
            throw new UncheckedIOException("Key Store loading failed", ioe);
        }

        File trustStoreFile = c2ServerHome.resolve(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE)).toFile();
        logger.debug("Loading Trust Store [{}]", trustStoreFile.getPath());
        try (FileInputStream trustStoreStream = new FileInputStream(trustStoreFile)) {
            truststore = new StandardKeyStoreBuilder()
                .type(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_TYPE))
                .inputStream(trustStoreStream)
                .password(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_PASSWD).toCharArray())
                .build();
        } catch (IOException ioe) {
            throw new UncheckedIOException("Trust Store loading failed", ioe);
        }

        return new StandardSslContextBuilder()
            .keyStore(keyStore)
            .keyPassword(properties.getProperty(MINIFI_C2_SERVER_KEY_PASSWD).toCharArray())
            .trustStore(truststore)
            .build();
    }

    public HttpURLConnection get(String endpointPath) throws ConfigurationProviderException {
        return get(endpointPath, Collections.emptyMap());
    }

    public HttpURLConnection get(String endpointPath, Map<String, List<String>> headers) throws ConfigurationProviderException {
        String endpointUrl = baseUrl + endpointPath;
        if (logger.isDebugEnabled()) {
            logger.debug("Connecting to endpoint: " + endpointUrl);
        }
        URL url;
        try {
            url = URI.create(endpointUrl).toURL();
        } catch (IllegalArgumentException | MalformedURLException e) {
            throw new ConfigurationProviderException("Malformed url " + endpointUrl, e);
        }

        HttpURLConnection httpURLConnection;
        try {
            if (proxy == null) {
                httpURLConnection = (HttpURLConnection) url.openConnection();
            } else {
                httpURLConnection = (HttpURLConnection) url.openConnection(proxy);
            }
            if (sslContext != null) {
                HttpsURLConnection httpsURLConnection = (HttpsURLConnection) httpURLConnection;
                SSLSocketFactory socketFactory = sslContext.getSocketFactory();
                httpsURLConnection.setSSLSocketFactory(socketFactory);
            }
        } catch (IOException e) {
            throw new ConfigurationProviderException("Unable to connect to " + url, e);
        }
        if (proxyAuthorization != null) {
            httpURLConnection.setRequestProperty("Proxy-Authorization", proxyAuthorization);
        }
        headers.forEach((s, strings) -> httpURLConnection.setRequestProperty(s, String.join(",", strings)));
        return httpURLConnection;
    }
}
