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
package org.apache.nifi.io.socket;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;

public class SSLContextFactory {

    private final String keystore;
    private final char[] keystorePass;
    private final String keystoreType;
    private final String truststore;
    private final char[] truststorePass;
    private final String truststoreType;

    private final KeyManager[] keyManagers;
    private final TrustManager[] trustManagers;

    public SSLContextFactory(final NiFiProperties properties) throws NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, KeyStoreException, UnrecoverableKeyException {
        keystore = properties.getProperty(NiFiProperties.SECURITY_KEYSTORE);
        keystorePass = getPass(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD));
        keystoreType = properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);

        truststore = properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE);
        truststorePass = getPass(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
        truststoreType = properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);

        // prepare the keystore
        final KeyStore keyStore = KeyStoreUtils.getKeyStore(keystoreType);
        final FileInputStream keyStoreStream = new FileInputStream(keystore);
        try {
            keyStore.load(keyStoreStream, keystorePass);
        } finally {
            FileUtils.closeQuietly(keyStoreStream);
        }
        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass);

        // prepare the truststore
        final KeyStore trustStore = KeyStoreUtils.getTrustStore(truststoreType);
        final FileInputStream trustStoreStream = new FileInputStream(truststore);
        try {
            trustStore.load(trustStoreStream, truststorePass);
        } finally {
            FileUtils.closeQuietly(trustStoreStream);
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        keyManagers = keyManagerFactory.getKeyManagers();
        trustManagers = trustManagerFactory.getTrustManagers();
    }

    private static char[] getPass(final String password) {
        return password == null ? null : password.toCharArray();
    }

    /**
     * Creates a SSLContext instance using the given information.
     *
     *
     * @return a SSLContext instance
     * @throws java.security.KeyStoreException if problem with keystore
     * @throws java.io.IOException if unable to create context
     * @throws java.security.NoSuchAlgorithmException if algorithm isn't known
     * @throws java.security.cert.CertificateException if certificate is invalid
     * @throws java.security.UnrecoverableKeyException if the key cannot be recovered
     * @throws java.security.KeyManagementException if the key is improper
     */
    public SSLContext createSslContext() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException {

        // initialize the ssl context
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, new SecureRandom());
        sslContext.getDefaultSSLParameters().setNeedClientAuth(true);

        return sslContext;

    }
}
