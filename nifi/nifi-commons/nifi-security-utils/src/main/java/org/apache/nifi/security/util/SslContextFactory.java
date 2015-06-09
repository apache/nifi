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
package org.apache.nifi.security.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

/**
 * A factory for creating SSL contexts using the application's security
 * properties.
 *
 */
public final class SslContextFactory {

    public static enum ClientAuth {

        WANT,
        REQUIRED,
        NONE
    }

    /**
     * Creates a SSLContext instance using the given information.
     *
     * @param keystore the full path to the keystore
     * @param keystorePasswd the keystore password
     * @param keystoreType the type of keystore (e.g., PKCS12, JKS)
     * @param truststore the full path to the truststore
     * @param truststorePasswd the truststore password
     * @param truststoreType the type of truststore (e.g., PKCS12, JKS)
     * @param clientAuth the type of client authentication
     * @param protocol         the protocol to use for the SSL connection
     *
     * @return a SSLContext instance
     * @throws java.security.KeyStoreException if any issues accessing the keystore
     * @throws java.io.IOException for any problems loading the keystores
     * @throws java.security.NoSuchAlgorithmException if an algorithm is found to be used but is unknown
     * @throws java.security.cert.CertificateException if there is an issue with the certificate
     * @throws java.security.UnrecoverableKeyException if the key is insufficient
     * @throws java.security.KeyManagementException if unable to manage the key
     */
    public static SSLContext createSslContext(
            final String keystore, final char[] keystorePasswd, final String keystoreType,
            final String truststore, final char[] truststorePasswd, final String truststoreType,
            final ClientAuth clientAuth, final String protocol)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException {

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (final InputStream keyStoreStream = new FileInputStream(keystore)) {
            keyStore.load(keyStoreStream, keystorePasswd);
        }
        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePasswd);

        // prepare the truststore
        final KeyStore trustStore = KeyStore.getInstance(truststoreType);
        try (final InputStream trustStoreStream = new FileInputStream(truststore)) {
            trustStore.load(trustStoreStream, truststorePasswd);
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        // initialize the ssl context
        final SSLContext sslContext = SSLContext.getInstance(protocol);
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
        if (ClientAuth.REQUIRED == clientAuth) {
            sslContext.getDefaultSSLParameters().setNeedClientAuth(true);
        } else if (ClientAuth.WANT == clientAuth) {
            sslContext.getDefaultSSLParameters().setWantClientAuth(true);
        } else {
            sslContext.getDefaultSSLParameters().setWantClientAuth(false);
        }

        return sslContext;

    }

    /**
     * Creates a SSLContext instance using the given information.
     *
     * @param keystore the full path to the keystore
     * @param keystorePasswd the keystore password
     * @param keystoreType the type of keystore (e.g., PKCS12, JKS)
     * @param protocol the protocol to use for the SSL connection
     *
     * @return a SSLContext instance
     * @throws java.security.KeyStoreException if any issues accessing the keystore
     * @throws java.io.IOException for any problems loading the keystores
     * @throws java.security.NoSuchAlgorithmException if an algorithm is found to be used but is unknown
     * @throws java.security.cert.CertificateException if there is an issue with the certificate
     * @throws java.security.UnrecoverableKeyException if the key is insufficient
     * @throws java.security.KeyManagementException if unable to manage the key
     */
    public static SSLContext createSslContext(
            final String keystore, final char[] keystorePasswd, final String keystoreType, final String protocol)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException {

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (final InputStream keyStoreStream = new FileInputStream(keystore)) {
            keyStore.load(keyStoreStream, keystorePasswd);
        }
        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePasswd);

        // initialize the ssl context
        final SSLContext ctx = SSLContext.getInstance(protocol);
        ctx.init(keyManagerFactory.getKeyManagers(), new TrustManager[0], new SecureRandom());

        return ctx;

    }

    /**
     * Creates a SSLContext instance using the given information.
     *
     * @param truststore the full path to the truststore
     * @param truststorePasswd the truststore password
     * @param truststoreType the type of truststore (e.g., PKCS12, JKS)
     * @param protocol the protocol to use for the SSL connection
     *
     * @return a SSLContext instance
     * @throws java.security.KeyStoreException if any issues accessing the keystore
     * @throws java.io.IOException for any problems loading the keystores
     * @throws java.security.NoSuchAlgorithmException if an algorithm is found to be used but is unknown
     * @throws java.security.cert.CertificateException if there is an issue with the certificate
     * @throws java.security.UnrecoverableKeyException if the key is insufficient
     * @throws java.security.KeyManagementException if unable to manage the key
     */
    public static SSLContext createTrustSslContext(
            final String truststore, final char[] truststorePasswd, final String truststoreType, final String protocol)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException {

        // prepare the truststore
        final KeyStore trustStore = KeyStore.getInstance(truststoreType);
        try (final InputStream trustStoreStream = new FileInputStream(truststore)) {
            trustStore.load(trustStoreStream, truststorePasswd);
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        // initialize the ssl context
        final SSLContext ctx = SSLContext.getInstance(protocol);
        ctx.init(new KeyManager[0], trustManagerFactory.getTrustManagers(), new SecureRandom());

        return ctx;

    }

}
