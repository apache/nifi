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
package org.apache.nifi.framework.security.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.util.NiFiProperties;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.X509CertSelector;

/**
 * A factory for creating SSL contexts using the application's security
 * properties.
 *
 */
public final class SslContextFactory {

    public static SSLContext createSslContext(final NiFiProperties props)
            throws SslContextCreationException {

        if (hasKeystoreProperties(props) == false) {
            return null;
        } else if (hasTruststoreProperties(props) == false) {
            throw new SslContextCreationException("SSL context cannot be created because truststore properties have not been configured.");
        }

        try {
            // prepare the trust store
            final KeyStore trustStore;
            if (hasTruststoreProperties(props)) {
                trustStore = KeyStoreUtils.getTrustStore(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
                try (final InputStream trustStoreStream = new FileInputStream(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE))) {
                    trustStore.load(trustStoreStream, props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray());
                }
            } else {
                trustStore = null;
            }

            final TrustManagerFactory trustManagerFactory = getTrustManagerFactory(trustStore, props.isOCSPEnabled());

            // prepare the key store
            final KeyStore keyStore = KeyStoreUtils.getKeyStore(props.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
            try (final InputStream keyStoreStream = new FileInputStream(props.getProperty(NiFiProperties.SECURITY_KEYSTORE))) {
                keyStore.load(keyStoreStream, props.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray());
            }
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            // if the key password is provided, try to use that - otherwise default to the keystore password
            if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEY_PASSWD))) {
                keyManagerFactory.init(keyStore, props.getProperty(NiFiProperties.SECURITY_KEY_PASSWD).toCharArray());
            } else {
                keyManagerFactory.init(keyStore, props.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray());
            }

            // initialize the ssl context
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(),
                    trustManagerFactory.getTrustManagers(), null);
            sslContext.getDefaultSSLParameters().setNeedClientAuth(true);

            return sslContext;

        } catch (final KeyStoreException | IOException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | CertificateException | UnrecoverableKeyException | KeyManagementException e) {
            throw new SslContextCreationException(e);
        }
    }

    private static boolean hasKeystoreProperties(final NiFiProperties props) {
        return (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEYSTORE))
                && StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD))
                && StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE)));
    }

    private static boolean hasTruststoreProperties(final NiFiProperties props) {
        return (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE))
                && StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD))
                && StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE)));
    }

    private static TrustManagerFactory getTrustManagerFactory(KeyStore trustStore, boolean ocspEnabled) throws KeyStoreException, InvalidAlgorithmParameterException, NoSuchAlgorithmException {

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (ocspEnabled) {
            if ("PKIX".equalsIgnoreCase(TrustManagerFactory.getDefaultAlgorithm())) {
                PKIXBuilderParameters pbParams = new PKIXBuilderParameters(trustStore, new X509CertSelector());
                pbParams.setRevocationEnabled(true);
                Security.setProperty("ocsp.enable", "true");
                trustManagerFactory.init(new CertPathTrustManagerParameters(pbParams));
            } else {
                throw new NoSuchAlgorithmException("PKIX algorithm was not available on this system. You must disable OCSP checking by changing " + NiFiProperties.SECURITY_OCSP_ENABLED + " to false.");
            }
        } else {
            trustManagerFactory.init(trustStore);
        }

        return trustManagerFactory;
    }

}
