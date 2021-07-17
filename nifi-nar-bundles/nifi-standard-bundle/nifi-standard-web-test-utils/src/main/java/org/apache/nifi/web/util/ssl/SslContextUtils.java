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
package org.apache.nifi.web.util.ssl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.security.Security;

public class SslContextUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextUtils.class);

    private static final String TLS_DISABLED_ALGORITHMS_PROPERTY = "jdk.tls.disabledAlgorithms";

    private static final String DISABLED_ALGORITHMS = "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL, include jdk.disabled.namedCurves";

    static {
        final String disabledAlgorithms = Security.getProperty(TLS_DISABLED_ALGORITHMS_PROPERTY);
        if (DISABLED_ALGORITHMS.equals(disabledAlgorithms)) {
            LOGGER.debug("Found Expected Default TLS Disabled Algorithms: {}", DISABLED_ALGORITHMS);
        } else {
            LOGGER.warn("Found System Default TLS Disabled Algorithms: {}", disabledAlgorithms);
            LOGGER.warn("Setting TLS Disabled Algorithms: {}", DISABLED_ALGORITHMS);
            Security.setProperty(TLS_DISABLED_ALGORITHMS_PROPERTY, DISABLED_ALGORITHMS);
        }
    }

    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks";

    private static final String KEYSTORE_AND_TRUSTSTORE_PASSWORD = "passwordpassword";

    private static final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks";

    private static final TlsConfiguration KEYSTORE_TLS_CONFIGURATION = new StandardTlsConfiguration(
            KEYSTORE_PATH,
            KEYSTORE_AND_TRUSTSTORE_PASSWORD,
            KEYSTORE_AND_TRUSTSTORE_PASSWORD,
            KeystoreType.JKS,
            TRUSTSTORE_PATH,
            KEYSTORE_AND_TRUSTSTORE_PASSWORD,
            KeystoreType.JKS,
            TlsConfiguration.TLS_1_2_PROTOCOL
    );

    private static final TlsConfiguration TRUSTSTORE_TLS_CONFIGURATION = new StandardTlsConfiguration(
            null,
            null,
            null,
            null,
            TRUSTSTORE_PATH,
            KEYSTORE_AND_TRUSTSTORE_PASSWORD,
            KeystoreType.JKS,
            TlsConfiguration.TLS_1_2_PROTOCOL
    );

    /**
     * Create SSLContext with Key Store and Trust Store configured
     *
     * @return SSLContext configured with Key Store and Trust Store
     * @throws TlsException Thrown on SslContextFactory.createSslContext()
     */
    public static SSLContext createKeyStoreSslContext() throws TlsException {
        return SslContextFactory.createSslContext(KEYSTORE_TLS_CONFIGURATION);
    }

    /**
     * Create SSLContext with Trust Store configured
     *
     * @return SSLContext configured with Trust Store
     * @throws TlsException Thrown on SslContextFactory.createSslContext()
     */
    public static SSLContext createTrustStoreSslContext() throws TlsException {
        return SslContextFactory.createSslContext(TRUSTSTORE_TLS_CONFIGURATION);
    }

    /**
     * Create SSLContext using Keystore and Truststore with deleteOnExit() for files
     *
     * @param tlsConfiguration TLS Configuration
     * @return SSLContext configured with generated Keystore and Truststore
     * @throws TlsException Thrown on SslContextFactory.createSslContext()
     */
    public static SSLContext createSslContext(final TlsConfiguration tlsConfiguration) throws TlsException {
        final String keystorePath = tlsConfiguration.getKeystorePath();
        if (StringUtils.isNotBlank(keystorePath)) {
            final File keystoreFile = new File(keystorePath);
            keystoreFile.deleteOnExit();
        }

        final String truststorePath = tlsConfiguration.getTruststorePath();
        if (StringUtils.isNotBlank(truststorePath)) {
            final File truststoreFile = new File(truststorePath);
            truststoreFile.deleteOnExit();
        }

        final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
        if (sslContext == null) {
            throw new TlsException(String.format("Failed to create SSLContext from Configuration %s", tlsConfiguration));
        }
        return sslContext;
    }
}
