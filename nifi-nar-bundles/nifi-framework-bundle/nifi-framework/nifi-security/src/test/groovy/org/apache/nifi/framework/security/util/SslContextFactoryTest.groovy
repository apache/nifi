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
package org.apache.nifi.framework.security.util

import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.security.util.KeyStoreUtils
import org.apache.nifi.util.file.FileUtils
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.security.Security

@RunWith(JUnit4.class)
class SslContextFactoryGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SslContextFactoryGroovyTest.class)
    static KeyStore keyStore;
    static KeyStore trustStore;
    static String keystorePass = "passwordpassword"
    static String trustStorePass = "passwordpassword"
    static final File ksFile = new File(SslContextFactoryTest.class.getResource("/keystore.jks").toURI());
    static final File trustFile = new File(SslContextFactoryTest.class.getResource("/truststore.jks").toURI());

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        // prepare the keystore
        keyStore = KeyStoreUtils.getKeyStore("JKS");
        FileInputStream keyStoreStream = new FileInputStream(ksFile);
        try {
            keyStore.load(keyStoreStream, keystorePass.getChars());
        } finally {
            FileUtils.closeQuietly(keyStoreStream);
        }

        trustStore = KeyStoreUtils.getKeyStore("JKS");
        FileInputStream trustStoreStream = new FileInputStream(trustFile);
        try {
            trustStore.load(trustStoreStream, trustStorePass.getChars());
        } finally {
            FileUtils.closeQuietly(trustStoreStream);
        }
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        keyStore = null
        trustStore = null
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {
        //Security.setProperty("ocsp.enable", "")
        Security.properties.remove("ocsp.enable")
    }

    @Test
    void testGetTrustManagerFactoryOCSPEnabled() {
        // Act
        TrustManagerFactory fac = SslContextFactory.getTrustManagerFactory(trustStore, true)

        // Assert
        assert fac.getTrustManagers().first().pkixParams.revocationEnabled
        assert Security.getProperty("ocsp.enable")
        Security.props.remove("ocsp.enable")
    }

    @Test
    void testGetTrustManagerFactoryOCSPDisabled() {
        // Arrange

        // Act
        TrustManagerFactory fac = SslContextFactory.getTrustManagerFactory(trustStore, false)

        // Assert
        assert fac
        assertNull fac.getTrustManagers().first().pkixParams
        assertNull Security.getProperty("ocsp.enable")
    }

    @Test
    void testCreateSslContextOCSPEnabled() {

        // Arrange
        Properties nifiProps = new Properties();
        nifiProps.setProperty(NiFiProperties.SECURITY_OCSP_ENABLED, "true")
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE, ksFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.WEB_HTTP_HOST, "localhost")
        nifiProps.setProperty(NiFiProperties.WEB_HTTPS_HOST, "secure.host.com")
        nifiProps.setProperty(NiFiProperties.WEB_THREADS, NiFiProperties.DEFAULT_WEB_THREADS.toString())
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE, ksFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS")
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD, keystorePass)
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, trustFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "passwordpassword")
        NiFiProperties mockProps = new StandardNiFiProperties(nifiProps)

        // Act
        SSLContext fac = SslContextFactory.createSslContext(mockProps)

        // Assert
        assert fac
        assert fac.contextSpi.properties.get("x509TrustManager").pkixParams.revocationEnabled
        assert Security.getProperty("ocsp.enable")
    }

    @Test
    void testCreateSslContextOCSPDisabled() {

        // Arrange
        Properties nifiProps = new Properties()
        nifiProps.setProperty(NiFiProperties.SECURITY_OCSP_ENABLED, "false")
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE, ksFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.WEB_HTTP_HOST, "localhost")
        nifiProps.setProperty(NiFiProperties.WEB_HTTPS_HOST, "secure.host.com")
        nifiProps.setProperty(NiFiProperties.WEB_THREADS, NiFiProperties.DEFAULT_WEB_THREADS.toString())
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE, ksFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS")
        nifiProps.setProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD, keystorePass)
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, trustFile.getAbsolutePath())
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        nifiProps.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "passwordpassword")
        NiFiProperties mockProps = new StandardNiFiProperties(nifiProps)

        // Act
        SSLContext fac = SslContextFactory.createSslContext(mockProps)

        // Assert
        assert fac
        assertNull fac.contextSpi.properties.get("x509TrustManager").pkixParams
        assertNull Security.getProperty("ocsp.enable")
    }

}