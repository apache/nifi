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

package org.apache.nifi.web.server;

import static org.apache.nifi.security.util.KeyStoreUtils.SUN_PROVIDER_NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

public class JettyServerTest {
    @Test
    public void testConfigureSslContextFactoryWithKeystorePasswordAndKeyPassword() {
        // Expect that if we set both passwords, KeyStore password is used for KeyStore, Key password is used for Key Manager
        String testKeystorePassword = "testKeystorePassword";
        String testKeyPassword = "testKeyPassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, testKeystorePassword);
        addProps.put(NiFiProperties.SECURITY_KEY_PASSWD, testKeyPassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setKeyStorePassword(testKeystorePassword);
        verify(mockSCF).setKeyManagerPassword(testKeyPassword);
    }

    @Test
    public void testConfigureSslContextFactoryWithKeyPassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyStore password, we will only need to set Key Manager Password
        String testKeyPassword = "testKeyPassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEY_PASSWD, testKeyPassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setKeyManagerPassword(testKeyPassword);
        verify(mockSCF, never()).setKeyStorePassword(anyString());
    }

    @Test
    public void testConfigureSslContextFactoryWithKeystorePassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyPassword, we use the same one from the KeyStore
        String testKeystorePassword = "testKeystorePassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, testKeystorePassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setKeyStorePassword(testKeystorePassword);
        verify(mockSCF).setKeyManagerPassword(testKeystorePassword);
    }

    @Test
    public void testConfigureSslContextFactoryWithJksKeyStore() {
        // Expect that we will not set provider for jks keystore
        final Map<String, String> addProps = new HashMap<>();
        String keyStoreType = KeystoreType.JKS.toString();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setKeyStoreType(keyStoreType);
        verify(mockSCF).setKeyStoreProvider(SUN_PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithPkcsKeyStore() {
        // Expect that we will set Bouncy Castle provider for pkcs12 keystore
        final Map<String, String> addProps = new HashMap<>();
        String keyStoreType = KeystoreType.PKCS12.toString();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setKeyStoreType(keyStoreType);
        verify(mockSCF).setKeyStoreProvider(BouncyCastleProvider.PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithJksTrustStore() {
        // Expect that we will not set provider for jks truststore
        final Map<String, String> addProps = new HashMap<>();
        String trustStoreType = KeystoreType.JKS.toString();
        addProps.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setTrustStoreType(trustStoreType);
        verify(mockSCF).setTrustStoreProvider(SUN_PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithPkcsTrustStore() {
        // Expect that we will set Bouncy Castle provider for pkcs12 truststore
        final Map<String, String> addProps = new HashMap<>();
        String trustStoreType = KeystoreType.PKCS12.toString();
        addProps.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);

        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);

        verify(mockSCF).setTrustStoreType(trustStoreType);
        verify(mockSCF).setTrustStoreProvider(BouncyCastleProvider.PROVIDER_NAME);
    }

    /**
     * Verify correct processing of cipher suites with multiple elements.  Verify call to override runtime ciphers.
     */
    @Test
    public void testConfigureSslIncludeExcludeCiphers() {
        final String[] includeCipherSuites = {"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"};
        final String includeCipherSuitesProp = StringUtils.join(includeCipherSuites, JettyServer.JOIN_ARRAY);
        final String[] excludeCipherSuites = {".*DHE.*", ".*ECDH.*"};
        final String excludeCipherSuitesProp = StringUtils.join(excludeCipherSuites, JettyServer.JOIN_ARRAY);
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.WEB_HTTPS_CIPHERSUITES_INCLUDE, includeCipherSuitesProp);
        addProps.put(NiFiProperties.WEB_HTTPS_CIPHERSUITES_EXCLUDE, excludeCipherSuitesProp);
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);

        final SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);
        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);
        verify(mockSCF, times(1)).setIncludeCipherSuites(includeCipherSuites);
        verify(mockSCF, times(1)).setExcludeCipherSuites(excludeCipherSuites);
    }

    /**
     * Verify skip cipher configuration when NiFiProperties are not specified.
     */
    @Test
    public void testDoNotConfigureSslIncludeExcludeCiphers() {
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null);
        final SslContextFactory.Server mockSCF = mock(SslContextFactory.Server.class);
        JettyServer.configureSslContextFactory(mockSCF, nifiProperties);
        verify(mockSCF, times(0)).setIncludeCipherSuites(any());
        verify(mockSCF, times(0)).setExcludeCipherSuites(any());
    }
}
