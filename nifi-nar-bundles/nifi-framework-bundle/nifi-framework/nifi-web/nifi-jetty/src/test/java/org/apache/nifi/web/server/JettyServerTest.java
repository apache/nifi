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

import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.security.util.KeyStoreUtils.SUN_PROVIDER_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStorePassword(testKeystorePassword);
        verify(contextFactory).setKeyManagerPassword(testKeyPassword);
    }

    @Test
    public void testConfigureSslContextFactoryWithKeyPassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyStore password, we will only need to set Key Manager Password
        String testKeyPassword = "testKeyPassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEY_PASSWD, testKeyPassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyManagerPassword(testKeyPassword);
        verify(contextFactory, never()).setKeyStorePassword(anyString());
    }

    @Test
    public void testConfigureSslContextFactoryWithKeystorePassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyPassword, we use the same one from the KeyStore
        String testKeystorePassword = "testKeystorePassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, testKeystorePassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStorePassword(testKeystorePassword);
        verify(contextFactory).setKeyManagerPassword(testKeystorePassword);
    }

    @Test
    public void testConfigureSslContextFactoryWithJksKeyStore() {
        // Expect that we will not set provider for jks keystore
        final Map<String, String> addProps = new HashMap<>();
        String keyStoreType = KeystoreType.JKS.toString();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStoreType(keyStoreType);
        verify(contextFactory).setKeyStoreProvider(SUN_PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithPkcsKeyStore() {
        // Expect that we will set Bouncy Castle provider for pkcs12 keystore
        final Map<String, String> addProps = new HashMap<>();
        String keyStoreType = KeystoreType.PKCS12.toString();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStoreType(keyStoreType);
        verify(contextFactory).setKeyStoreProvider(BouncyCastleProvider.PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithJksTrustStore() {
        // Expect that we will not set provider for jks truststore
        final Map<String, String> addProps = new HashMap<>();
        String trustStoreType = KeystoreType.JKS.toString();
        addProps.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setTrustStoreType(trustStoreType);
        verify(contextFactory).setTrustStoreProvider(SUN_PROVIDER_NAME);
    }

    @Test
    public void testConfigureSslContextFactoryWithPkcsTrustStore() {
        // Expect that we will set Bouncy Castle provider for pkcs12 truststore
        final Map<String, String> addProps = new HashMap<>();
        String trustStoreType = KeystoreType.PKCS12.toString();
        addProps.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setTrustStoreType(trustStoreType);
        verify(contextFactory).setTrustStoreProvider(BouncyCastleProvider.PROVIDER_NAME);
    }

    @Test
    public void testNoDuplicateXFrameOptions() throws NoSuchFieldException, IllegalAccessException, ServletException, IOException {
        Field xOptionsFilter = JettyServer.class.getDeclaredField("FRAME_OPTIONS_FILTER");
        xOptionsFilter.setAccessible(true);
        Filter filter = (Filter) xOptionsFilter.get(xOptionsFilter);

        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/");

        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);
        ServletContext mockContext = Mockito.mock(ServletContext.class);
        FilterConfig mockFilterConfig = Mockito.mock(FilterConfig.class);

        when(mockFilterConfig.getServletContext()).thenReturn(mockContext);

        filter.init(mockFilterConfig);

        // Call doFilter twice, then check the header only appears once.
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        assertEquals(1, mockResponse.getHeaders("X-Frame-Options").size());
    }
}
