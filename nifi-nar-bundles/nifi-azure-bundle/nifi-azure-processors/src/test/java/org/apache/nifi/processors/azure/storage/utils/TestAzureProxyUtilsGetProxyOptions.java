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
package org.apache.nifi.processors.azure.storage.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.InetSocketAddress;
import java.net.Proxy;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.ProxyOptions.Type;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.ListAzureBlobStorage;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

public class TestAzureProxyUtilsGetProxyOptions {

    private MockProcessContext processContext;
    private ProxyConfiguration proxyConfig;

    private static final String PROXY_CONFIG_SERVICE_VALUE = "ProxyConfigurationService";
    private static final String PROXY_HOST = "localhost";
    private static final Integer PROXY_PORT = 9000;
    private static final String PROXY_USER = "microsoft";
    private static final String PROXY_PASSWORD = "azure";

    @Before
    public void setUp() {
        final Processor processor = new ListAzureBlobStorage();
        processContext = new MockProcessContext(processor);
    }

    private void configureMockedHTTPProxyService(String proxyHost, Integer proxyPort, String proxyUser, String proxyUserPassword) {
        proxyConfig = new ProxyConfiguration();
        proxyConfig.setProxyType(Proxy.Type.HTTP);

        if(StringUtils.isNotBlank(proxyHost)) {
            proxyConfig.setProxyServerHost(proxyHost);
        }
        if(proxyPort != null) {
            proxyConfig.setProxyServerPort(proxyPort);
        }
        if(StringUtils.isNotBlank(proxyUser)) {
            proxyConfig.setProxyUserName(proxyUser);
        }
        if(StringUtils.isNotBlank(proxyUserPassword)) {
            proxyConfig.setProxyUserPassword(proxyUserPassword);
        }

        MockProxyConfigurationService mockProxyConfigurationService = new MockProxyConfigurationService(proxyConfig);
        // set mocked proxy service
        processContext.addControllerService(mockProxyConfigurationService, PROXY_CONFIG_SERVICE_VALUE);
        processContext.setProperty(AzureProxyUtils.PROXY_CONFIGURATION_SERVICE, PROXY_CONFIG_SERVICE_VALUE);
    }

    @Test
    public void testHTTPProxy() {
        configureMockedHTTPProxyService(PROXY_HOST, PROXY_PORT, null, null);
        final MockProxyConfigurationService mockProxyConfigurationService = processContext.getProperty(
            AzureProxyUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(MockProxyConfigurationService.class);

        final ProxyOptions proxyOptions = AzureProxyUtils.getProxyOptions(mockProxyConfigurationService.getConfiguration());
        final InetSocketAddress socketAddress = new InetSocketAddress(PROXY_HOST, PROXY_PORT);

        assertEquals(proxyOptions.getAddress(), socketAddress);
        assertEquals(proxyOptions.getType(), Type.HTTP);
        // null asserts
        assertNull(proxyOptions.getUsername());
        assertNull(proxyOptions.getPassword());
    }

    @Test
    public void testHTTPProxyWithAuth() {
        configureMockedHTTPProxyService(PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASSWORD);
        final MockProxyConfigurationService mockProxyConfigurationService = processContext.getProperty(
            AzureProxyUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(MockProxyConfigurationService.class);

        final ProxyOptions proxyOptions = AzureProxyUtils.getProxyOptions(mockProxyConfigurationService.getConfiguration());
        final InetSocketAddress socketAddress = new InetSocketAddress(PROXY_HOST, PROXY_PORT);

        assertEquals(proxyOptions.getAddress(), socketAddress);
        assertEquals(proxyOptions.getType(), Type.HTTP);
        assertEquals(proxyOptions.getUsername(), PROXY_USER);
        assertEquals(proxyOptions.getPassword(), PROXY_PASSWORD);
    }

    @Test
    public void testHTTPProxyWithOnlyUser() {
        configureMockedHTTPProxyService(PROXY_HOST, PROXY_PORT, PROXY_USER, null);
        final MockProxyConfigurationService mockProxyConfigurationService = processContext.getProperty(
            AzureProxyUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(MockProxyConfigurationService.class);

        final ProxyOptions proxyOptions = AzureProxyUtils.getProxyOptions(mockProxyConfigurationService.getConfiguration());
        final InetSocketAddress socketAddress = new InetSocketAddress(PROXY_HOST, PROXY_PORT);

        assertEquals(proxyOptions.getAddress(), socketAddress);
        assertEquals(proxyOptions.getType(), Type.HTTP);
        // null asserts
        assertNull(proxyOptions.getUsername());
        assertNull(proxyOptions.getPassword());
    }

    @Test
    public void testHTTPProxyWithOnlyProxyPort() {
        configureMockedHTTPProxyService(null, PROXY_PORT, null, null);
        final MockProxyConfigurationService mockProxyConfigurationService = processContext.getProperty(
            AzureProxyUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(MockProxyConfigurationService.class);

        final ProxyOptions proxyOptions = AzureProxyUtils.getProxyOptions(mockProxyConfigurationService.getConfiguration());

        // null asserts
        assertNull(proxyOptions);
    }

    @Test
    public void testHTTPProxyWithoutInput() {
        configureMockedHTTPProxyService(null, null, null, null);
        final MockProxyConfigurationService mockProxyConfigurationService = processContext.getProperty(
            AzureProxyUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(MockProxyConfigurationService.class);

        final ProxyOptions proxyOptions = AzureProxyUtils.getProxyOptions(mockProxyConfigurationService.getConfiguration());

        // null asserts
        assertNull(proxyOptions);
    }

    private class MockProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {
        private final ProxyConfiguration proxyConfiguration;

        public MockProxyConfigurationService(final ProxyConfiguration proxyConfiguration) {
            this.proxyConfiguration = proxyConfiguration;
        }

        @Override
        public ProxyConfiguration getConfiguration() {
            return this.proxyConfiguration;
        }
    }
}