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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.ListAzureBlobStorage;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Before;
import org.junit.Test;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAzureProxyUtilsValidateProxySpec {

    private static final String PROXY_CONFIGURATION_SERVICE_VALUE = "ProxyConfigurationService";

    private MockProcessContext processContext;
    private MockValidationContext validationContext;
    private Collection<ValidationResult> validationResults;

    @Before
    public void setUp() {
        Processor processor = new ListAzureBlobStorage();
        processContext = new MockProcessContext(processor);
        validationContext = new MockValidationContext(processContext);
        validationResults = new ArrayList<>();
    }


    private void configureProxyConfigurationService(Proxy.Type proxyType, String proxyHost, Integer proxyPort, String proxyUser, String proxyPassword) {

        ProxyConfiguration proxyConfig = new ProxyConfiguration();

        // set all values
        proxyConfig.setProxyType(proxyType);
        proxyConfig.setProxyServerHost(proxyHost);
        proxyConfig.setProxyServerPort(proxyPort);
        proxyConfig.setProxyUserName(proxyUser);
        proxyConfig.setProxyUserPassword(proxyPassword);

        MockProxyConfigurationService mockProxyConfigurationService = new MockProxyConfigurationService(proxyConfig);

        processContext.addControllerService(mockProxyConfigurationService, PROXY_CONFIGURATION_SERVICE_VALUE);
        processContext.setProperty(AzureProxyUtils.PROXY_CONFIGURATION_SERVICE, PROXY_CONFIGURATION_SERVICE_VALUE);
    }

    @Test
    public void testConfigureValid() {
        configureProxyConfigurationService(Proxy.Type.DIRECT, null, null, null, null);
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertValid(validationResults);
    }

    @Test
    public void testConfigureProxyDirectInvalid() {
        configureProxyConfigurationService(Proxy.Type.DIRECT, "apache.org", null, null, null);
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertNotValid(validationResults);
    }

    @Test
    public void testConfigureProxyHostAndPortCombinationNotComplete() {
        configureProxyConfigurationService(Proxy.Type.HTTP, "apache.org", null, null, null);
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertNotValid(validationResults);
    }

    @Test
    public void testConfigureProxyHostAndPortCombinationComplete() {
        configureProxyConfigurationService(Proxy.Type.HTTP, "apache.org", 443, null, null);
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertValid(validationResults);
    }

    @Test
    public void testConfigureProxyUserPassCombinationNotComplete() {
        configureProxyConfigurationService(Proxy.Type.HTTP, "apache.org", null, null, "pass");
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertNotValid(validationResults);
    }

    @Test
    public void testConfigureProxyUserPassCombinationComplete() {
        configureProxyConfigurationService(Proxy.Type.HTTP, "apache.org", 443, "user", "pass");
        AzureProxyUtils.validateProxySpec(validationContext, validationResults);
        assertValid(validationResults);
    }

    private void assertValid(Collection<ValidationResult> result) {
        assertTrue("There should be no validation error", result.isEmpty());
    }

    private void assertNotValid(Collection<ValidationResult> result) {
        assertFalse("There should be validation error", result.isEmpty());
    }

    private static class MockProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {
        private final ProxyConfiguration proxyConfiguration;

        MockProxyConfigurationService(final ProxyConfiguration proxyConfiguration) {
            this.proxyConfiguration = proxyConfiguration;
        }

        @Override
        public ProxyConfiguration getConfiguration() {
            return this.proxyConfiguration;
        }
    }
}
