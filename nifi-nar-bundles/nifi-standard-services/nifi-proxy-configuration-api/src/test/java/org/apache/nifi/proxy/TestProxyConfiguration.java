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
package org.apache.nifi.proxy;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.proxy.ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestProxyConfiguration {

    public static class ComponentUsingProxy extends AbstractProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(PROXY_CONFIGURATION_SERVICE);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }

        @Override
        protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
            final List<ValidationResult> results = new ArrayList<>();
            ProxyConfiguration.validateProxyType(validationContext, results, Proxy.Type.HTTP);
            return results;
        }
    }

    @Test
    public void testValidateProxyType() throws Exception {
        final String serviceId = "proxyConfigurationService";
        final ProxyConfigurationService service = mock(ProxyConfigurationService.class);
        when(service.getIdentifier()).thenReturn(serviceId);

        final ProxyConfiguration httpConfig = new ProxyConfiguration();
        httpConfig.setProxyType(Proxy.Type.HTTP);

        final ProxyConfiguration socksConfig = new ProxyConfiguration();
        socksConfig.setProxyType(Proxy.Type.SOCKS);

        when(service.getConfiguration()).thenReturn(ProxyConfiguration.DIRECT_CONFIGURATION, httpConfig, socksConfig);

        final TestRunner testRunner = TestRunners.newTestRunner(ComponentUsingProxy.class);
        testRunner.addControllerService(serviceId, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(PROXY_CONFIGURATION_SERVICE, serviceId);
        // DIRECT is supported
        testRunner.assertValid();
        // HTTP is supported
        testRunner.assertValid();
        // SOCKS is not supported
        testRunner.assertNotValid();
    }

}
