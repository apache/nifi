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
package org.apache.nifi.web.security.spring;

import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.spring.mock.MockLoginIdentityProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoginIdentityProviderFactoryBeanTest {
    private static final String PROVIDERS_PATH = "/login-identity-providers.xml";

    private static final String PROVIDER_ID = "login-identity-provider";

    @Mock
    NiFiProperties properties;

    @Mock
    ExtensionManager extensionManager;

    @Mock
    Bundle bundle;

    @Test
    void testGetObjectNotConfigured() throws Exception {
        final LoginIdentityProviderFactoryBean bean = new LoginIdentityProviderFactoryBean();

        bean.setProperties(properties);
        bean.setExtensionManager(extensionManager);

        final Object object = bean.getObject();

        assertNull(object);
    }

    @Test
    void testGetObject() throws Exception {
        when(properties.getProperty(eq(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER))).thenReturn(PROVIDER_ID);
        when(properties.getLoginIdentityProviderConfigurationFile()).thenReturn(getLoginIdentityProvidersFile());

        when(bundle.getClassLoader()).thenReturn(getClass().getClassLoader());
        final List<Bundle> bundles = Collections.singletonList(bundle);

        when(extensionManager.getBundles(eq(MockLoginIdentityProvider.class.getName()))).thenReturn(bundles);

        final LoginIdentityProviderFactoryBean bean = new LoginIdentityProviderFactoryBean();

        bean.setProperties(properties);
        bean.setExtensionManager(extensionManager);

        final Object object = bean.getObject();

        assertInstanceOf(LoginIdentityProvider.class, object);
    }

    private File getLoginIdentityProvidersFile() {
        final URL url = LoginIdentityProviderFactoryBeanTest.class.getResource(PROVIDERS_PATH);
        if (url == null) {
            throw new IllegalStateException(String.format("Providers [%s] not found", PROVIDERS_PATH));
        }
        return new File(url.getPath());
    }
}
