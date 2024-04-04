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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.mock.MockAccessPolicyProvider;
import org.apache.nifi.authorization.mock.MockAuthorizer;
import org.apache.nifi.authorization.mock.MockUserGroupProvider;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizerFactoryBeanTest {
    private static final String AUTHORIZER_ID = "authorizer";

    private static final String AUTHORIZERS_PATH = "/authorizers.xml";

    private static final AuthorizationRequest REQUEST = new AuthorizationRequest.Builder()
            .resource(ResourceFactory.getFlowResource())
            .action(RequestAction.READ)
            .accessAttempt(true)
            .anonymous(true)
            .build();

    @Mock
    NiFiProperties properties;

    @Mock
    ExtensionManager extensionManager;

    @Mock
    Bundle bundle;

    @Test
    void testGetObjectDefaultAuthorizerRequestApproved() throws Exception {
        when(properties.getSslPort()).thenReturn(null);

        final AuthorizerFactoryBean bean = new AuthorizerFactoryBean();
        bean.setProperties(properties);

        final Authorizer authorizer = bean.getObject();

        assertNotNull(authorizer);

        final AuthorizationResult authorizationResult = authorizer.authorize(REQUEST);
        assertEquals(AuthorizationResult.approved(), authorizationResult);
    }

    @Test
    void testGetObject() throws Exception {
        when(properties.getSslPort()).thenReturn(8443);
        when(properties.getProperty(eq(NiFiProperties.SECURITY_USER_AUTHORIZER))).thenReturn(AUTHORIZER_ID);
        when(properties.getAuthorizerConfigurationFile()).thenReturn(getAuthorizersConfigurationFile());

        when(bundle.getClassLoader()).thenReturn(getClass().getClassLoader());
        final List<Bundle> bundles = Collections.singletonList(bundle);

        when(extensionManager.getBundles(eq(MockUserGroupProvider.class.getName()))).thenReturn(bundles);
        when(extensionManager.getBundles(eq(MockAccessPolicyProvider.class.getName()))).thenReturn(bundles);
        when(extensionManager.getBundles(eq(MockAuthorizer.class.getName()))).thenReturn(bundles);

        final AuthorizerFactoryBean bean = new AuthorizerFactoryBean();
        bean.setProperties(properties);
        bean.setExtensionManager(extensionManager);

        final Authorizer authorizer = bean.getObject();

        assertNotNull(authorizer);
    }

    private File getAuthorizersConfigurationFile() {
        final URL url = AuthorizerFactoryBeanTest.class.getResource(AUTHORIZERS_PATH);
        if (url == null) {
            throw new IllegalStateException(String.format("Authorizers [%s] not found", AUTHORIZERS_PATH));
        }
        return new File(url.getPath());
    }
}
