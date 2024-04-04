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
package org.apache.nifi.authorization.single.user;

import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SingleUserAuthorizerTest {
    private static final String BLANK_PROVIDERS = "/conf/login-identity-providers.xml";

    private static final String UNSUPPORTED_PROVIDERS = "/conf/unsupported-login-identity-providers.xml";

    private static final String PROVIDER_IDENTIFIER = "single-user-provider";

    private static final String UNSUPPORTED_PROVIDER_IDENTIFIER = "unsupported-provider";

    private static final String AUTHORIZER_IDENTIFIER = "single-user-authorizer";

    private static final String OTHER_AUTHORIZER_IDENTIFIER = "other-authorizer";

    private static final String EMPTY_PROPERTIES_PATH = "";

    private SingleUserAuthorizer authorizer;

    @Mock
    private AuthorizerInitializationContext initializationContext;

    @BeforeEach
    public void setAuthorizer() {
        authorizer = new SingleUserAuthorizer();
        when(initializationContext.getIdentifier()).thenReturn(AUTHORIZER_IDENTIFIER);
    }

    @Test
    public void testInitializeSingleUserAuthorizerNotConfigured() throws URISyntaxException {
        final Path providersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, providersPath.toString());
        properties.put(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER, PROVIDER_IDENTIFIER);
        properties.put(NiFiProperties.SECURITY_USER_AUTHORIZER, OTHER_AUTHORIZER_IDENTIFIER);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        authorizer.setProperties(niFiProperties);
        authorizer.initialize(initializationContext);
    }

    @Test
    public void testInitializeSingleUserIdentityProviderConfigured() throws URISyntaxException {
        final Path providersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, providersPath.toString());
        properties.put(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER, PROVIDER_IDENTIFIER);
        properties.put(NiFiProperties.SECURITY_USER_AUTHORIZER, AUTHORIZER_IDENTIFIER);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        authorizer.setProperties(niFiProperties);
        authorizer.initialize(initializationContext);
    }

    @Test
    public void testInitializeSingleUserIdentityProviderNotSpecified() throws URISyntaxException {
        final Path providersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, providersPath.toString());
        properties.put(NiFiProperties.SECURITY_USER_AUTHORIZER, AUTHORIZER_IDENTIFIER);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        authorizer.setProperties(niFiProperties);
        assertThrows(AuthorizerCreationException.class, () -> authorizer.initialize(initializationContext));
    }

    @Test
    public void testInitializeAuthorizerCreationException() throws URISyntaxException {
        final Path providersPath = Paths.get(getClass().getResource(UNSUPPORTED_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, providersPath.toString());
        properties.put(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER, UNSUPPORTED_PROVIDER_IDENTIFIER);
        properties.put(NiFiProperties.SECURITY_USER_AUTHORIZER, AUTHORIZER_IDENTIFIER);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        authorizer.setProperties(niFiProperties);
        assertThrows(AuthorizerCreationException.class, () -> authorizer.initialize(initializationContext));
    }
}
