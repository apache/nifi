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
package org.apache.nifi.authentication.single.user;

import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.single.user.encoder.PasswordEncoder;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class SingleUserLoginIdentityProviderTest {
    private static final String BLANK_PROVIDERS = "/conf/login-identity-providers.xml";

    private static final String XML_SUFFIX = ".xml";

    private static final String EMPTY_PROPERTIES_PATH = "";

    private static final Pattern USERNAME_PATTERN = Pattern.compile("Username\">([^<]+)<");

    private static final Pattern PASSWORD_PATTERN = Pattern.compile("Password\">([^<]+)<");

    private static final int FIRST_GROUP = 1;

    @Mock
    private LoginIdentityProviderConfigurationContext configurationContext;

    private StringPasswordEncoder encoder;

    private SingleUserLoginIdentityProvider provider;

    @Before
    public void setProvider() {
        provider = new SingleUserLoginIdentityProvider();
        encoder = new StringPasswordEncoder();
        provider.passwordEncoder = encoder;
    }

    @Test
    public void testOnConfiguredGeneratePasswordAuthenticateSuccess() throws IOException, URISyntaxException {
        final Path configuredProvidersPath = Files.createTempFile(getClass().getSimpleName(), XML_SUFFIX);
        configuredProvidersPath.toFile().deleteOnExit();

        final Path blankProvidersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        Files.copy(blankProvidersPath, configuredProvidersPath, StandardCopyOption.REPLACE_EXISTING);

        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, configuredProvidersPath.toString());

        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        provider.setProperties(niFiProperties);
        provider.onConfigured(configurationContext);

        final String providersConfiguration = new String(Files.readAllBytes(configuredProvidersPath));

        final Matcher usernameMatcher = USERNAME_PATTERN.matcher(providersConfiguration);
        assertTrue("Username not found", usernameMatcher.find());
        final String username = usernameMatcher.group(FIRST_GROUP);

        final Matcher passwordMatcher = PASSWORD_PATTERN.matcher(providersConfiguration);
        assertTrue("Password not found", passwordMatcher.find());

        final LoginCredentials loginCredentials = new LoginCredentials(username, encoder.encoded);
        final AuthenticationResponse response = provider.authenticate(loginCredentials);
        assertEquals(username, response.getUsername());
    }

    @Test
    public void testOnConfiguredAuthenticateInvalidPassword() throws URISyntaxException {
        final Path blankProvidersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, blankProvidersPath.toString());
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        provider.setProperties(niFiProperties);

        final String username = String.class.getSimpleName();
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.USERNAME_PROPERTY))).thenReturn(username);
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.PASSWORD_PROPERTY))).thenReturn(String.class.getName());
        provider.onConfigured(configurationContext);

        final LoginCredentials loginCredentials = new LoginCredentials(username, LoginCredentials.class.getName());
        assertThrows(InvalidLoginCredentialsException.class, () -> provider.authenticate(loginCredentials));
    }

    @Test
    public void testOnConfiguredAuthenticateInvalidUsername() throws URISyntaxException {
        final Path blankProvidersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, blankProvidersPath.toString());
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        provider.setProperties(niFiProperties);

        final String username = String.class.getSimpleName();
        final String password = String.class.getName();
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.USERNAME_PROPERTY))).thenReturn(username);
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.PASSWORD_PROPERTY))).thenReturn(password);
        provider.onConfigured(configurationContext);

        final LoginCredentials loginCredentials = new LoginCredentials(LoginCredentials.class.getName(), password);
        assertThrows(InvalidLoginCredentialsException.class, () -> provider.authenticate(loginCredentials));

    }

    @Test
    public void testOnConfiguredAuthenticateSuccess() throws URISyntaxException {
        final Path blankProvidersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        final Properties properties = new Properties();
        properties.put(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, blankProvidersPath.toString());
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(EMPTY_PROPERTIES_PATH, properties);
        provider.setProperties(niFiProperties);

        final String username = String.class.getSimpleName();
        final String password = String.class.getName();
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.USERNAME_PROPERTY))).thenReturn(username);
        when(configurationContext.getProperty(eq(SingleUserLoginIdentityProvider.PASSWORD_PROPERTY))).thenReturn(password);
        provider.onConfigured(configurationContext);

        final LoginCredentials loginCredentials = new LoginCredentials(username, password);
        final AuthenticationResponse response = provider.authenticate(loginCredentials);
        assertEquals(username, response.getUsername());
    }

    private static class StringPasswordEncoder implements PasswordEncoder {
        private String encoded;

        @Override
        public String encode(final char[] password) {
            encoded = new String(password);
            return encoded;
        }

        @Override
        public boolean matches(final char[] password, final String encodedPassword) {
            return encodedPassword.equals(new String(password));
        }
    }
}
