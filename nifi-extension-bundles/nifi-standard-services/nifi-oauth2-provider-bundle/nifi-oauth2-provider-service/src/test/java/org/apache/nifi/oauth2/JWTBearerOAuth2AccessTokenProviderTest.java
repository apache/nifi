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
package org.apache.nifi.oauth2;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URISyntaxException;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JWTBearerOAuth2AccessTokenProviderTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext mockContext;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ValidationContext mockValidationContext;

    @Mock
    private PrivateKeyService mockKeyService;

    @Mock
    private WebClientServiceProvider mockWebClientServiceProvider;

    @Mock
    private WebClientService mockWebClientService;

    @Mock
    private HttpResponseEntity mockResponseEntity;

    @Mock
    private ComponentLog mockLogger;

    private JWTBearerOAuth2AccessTokenProviderForTests provider;

    @BeforeEach
    void setUp() {
        // Initialize mocks
        MockitoAnnotations.openMocks(this);
        provider = new JWTBearerOAuth2AccessTokenProviderForTests();

        // Mock PropertyValue for WEB_CLIENT_SERVICE
        PropertyValue webClientServicePropertyValue = mock(PropertyValue.class);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.WEB_CLIENT_SERVICE))
                .thenReturn(webClientServicePropertyValue);

        // Mock WebClientService
        when(webClientServicePropertyValue.asControllerService(WebClientServiceProvider.class))
                .thenReturn(mockWebClientServiceProvider);

        when(mockWebClientServiceProvider.getWebClientService()).thenReturn(mockWebClientService);
    }

    @Test
    void testClaimsAreSetProperly() throws Exception {
        // Setup context
        setContext();

        setPrivateKeyMock(RSAPrivateKey.class);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.RS256.getName());

        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SCOPE).isSet()).thenReturn(true);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SCOPE).evaluateAttributeExpressions().getValue()).thenReturn("TestScope");

        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.NBF).asBoolean()).thenReturn(true);

        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.JTI).isSet()).thenReturn(true);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.JTI).evaluateAttributeExpressions().getValue()).thenReturn("TestJTI");

        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.HEADER_TYPE).asBoolean()).thenReturn(true);

        PropertyDescriptor customClaim1 = setDynamicPropertyMock(JWTBearerOAuth2AccessTokenProvider.CLAIM_PREFIX + "customClaim1", "customClaimValue1");
        PropertyDescriptor customClaim2 = setDynamicPropertyMock(JWTBearerOAuth2AccessTokenProvider.CLAIM_PREFIX + "customClaim2", "customClaimValue2");
        PropertyDescriptor customFormParam1 = setDynamicPropertyMock(JWTBearerOAuth2AccessTokenProvider.FORM_PREFIX + "clientId", "clientId");
        PropertyDescriptor customFormParam2 = setDynamicPropertyMock(JWTBearerOAuth2AccessTokenProvider.FORM_PREFIX + "clientSecret", "clientSecret");

        // dynamic properties
        when(mockContext.getProperties().keySet()).thenReturn(Set.of(
                customClaim1, customClaim2, customFormParam1, customFormParam2));

        // Call onEnabled to apply these properties
        provider.onEnabled(mockContext);

        // Generate the JWT
        provider.getAccessDetails();

        // Validate the claims are properly set
        JWTClaimsSet claimsSet = provider.getJwtClaimsSet();
        assertEquals("TestIssuer", claimsSet.getIssuer());
        assertEquals("TestSubject", claimsSet.getSubject());
        assertEquals("TestAudience1", claimsSet.getAudience().get(0));
        assertEquals("TestAudience2", claimsSet.getAudience().get(1));
        assertEquals("TestScope", claimsSet.getStringClaim("scope"));
        assertEquals("TestJTI", claimsSet.getJWTID());
        assertNotNull(claimsSet.getNotBeforeTime());

        // validate custom claims
        assertEquals("customClaimValue1", claimsSet.getStringClaim("customClaim1"));
        assertEquals("customClaimValue2", claimsSet.getStringClaim("customClaim2"));

        // validate the header
        JWSHeader jwsHeader = provider.getJwsHeader();
        assertEquals(JWSAlgorithm.RS256, jwsHeader.getAlgorithm());
        assertEquals("{\"typ\":\"JWT\",\"alg\":\"RS256\"}", jwsHeader.toString());

        // validate the form parameters
        Map<String, String> formParams = provider.getFormParams();
        assertEquals("test-assertion", formParams.get("customAssertionField"));
        assertEquals("clientId", formParams.get("clientId"));
        assertEquals("clientSecret", formParams.get("clientSecret"));
        assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", formParams.get("grant_type"));
        assertEquals(4, formParams.size());
    }

    private PropertyDescriptor setDynamicPropertyMock(final String key, final String value) {
        PropertyDescriptor mock = mock(PropertyDescriptor.class);
        when(mock.getName()).thenReturn(key);
        when(mock.isDynamic()).thenReturn(true);
        when(mock.isExpressionLanguageSupported()).thenReturn(true);
        when(mockContext.getProperty(mock).evaluateAttributeExpressions().getValue()).thenReturn(value);
        return mock;
    }

    @Test
    void testES512vsRSA() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.ES512.getName());

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(1, results.size());
    }

    @Test
    void testRS512vsRSA() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.RS512.getName());

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(0, results.size());
    }

    @Test
    void testRS512vsRSAwithX5T() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.RS512.getName());
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.HEADER_X5T).asBoolean()).thenReturn(true);

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(0, results.size());
    }

    @Test
    void testRS512vsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.RS512.getName());

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(1, results.size());
    }

    @Test
    void testX5TvsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.RS512.getName());
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.HEADER_X5T).asBoolean()).thenReturn(true);

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(2, results.size());
    }

    @Test
    void testES512vsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.ES512.getName());

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(0, results.size());
    }

    @Test
    void testEdvsOctet() throws Exception {
        setPrivateKeyMock(PrivateKey.class);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM).getValue()).thenReturn(JWSAlgorithm.Ed25519.getName());

        Collection<ValidationResult> results = provider.customValidate(mockValidationContext);
        assertEquals(0, results.size());
    }

    private void setContext() {
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.ISSUER).isSet()).thenReturn(true);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.ISSUER).evaluateAttributeExpressions().getValue()).thenReturn("TestIssuer");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SUBJECT).isSet()).thenReturn(true);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SUBJECT).evaluateAttributeExpressions().getValue()).thenReturn("TestSubject");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.AUDIENCE).isSet()).thenReturn(true);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.AUDIENCE).evaluateAttributeExpressions().getValue()).thenReturn("TestAudience1 TestAudience2");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.NBF).getValue()).thenReturn("true");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.JTI).getValue()).thenReturn("TestJTI");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.TOKEN_ENDPOINT).getValue()).thenReturn("http://example.com/token");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.GRANT_TYPE).evaluateAttributeExpressions().getValue()).thenReturn("urn:ietf:params:oauth:grant-type:jwt-bearer");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.ASSERTION).evaluateAttributeExpressions().getValue()).thenReturn("customAssertionField");
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.JWT_VALIDITY).asDuration()).thenReturn(Duration.ofHours(1));
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.REFRESH_WINDOW).asDuration()).thenReturn(Duration.ofMinutes(5));
    }

    private void setPrivateKeyMock(Class<? extends PrivateKey> pkClass) {
        // Mock the PrivateKeyService directly
        PrivateKey mockPrivateKey = mock(pkClass);
        PrivateKeyService keyService = mock(PrivateKeyService.class);
        when(keyService.getPrivateKey()).thenReturn(mockPrivateKey);

        // Mock PropertyValue for PRIVATE_KEY_SERVICE
        PropertyValue privateKeyServicePropertyValue = mock(PropertyValue.class);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.PRIVATE_KEY_SERVICE))
                .thenReturn(privateKeyServicePropertyValue);
        when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.PRIVATE_KEY_SERVICE))
                .thenReturn(privateKeyServicePropertyValue);

        // When asControllerService() is called on the PropertyDescriptor, return the
        // mock PrivateKeyService
        when(privateKeyServicePropertyValue.asControllerService(PrivateKeyService.class)).thenReturn(keyService);
    }

    private class JWTBearerOAuth2AccessTokenProviderForTests extends JWTBearerOAuth2AccessTokenProvider {
        private JWSHeader jwsHeader;
        private JWTClaimsSet jwtClaimsSet;
        private Map<String, String> formParams;

        @Override
        protected ComponentLog getLogger() {
            return mockLogger;
        }

        @Override
        protected String getAssertion(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) throws JOSEException {
            this.jwsHeader = jwsHeader;
            this.jwtClaimsSet = jwtClaimsSet;
            return "test-assertion";
        }

        @Override
        protected void requestTokenEndpoint(Map<String, String> formParams) throws URISyntaxException {
            this.formParams = formParams;
        }

        public JWSHeader getJwsHeader() {
            return jwsHeader;
        }

        public JWTClaimsSet getJwtClaimsSet() {
            return jwtClaimsSet;
        }

        public Map<String, String> getFormParams() {
            return formParams;
        }
    }

}
