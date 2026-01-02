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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URISyntaxException;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.X509ExtendedKeyManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
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
    private SSLContextProvider sslContextServiceProvider;

    @Mock
    private WebClientService mockWebClientService;

    @Mock
    private X509ExtendedKeyManager keyManager;

    @Mock
    private HttpResponseEntity mockResponseEntity;

    @Mock
    private ComponentLog mockLogger;

    private JWTBearerOAuth2AccessTokenProviderForTests provider;
    private Processor processor = new NoOpProcessor();
    private TestRunner runner;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(processor);
        provider = new JWTBearerOAuth2AccessTokenProviderForTests();
        runner.addControllerService("oauthTokenProvider", provider);

        // Mock PropertyValue for WEB_CLIENT_SERVICE
        PropertyValue webClientServicePropertyValue = mock(PropertyValue.class);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.WEB_CLIENT_SERVICE))
                .thenReturn(webClientServicePropertyValue);

        // Mock WebClientService
        lenient().when(webClientServicePropertyValue.asControllerService(WebClientServiceProvider.class))
                .thenReturn(mockWebClientServiceProvider);

        lenient().when(mockWebClientServiceProvider.getWebClientService()).thenReturn(mockWebClientService);
        lenient().when(mockWebClientServiceProvider.getIdentifier()).thenReturn("webClientService");

        runner.addControllerService("webClientService", mockWebClientServiceProvider);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.WEB_CLIENT_SERVICE, "webClientService");
        runner.enableControllerService(mockWebClientServiceProvider);

        setContext();
    }

    @Test
    void testClaimsAreSetProperly() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);

        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS256.getName());
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SCOPE, "TestScope");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.JTI, "TestJTI");

        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.CLAIM_PREFIX + "customClaim1", "customClaimValue1");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.CLAIM_PREFIX + "customClaim2", "customClaimValue2");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.FORM_PREFIX + "clientId", "clientId");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.FORM_PREFIX + "clientSecret", "clientSecret");

        runner.enableControllerService(provider);

        // Generate the JWT using the verify method to confirm it works
        final List<ConfigVerificationResult> configVerifResults = runner.verify(provider, Map.of());

        // all configuration results should be successful
        assertFalse(configVerifResults.isEmpty());
        assertEquals(0, configVerifResults.stream().filter(result -> !result.getOutcome().equals(Outcome.SUCCESSFUL)).count());

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

    @Test
    void testJtiClaimIsUniqueWhenUuidIsUsed() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);

        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.JTI, "${UUID()}");

        runner.enableControllerService(provider);

        // generate 2 JWTs using getAccessDetails() method and then access the claims via the test controller service
        provider.getAccessDetails();
        JWTClaimsSet claimsSet1 = provider.getJwtClaimsSet();
        provider.getAccessDetails();
        JWTClaimsSet claimsSet2 = provider.getJwtClaimsSet();

        // assert that the 'jti' claims are different in the 2 JWTs
        assertNotEquals(claimsSet1.getJWTID(), claimsSet2.getJWTID());
    }

    @Test
    void testES512vsRSA() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.ES512.getName());
        final Collection<ValidationResult> validations = runner.validate(provider);
        assertEquals(1, validations.size());
        assertTrue(validations.stream().anyMatch(validation -> validation.getSubject().equals(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM.getDisplayName())));
    }

    @Test
    void testRS512vsRSA() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS512.getName());
        runner.assertValid(provider);
    }

    @Test
    void testRS512vsRSAwithoutX5T() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS512.getName());
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.HEADER_X5T, Boolean.FALSE.toString());
        runner.assertValid(provider);
    }

    @Test
    void testRS512vsRSAwithX5TNoSSL() throws Exception {
        setPrivateKeyMock(RSAPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS512.getName());
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.HEADER_X5T, Boolean.TRUE.toString());
        runner.assertNotValid(provider);
        final Collection<ValidationResult> validations = runner.validate(provider);
        assertEquals(1, validations.size());
        // SSL context provider should be present since x5t property is set to true
        assertTrue(validations.stream().anyMatch(validation -> validation.getSubject().equals(JWTBearerOAuth2AccessTokenProvider.SSL_CONTEXT_PROVIDER.getDisplayName())));
    }

    @Test
    void testRS512vsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS512.getName());
        final Collection<ValidationResult> validations = runner.validate(provider);
        assertEquals(1, validations.size());
        assertTrue(validations.stream().anyMatch(validation -> validation.getSubject().equals(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM.getDisplayName())));
    }

    @Test
    void testX5TvsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.RS512.getName());
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.HEADER_X5T, Boolean.TRUE.toString());

        PropertyValue sslServicePropertyValue = mock(PropertyValue.class);
        when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.SSL_CONTEXT_PROVIDER))
                .thenReturn(sslServicePropertyValue);

        lenient().when(sslServicePropertyValue.asControllerService(SSLContextProvider.class))
                .thenReturn(sslContextServiceProvider);

        lenient().when(sslContextServiceProvider.createKeyManager()).thenReturn(Optional.of(keyManager));
        lenient().when(sslContextServiceProvider.getIdentifier()).thenReturn("sslService");

        runner.addControllerService("sslService", sslContextServiceProvider);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SSL_CONTEXT_PROVIDER, "sslService");
        runner.enableControllerService(sslContextServiceProvider);

        final Collection<ValidationResult> validations = runner.validate(provider);
        assertEquals(2, validations.size());
        assertTrue(validations.stream().anyMatch(validation -> validation.getSubject().equals(JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM.getDisplayName())));
        assertTrue(validations.stream().anyMatch(validation -> validation.getSubject().equals(JWTBearerOAuth2AccessTokenProvider.HEADER_X5T.getDisplayName())));
    }

    @Test
    void testES512vsEC() throws Exception {
        setPrivateKeyMock(ECPrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.ES512.getName());
        runner.assertValid(provider);
    }

    @Test
    void testEdvsOctet() throws Exception {
        setPrivateKeyMock(PrivateKey.class);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SIGNING_ALGORITHM, JWSAlgorithm.Ed25519.getName());
        runner.assertValid(provider);
    }

    private void setContext() {
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.ISSUER, "TestIssuer");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.SUBJECT, "TestSubject");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.AUDIENCE, "TestAudience1 TestAudience2");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.JTI, "TestJTI");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.TOKEN_ENDPOINT, "http://example.com/token");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.GRANT_TYPE, "urn:ietf:params:oauth:grant-type:jwt-bearer");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.ASSERTION, "customAssertionField");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.JWT_VALIDITY, "1 hour");
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.REFRESH_WINDOW, "5 minutes");
    }

    private void setPrivateKeyMock(Class<? extends PrivateKey> pkClass) throws InitializationException {
        // Mock the PrivateKeyService directly
        PrivateKey mockPrivateKey = mock(pkClass);
        PrivateKeyService keyService = mock(PrivateKeyService.class);
        lenient().when(keyService.getPrivateKey()).thenReturn(mockPrivateKey);

        // Mock PropertyValue for PRIVATE_KEY_SERVICE
        PropertyValue privateKeyServicePropertyValue = mock(PropertyValue.class);
        lenient().when(mockContext.getProperty(JWTBearerOAuth2AccessTokenProvider.PRIVATE_KEY_SERVICE))
                .thenReturn(privateKeyServicePropertyValue);
        lenient().when(mockValidationContext.getProperty(JWTBearerOAuth2AccessTokenProvider.PRIVATE_KEY_SERVICE))
                .thenReturn(privateKeyServicePropertyValue);

        // When asControllerService() is called on the PropertyDescriptor, return the
        // mock PrivateKeyService
        lenient().when(privateKeyServicePropertyValue.asControllerService(PrivateKeyService.class)).thenReturn(keyService);
        lenient().when(keyService.getIdentifier()).thenReturn("keyService");

        runner.addControllerService("keyService", keyService);
        runner.setProperty(provider, JWTBearerOAuth2AccessTokenProvider.PRIVATE_KEY_SERVICE, "keyService");
        runner.enableControllerService(keyService);
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
