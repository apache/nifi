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

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardOauth2AccessTokenProviderTest {
    private static final String AUTHORIZATION_SERVER_URL = "http://authorizationServerUrl";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_SECRET = "clientSecret";
    private static final String SCOPE = "scope";
    private static final String RESOURCE = "resource";
    private static final String AUDIENCE = "audience";
    private static final long FIVE_MINUTES = 300;

    private static final int HTTP_OK = 200;
    private static final int HTTP_ACCEPTED = 201;

    private StandardOauth2AccessTokenProvider testSubject;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private OkHttpClient mockHttpClient;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext mockContext;

    @Mock
    private ComponentLog mockLogger;
    @Captor
    private ArgumentCaptor<String> errorCaptor;
    @Captor
    private ArgumentCaptor<Throwable> throwableCaptor;
    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    void mockSetup() {
        testSubject = new StandardOauth2AccessTokenProvider() {
            @Override
            protected OkHttpClient createHttpClient(final ConfigurationContext context) {
                return mockHttpClient;
            }

            @Override
            protected ComponentLog getLogger() {
                return mockLogger;
            }
        };

        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.GRANT_TYPE).getValue()).thenReturn(StandardOauth2AccessTokenProvider.RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE.getValue());
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL).evaluateAttributeExpressions().getValue()).thenReturn(AUTHORIZATION_SERVER_URL);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.USERNAME).evaluateAttributeExpressions().getValue()).thenReturn(USERNAME);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.PASSWORD).getValue()).thenReturn(PASSWORD);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.CLIENT_ID).evaluateAttributeExpressions().getValue()).thenReturn(CLIENT_ID);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.CLIENT_SECRET).getValue()).thenReturn(CLIENT_SECRET);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.SCOPE).getValue()).thenReturn(SCOPE);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.RESOURCE).getValue()).thenReturn(RESOURCE);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.AUDIENCE).getValue()).thenReturn(AUDIENCE);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.REFRESH_WINDOW).asTimePeriod(eq(TimeUnit.SECONDS))).thenReturn(FIVE_MINUTES);
        when(mockContext.getProperty(StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY).getValue()).thenReturn(ClientAuthenticationStrategy.BASIC_AUTHENTICATION.getValue());
        when(mockContext.getProperties()).thenReturn(Collections.emptyMap());
    }

    @Nested
    class WithEnabledControllerService {
        @BeforeEach
        public void setUp() {
            mockSetup();
            testSubject.onEnabled(mockContext);
        }

        @Test
        public void testInvalidWhenClientCredentialsGrantTypeSetWithoutClientId() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);

            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);

            // WHEN
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE);

            // THEN
            runner.assertNotValid(testSubject);
        }

        @Test
        public void testValidWhenClientCredentialsGrantTypeSetWithClientId() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);

            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);

            // WHEN
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_ID, CLIENT_ID);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_SECRET, CLIENT_SECRET);

            // THEN
            runner.assertValid(testSubject);
        }

        @Test
        public void testInvalidWhenClientAuthenticationStrategyIsInvalid() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_ID, CLIENT_ID);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_SECRET, CLIENT_SECRET);

            // WHEN
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY, "UNKNOWN");

            // THEN
            runner.assertNotValid(testSubject);
        }

        @Test
        public void testInvalidWhenRefreshTokenGrantTypeSetWithoutRefreshToken() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.REFRESH_TOKEN_GRANT_TYPE);

            // THEN
            runner.assertNotValid(testSubject);
        }

        @Test
        public void testValidWhenRefreshTokenGrantTypeSetWithRefreshToken() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.REFRESH_TOKEN_GRANT_TYPE);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.REFRESH_TOKEN, "refresh_token");

            // THEN
            runner.assertValid(testSubject);
        }

        @Test
        public void testAcquireNewTokenWhenGrantTypeIsRefreshToken() throws Exception {
            // GIVEN
            final String refreshToken = "refresh_token_123";
            final String accessToken = "access_token_123";

            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.REFRESH_TOKEN_GRANT_TYPE);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.REFRESH_TOKEN, refreshToken);

            runner.enableControllerService(testSubject);

            final Response response = buildResponse(HTTP_OK, "{\"access_token\":\"" + accessToken + "\"}");
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);

            // WHEN
            final String actualAccessToken = testSubject.getAccessDetails().getAccessToken();

            // THEN
            verify(mockHttpClient, atLeast(1)).newCall(requestCaptor.capture());
            final FormBody capturedRequestBody = (FormBody) requestCaptor.getValue().body();

            assertEquals("grant_type", capturedRequestBody.encodedName(0));
            assertEquals("refresh_token", capturedRequestBody.encodedValue(0));

            assertEquals("refresh_token", capturedRequestBody.encodedName(1));
            assertEquals("refresh_token_123", capturedRequestBody.encodedValue(1));

            assertEquals(accessToken, actualAccessToken);
        }

        @Test
        public void testValidWhenClientAuthenticationStrategyIsValid() throws Exception {
            // GIVEN
            final Processor processor = new NoOpProcessor();
            final TestRunner runner = TestRunners.newTestRunner(processor);

            runner.addControllerService("testSubject", testSubject);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.GRANT_TYPE, StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_ID, CLIENT_ID);
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_SECRET, CLIENT_SECRET);

            // WHEN
            runner.setProperty(testSubject, StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY, ClientAuthenticationStrategy.REQUEST_BODY.getValue());

            // THEN
            runner.assertValid(testSubject);
        }

        @Test
        public void testAcquireNewToken() throws Exception {
            final String accessTokenValue = "access_token_value";
            final String scopeValue = "scope_value";
            final String customFieldKey = "custom_field_key";
            final String customFieldValue = "custom_field_value";

            // GIVEN
            final Response response = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"" + accessTokenValue + "\", \"scope\":\"" + scopeValue + "\", \"" + customFieldKey + "\":\"" + customFieldValue + "\" }"
            );

            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);

            // WHEN
            final AccessToken accessToken = testSubject.getAccessDetails();

            // THEN
            assertEquals(accessTokenValue, accessToken.getAccessToken());
            assertEquals(scopeValue, accessToken.getScope());
            assertEquals(customFieldValue, accessToken.getAdditionalParameters().get(customFieldKey));
        }

        @Test
        public void testRefreshToken() throws Exception {
            // GIVEN
            final String firstToken = "first_token";
            final String expectedToken = "second_token";

            final Response response1 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"" + firstToken + "\", \"expires_in\":\"0\", \"refresh_token\":\"not_checking_in_this_test\" }"
            );

            final Response response2 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"" + expectedToken + "\" }"
            );

            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response1, response2);

            // WHEN
            testSubject.getAccessDetails();
            final String actualToken = testSubject.getAccessDetails().getAccessToken();

            // THEN
            assertEquals(expectedToken, actualToken);
        }

        @Test
        public void testKeepPreviousRefreshTokenWhenNewOneIsNotProvided() throws Exception {
            // GIVEN
            final String refreshTokenBeforeRefresh = "refresh_token";

            final Response response1 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"not_checking_in_this_test\", \"expires_in\":\"0\", \"refresh_token\":\"" + refreshTokenBeforeRefresh + "\" }"
            );

            final Response response2 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"not_checking_in_this_test_either\" }"
            );

            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response1, response2);

            // WHEN
            testSubject.getAccessDetails();
            final String refreshTokenAfterRefresh = testSubject.getAccessDetails().getRefreshToken();

            // THEN
            assertEquals(refreshTokenBeforeRefresh, refreshTokenAfterRefresh);
        }

        @Test
        public void testOverwritePreviousRefreshTokenWhenNewOneIsProvided() throws Exception {
            // GIVEN
            final String refreshTokenBeforeRefresh = "refresh_token_before_refresh";
            final String expectedRefreshTokenAfterRefresh = "refresh_token_after_refresh";

            final Response response1 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"not_checking_in_this_test\", \"expires_in\":\"0\", \"refresh_token\":\"" + refreshTokenBeforeRefresh + "\" }"
            );

            final Response response2 = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"not_checking_in_this_test_either\", \"refresh_token\":\"" + expectedRefreshTokenAfterRefresh + "\" }"
            );

            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response1, response2);

            // WHEN
            testSubject.getAccessDetails();
            final String actualRefreshTokenAfterRefresh = testSubject.getAccessDetails().getRefreshToken();

            // THEN
            assertEquals(expectedRefreshTokenAfterRefresh, actualRefreshTokenAfterRefresh);
        }

        @Test
        public void testBasicAuthentication() throws Exception {
            // GIVEN
            final Response response = buildResponse(HTTP_OK, "{\"access_token\":\"foobar\"}");
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);
            final String expected = "Basic " + Base64.getEncoder().withoutPadding().encodeToString((CLIENT_ID + ":" + CLIENT_SECRET).getBytes());

            // WHEN
            testSubject.getAccessDetails();

            // THEN
            verify(mockHttpClient, atLeast(1)).newCall(requestCaptor.capture());
            assertEquals(expected, requestCaptor.getValue().header("Authorization"));
        }

        @Test
        public void testRequestBodyFormData() throws Exception {
            when(mockContext.getProperty(StandardOauth2AccessTokenProvider.GRANT_TYPE).getValue()).thenReturn(StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE.getValue());
            when(mockContext.getProperty(StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY).getValue()).thenReturn(ClientAuthenticationStrategy.REQUEST_BODY.getValue());
            testSubject.onEnabled(mockContext);

            // GIVEN
            final Response response = buildResponse(HTTP_OK, "{\"access_token\":\"foobar\"}");
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);
            final String expected = "grant_type=client_credentials&client_id=" + CLIENT_ID
                + "&client_secret=" + CLIENT_SECRET
                + "&scope=" + SCOPE
                + "&resource=" + RESOURCE
                + "&audience=" + AUDIENCE;

            // WHEN
            testSubject.getAccessDetails();

            // THEN
            final Buffer buffer = new Buffer();
            verify(mockHttpClient, atLeast(1)).newCall(requestCaptor.capture());
            requestCaptor.getValue().body().writeTo(buffer);
            assertEquals(expected, buffer.readString(Charset.defaultCharset()));
        }

        @Test
        public void testRequestBodyFormDataIncludesCustomParameters() throws Exception {
            final PropertyDescriptor accountIdDescriptor = new PropertyDescriptor.Builder()
                .name("FORM.account_id")
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build();

            when(mockContext.getProperty(StandardOauth2AccessTokenProvider.GRANT_TYPE).getValue()).thenReturn(StandardOauth2AccessTokenProvider.CLIENT_CREDENTIALS_GRANT_TYPE.getValue());
            when(mockContext.getProperty(StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY).getValue()).thenReturn(ClientAuthenticationStrategy.REQUEST_BODY.getValue());
            final Map<PropertyDescriptor, String> properties = new HashMap<>();
            properties.put(accountIdDescriptor, "12345");
            when(mockContext.getProperties()).thenReturn(properties);
            when(mockContext.getProperty(accountIdDescriptor)).thenReturn(new MockPropertyValue("12345"));

            testSubject.onEnabled(mockContext);

            final Response response = buildResponse(HTTP_OK, "{\"access_token\":\"foobar\"}");
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);

            testSubject.getAccessDetails();

            verify(mockHttpClient, atLeast(1)).newCall(requestCaptor.capture());
            final FormBody formBody = (FormBody) requestCaptor.getValue().body();
            assertNotNull(formBody);

            final Map<String, String> parameters = new HashMap<>();
            for (int i = 0; i < formBody.size(); i++) {
                parameters.put(formBody.encodedName(i), formBody.encodedValue(i));
            }

            assertEquals("client_credentials", parameters.get("grant_type"));
            assertEquals(CLIENT_ID, parameters.get("client_id"));
            assertEquals(CLIENT_SECRET, parameters.get("client_secret"));
            assertEquals(SCOPE, parameters.get("scope"));
            assertEquals(RESOURCE, parameters.get("resource"));
            assertEquals(AUDIENCE, parameters.get("audience"));
            assertEquals("12345", parameters.get("account_id"));
        }

        @Test
        public void testIOExceptionDuringRefreshAndSubsequentAcquire() throws Exception {
            // GIVEN
            final String refreshErrorMessage = "refresh_error";
            final String acquireErrorMessage = "acquire_error";

            final AtomicInteger callCounter = new AtomicInteger(0);
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenAnswer(invocation -> {
                callCounter.incrementAndGet();

                if (callCounter.get() == 1) {
                    return buildSuccessfulInitResponse();
                } else if (callCounter.get() == 2) {
                    throw new IOException(refreshErrorMessage);
                } else if (callCounter.get() == 3) {
                    throw new IOException(acquireErrorMessage);
                }

                throw new IllegalStateException("Test improperly defined mock HTTP responses.");
            });

            // Get a good accessDetails so we can have a refresh a second time
            testSubject.getAccessDetails();

            // WHEN
            final UncheckedIOException actualException = assertThrows(
                UncheckedIOException.class,
                () -> testSubject.getAccessDetails()
            );

            // THEN
            checkLoggedDebugWhenRefreshFails();

            checkLoggedRefreshError(new UncheckedIOException("OAuth2 access token request failed", new IOException(refreshErrorMessage)));

            checkError(new UncheckedIOException("OAuth2 access token request failed", new IOException(acquireErrorMessage)), actualException);
        }

        @Test
        public void testIOExceptionDuringRefreshSuccessfulSubsequentAcquire() throws Exception {
            // GIVEN
            final String refreshErrorMessage = "refresh_error";
            final String expectedToken = "expected_token";

            final Response successfulAcquireResponse = buildResponse(
                HTTP_ACCEPTED,
                "{ \"access_token\":\"" + expectedToken + "\", \"expires_in\":\"0\", \"refresh_token\":\"not_checking_in_this_test\" }"
            );

            final AtomicInteger callCounter = new AtomicInteger(0);
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenAnswer(invocation -> {
                callCounter.incrementAndGet();

                if (callCounter.get() == 1) {
                    return buildSuccessfulInitResponse();
                } else if (callCounter.get() == 2) {
                    throw new IOException(refreshErrorMessage);
                } else if (callCounter.get() == 3) {
                    return successfulAcquireResponse;
                }

                throw new IllegalStateException("Test improperly defined mock HTTP responses.");
            });

            // Get a good accessDetails so we can have a refresh a second time
            testSubject.getAccessDetails();

            // WHEN
            final String actualToken = testSubject.getAccessDetails().getAccessToken();

            // THEN
            checkLoggedDebugWhenRefreshFails();

            checkLoggedRefreshError(new UncheckedIOException("OAuth2 access token request failed", new IOException(refreshErrorMessage)));

            assertEquals(expectedToken, actualToken);
        }

        @Test
        public void testHTTPErrorDuringRefreshAndSubsequentAcquire() throws Exception {
            // GIVEN
            final String errorRefreshResponseBody = "{ \"error_response\":\"refresh_error\" }";
            final String errorAcquireResponseBody = "{ \"error_response\":\"acquire_error\" }";

            final Response errorRefreshResponse = buildResponse(500, errorRefreshResponseBody);
            final Response errorAcquireResponse = buildResponse(503, errorAcquireResponseBody);

            final AtomicInteger callCounter = new AtomicInteger(0);
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenAnswer(invocation -> {
                callCounter.incrementAndGet();

                if (callCounter.get() == 1) {
                    return buildSuccessfulInitResponse();
                } else if (callCounter.get() == 2) {
                    return errorRefreshResponse;
                } else if (callCounter.get() == 3) {
                    return errorAcquireResponse;
                }

                throw new IllegalStateException("Test improperly defined mock HTTP responses.");
            });

            final List<String> expectedLoggedError = Arrays.asList(
                String.format("OAuth2 access token request failed [HTTP %d], response:%n%s", 500, errorRefreshResponseBody),
                String.format("OAuth2 access token request failed [HTTP %d], response:%n%s", 503, errorAcquireResponseBody)
            );

            // Get a good accessDetails so we can have a refresh a second time
            testSubject.getAccessDetails();

            // WHEN
            final ProcessException actualException = assertThrows(
                ProcessException.class,
                () -> testSubject.getAccessDetails()
            );

            // THEN
            checkLoggedDebugWhenRefreshFails();

            checkLoggedRefreshError(new ProcessException("OAuth2 access token request failed [HTTP 500]"));

            checkedLoggedErrorWhenRefreshReturnsBadHTTPResponse(expectedLoggedError);

            checkError(new ProcessException("OAuth2 access token request failed [HTTP 503]"), actualException);
        }

        @Test
        public void testHTTPErrorDuringRefreshSuccessfulSubsequentAcquire() throws Exception {
            // GIVEN
            final String expectedRefreshErrorResponse = "{ \"error_response\":\"refresh_error\" }";
            final String expectedToken = "expected_token";

            final Response errorRefreshResponse = buildResponse(500, expectedRefreshErrorResponse);
            final Response successfulAcquireResponse = buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"" + expectedToken + "\", \"expires_in\":\"0\", \"refresh_token\":\"not_checking_in_this_test\" }"
            );

            final AtomicInteger callCounter = new AtomicInteger(0);
            when(mockHttpClient.newCall(any(Request.class)).execute()).thenAnswer(invocation -> {
                callCounter.incrementAndGet();

                if (callCounter.get() == 1) {
                    return buildSuccessfulInitResponse();
                } else if (callCounter.get() == 2) {
                    return errorRefreshResponse;
                } else if (callCounter.get() == 3) {
                    return successfulAcquireResponse;
                }

                throw new IllegalStateException("Test improperly defined mock HTTP responses.");
            });

            final List<String> expectedLoggedError = Collections.singletonList(String.format("OAuth2 access token request failed [HTTP %d], response:%n%s", 500, expectedRefreshErrorResponse));

            // Get a good accessDetails so we can have a refresh a second time
            testSubject.getAccessDetails();

            // WHEN
            final String actualToken = testSubject.getAccessDetails().getAccessToken();

            // THEN
            checkLoggedDebugWhenRefreshFails();

            checkLoggedRefreshError(new ProcessException("OAuth2 access token request failed [HTTP 500]"));

            checkedLoggedErrorWhenRefreshReturnsBadHTTPResponse(expectedLoggedError);

            assertEquals(expectedToken, actualToken);
        }
    }

    @Test
    public void testVerifySuccess() throws Exception {
        mockSetup();
        final Processor processor = new NoOpProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final String accessTokenValue = "access_token_value";

        // GIVEN
        final Response response = buildResponse(
            HTTP_OK,
            "{ \"access_token\":\"" + accessTokenValue + "\" }"
        );

        when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response);

        final List<ConfigVerificationResult> results = ((VerifiableControllerService) testSubject).verify(
            mockContext,
            runner.getLogger(),
            Collections.emptyMap()
        );

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(SUCCESSFUL, results.get(0).getOutcome());
    }

    @Test
    public void testVerifyError() throws Exception {
        mockSetup();
        final Processor processor = new NoOpProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        when(mockHttpClient.newCall(any(Request.class)).execute()).thenThrow(new IOException());

        final List<ConfigVerificationResult> results = ((VerifiableControllerService) testSubject).verify(
            mockContext,
            runner.getLogger(),
            Collections.emptyMap()
        );

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(FAILED, results.get(0).getOutcome());
    }

    @Test
    void testMigrateProperties() {
        testSubject = new StandardOauth2AccessTokenProvider();

        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("authorization-server-url", StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL.getName()),
                Map.entry("client-authentication-strategy", StandardOauth2AccessTokenProvider.CLIENT_AUTHENTICATION_STRATEGY.getName()),
                Map.entry("grant-type", StandardOauth2AccessTokenProvider.GRANT_TYPE.getName()),
                Map.entry("service-user-name", StandardOauth2AccessTokenProvider.USERNAME.getName()),
                Map.entry("service-password", StandardOauth2AccessTokenProvider.PASSWORD.getName()),
                Map.entry("refresh-token", StandardOauth2AccessTokenProvider.REFRESH_TOKEN.getName()),
                Map.entry("client-id", StandardOauth2AccessTokenProvider.CLIENT_ID.getName()),
                Map.entry("client-secret", StandardOauth2AccessTokenProvider.CLIENT_SECRET.getName()),
                Map.entry("Client secret", StandardOauth2AccessTokenProvider.CLIENT_SECRET.getName()),
                Map.entry("scope", StandardOauth2AccessTokenProvider.SCOPE.getName()),
                Map.entry("resource", StandardOauth2AccessTokenProvider.RESOURCE.getName()),
                Map.entry("audience", StandardOauth2AccessTokenProvider.AUDIENCE.getName()),
                Map.entry("refresh-window", StandardOauth2AccessTokenProvider.REFRESH_WINDOW.getName()),
                Map.entry("ssl-context-service", StandardOauth2AccessTokenProvider.SSL_CONTEXT_SERVICE.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        testSubject.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }

    private Response buildSuccessfulInitResponse() {
        return buildResponse(
                HTTP_OK,
                "{ \"access_token\":\"exists_but_value_irrelevant\", \"expires_in\":\"0\", \"refresh_token\":\"not_checking_in_this_test\" }"
        );
    }

    private Response buildResponse(final int code, final String body) {
        return new Response.Builder()
            .request(new Request.Builder()
                .url("http://unimportant_but_required")
                .build()
            )
            .protocol(Protocol.HTTP_2)
            .message("unimportant_but_required")
            .code(code)
            .body(ResponseBody.create(
                body.getBytes(),
                MediaType.parse("application/json"))
            )
            .build();
    }

    private void checkLoggedDebugWhenRefreshFails() {
        verify(mockLogger, times(3)).debug(anyString(), eq(AUTHORIZATION_SERVER_URL));
    }

    private void checkedLoggedErrorWhenRefreshReturnsBadHTTPResponse(final List<String> expectedLoggedError) {
        verify(mockLogger, times(expectedLoggedError.size())).error(errorCaptor.capture());
        final List<String> actualLoggedError = errorCaptor.getAllValues();

        assertEquals(expectedLoggedError, actualLoggedError);
    }

    private void checkLoggedRefreshError(final Throwable expectedRefreshError) {
        verify(mockLogger).info(anyString(), eq(AUTHORIZATION_SERVER_URL), throwableCaptor.capture());
        final Throwable actualRefreshError = throwableCaptor.getValue();

        checkError(expectedRefreshError, actualRefreshError);
    }

    private void checkError(final Throwable expectedError, final Throwable actualError) {
        assertEquals(expectedError.getClass(), actualError.getClass());
        assertEquals(expectedError.getMessage(), actualError.getMessage());
        if (expectedError.getCause() != null || actualError.getCause() != null) {
            assertEquals(expectedError.getCause().getClass(), actualError.getCause().getClass());
            assertEquals(expectedError.getCause().getMessage(), actualError.getCause().getMessage());
        }
    }
}
