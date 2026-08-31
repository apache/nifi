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
package org.apache.nifi.processors.gcp.cloudsql;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.GCP_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.SQLSERVICE_LOGIN_SCOPE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_CREDENTIALS_UNAVAILABLE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_IMPERSONATION_REQUIRED;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_SCOPE_STEP;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_TOKEN_ACQUISITION_FAILED;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_TOKEN_MISSING;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.VERIFY_TOKEN_STEP;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpCloudSqlIamDatabasePasswordProviderTest {

    private static final String CREDENTIALS_SERVICE_ID = "gcpCredentials";
    private static final String PASSWORD_PROVIDER_ID = "cloudSqlIamProvider";
    private static final String DRIVER_CLASS = "org.postgresql.Driver";
    private static final String DATABASE_USER = "service-account@test-project.iam";
    private static final String JDBC_URL = "jdbc:postgresql://example:5432/database";
    private static final String TOKEN_VALUE = "cloud-sql-token";
    private static final String REFRESHED_TOKEN_VALUE = "refreshed-cloud-sql-token";
    private static final String LEAK_SENTINEL = "sentinel-token-value";

    private ExecutorService executorService;

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void testSupportedPropertyDescriptorsContainOnlyCredentialsService() throws Exception {
        final List<PropertyDescriptor> descriptors = getSupportedPropertyDescriptors(new GcpCloudSqlIamDatabasePasswordProvider());

        assertEquals(1, descriptors.size());
        assertEquals(GCP_CREDENTIALS_PROVIDER_SERVICE, descriptors.get(0));
        assertTrue(descriptors.get(0).isRequired());
    }

    @Test
    void testOnEnabledCachesScopedCredentialAndReusesIt() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        assertEquals(1, rootCredentials.getCreateScopedCount());
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
        assertSame(scopedCredentials, getScopedCredentials(provider));
        assertEquals(TOKEN_VALUE, new String(provider.getPassword(requestContext())));
        assertEquals(TOKEN_VALUE, new String(provider.getPassword(requestContext())));
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testOnDisabledClearsCachedCredential() throws Exception {
        final TestRunner runner = configureRunner(new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15))), true);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        runner.disableControllerService(provider);

        assertNull(getScopedCredentials(provider));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertTrue(exception.getMessage().contains("Cloud SQL IAM"));
        assertNull(exception.getCause());
    }

    @Test
    void testVerifyImpersonatedCredentialsAcquireLiveToken() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL, "impersonation");
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL, "DBCP Verify");
        assertEquals(2, rootCredentials.getCreateScopedCount());
        Mockito.verify(scopedCredentials).refreshAccessToken();
    }

    @Test
    void testVerifyUsesFreshScopedCredentialWithoutMutatingEnabledState() throws Exception {
        final TestScopedGoogleCredentials enabledScopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestScopedGoogleCredentials verificationScopedCredentials = new TestScopedGoogleCredentials(accessToken(REFRESHED_TOKEN_VALUE, -15));
        verificationScopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(enabledScopedCredentials, verificationScopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());
        final char[] password = provider.getPassword(requestContext());

        assertEquals(2, rootCredentials.getCreateScopedCount());
        assertEquals(0, enabledScopedCredentials.getRefreshAccessTokenCount());
        assertEquals(1, verificationScopedCredentials.getRefreshAccessTokenCount());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL, "Cloud SQL IAM access token");
        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testVerifyIdentityPoolCredentialsRequiresImpersonation() throws Exception {
        final IdentityPoolCredentials scopedCredentials = identityPoolCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, FAILED, VERIFY_IMPERSONATION_REQUIRED);
        Mockito.verify(scopedCredentials, Mockito.never()).refreshAccessToken();
    }

    @Test
    void testOnEnabledRejectsIdentityPoolCredentialsBeforePublishingState() throws Exception {
        final IdentityPoolCredentials scopedCredentials = identityPoolCredentials(accessToken(TOKEN_VALUE, 15));
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue credentialsPropertyValue = mock(PropertyValue.class);
        final GCPCredentialsService credentialsService = mock(GCPCredentialsService.class);

        when(context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)).thenReturn(credentialsPropertyValue);
        when(credentialsPropertyValue.asControllerService(GCPCredentialsService.class)).thenReturn(credentialsService);
        when(credentialsService.getGoogleCredentials()).thenReturn(new RootGoogleCredentials(scopedCredentials));

        final InitializationException exception = assertThrows(InitializationException.class, () -> provider.onEnabled(context));

        assertTrue(exception.getMessage().contains("impersonation"));
        assertNull(getScopedCredentials(provider));
    }

    @Test
    void testVerifyNullCredentialsFails() throws Exception {
        final TestRunner runner = configureRunner(null, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, FAILED, VERIFY_CREDENTIALS_UNAVAILABLE);
    }

    @Test
    void testOnEnabledNullCredentialsFails() throws Exception {
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue credentialsPropertyValue = mock(PropertyValue.class);

        when(context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)).thenReturn(credentialsPropertyValue);
        when(credentialsPropertyValue.asControllerService(GCPCredentialsService.class)).thenReturn(null);

        final InitializationException exception = assertThrows(InitializationException.class, () -> provider.onEnabled(context));

        assertTrue(exception.getMessage().contains("credentials"));
        assertNull(getScopedCredentials(provider));
    }

    @Test
    void testVerifyScopedCredentialCreationReturningNullFails() throws Exception {
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials((GoogleCredentials) null);
        final TestRunner runner = configureRunner(rootCredentials, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE);
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
    }

    @Test
    void testVerifyScopedCredentialCreationFailureIsReported() throws Exception {
        final TestRunner runner = configureRunner(new RootGoogleCredentials(new IllegalStateException(LEAK_SENTINEL)), false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE);
        assertFalse(results.get(0).getExplanation().contains(LEAK_SENTINEL));
    }

    @Test
    void testOnEnabledScopedCredentialCreationReturningNullFails() throws Exception {
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue credentialsPropertyValue = mock(PropertyValue.class);
        final GCPCredentialsService credentialsService = mock(GCPCredentialsService.class);

        when(context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)).thenReturn(credentialsPropertyValue);
        when(credentialsPropertyValue.asControllerService(GCPCredentialsService.class)).thenReturn(credentialsService);
        when(credentialsService.getGoogleCredentials()).thenReturn(new RootGoogleCredentials((GoogleCredentials) null));

        final InitializationException exception = assertThrows(InitializationException.class, () -> provider.onEnabled(context));

        assertTrue(exception.getMessage().contains("scope"));
        assertNull(getScopedCredentials(provider));
    }

    @Test
    void testVerifyRefreshFailureIsSanitized() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(ioException(LEAK_SENTINEL));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_ACQUISITION_FAILED);
        assertFalse(results.get(1).getExplanation().contains(LEAK_SENTINEL));
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyNullAccessTokenFails() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials((AccessToken) null);
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_MISSING);
    }

    @Test
    void testVerifyBlankAccessTokenFails() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(accessToken(" ", 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_MISSING);
    }

    @Test
    void testFreshTokenDoesNotRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final DatabasePasswordProvider provider = getProvider(configureRunner(new RootGoogleCredentials(scopedCredentials)));

        final char[] password = provider.getPassword(requestContext());

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testExpiredTokenRefreshes() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final DatabasePasswordProvider provider = getProvider(configureRunner(new RootGoogleCredentials(scopedCredentials)));

        final char[] password = provider.getPassword(requestContext());

        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), password);
        assertEquals(1, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testConcurrentGetPasswordPerformsSingleRefresh() throws Exception {
        final BlockingScopedGoogleCredentials scopedCredentials = new BlockingScopedGoogleCredentials();
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final DatabasePasswordProvider provider = getProvider(configureRunner(new RootGoogleCredentials(scopedCredentials)));

        executorService = Executors.newFixedThreadPool(2);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final Future<char[]> first = executorService.submit(() -> getPasswordAfterStart(provider, startLatch));
        final Future<char[]> second = executorService.submit(() -> getPasswordAfterStart(provider, startLatch));

        startLatch.countDown();
        assertTrue(scopedCredentials.awaitRefreshEntry());
        scopedCredentials.releaseRefresh();

        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), first.get(5, TimeUnit.SECONDS));
        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), second.get(5, TimeUnit.SECONDS));
        assertEquals(1, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testNullAccessTokenRejectedForPasswordGeneration() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshedAccessToken(null);
        final DatabasePasswordProvider provider = getProvider(configureRunner(new RootGoogleCredentials(scopedCredentials)));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertTrue(exception.getMessage().contains("Cloud SQL IAM"));
    }

    @Test
    void testBlankAccessTokenRejectedForPasswordGeneration() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshedAccessToken(accessToken(" ", 15));
        final DatabasePasswordProvider provider = getProvider(configureRunner(new RootGoogleCredentials(scopedCredentials)));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertTrue(exception.getMessage().contains("Cloud SQL IAM"));
    }

    @Test
    void testRefreshFailureIsSanitizedForPasswordGeneration() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshException(ioException(LEAK_SENTINEL));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertTrue(exception.getMessage().contains("Cloud SQL IAM"));
        assertNull(exception.getCause());
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testIdentityPoolCredentialsFailClosedAtRuntime() throws Exception {
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(
                configureRunner(new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15)))));

        setScopedCredentials(provider, identityPoolCredentials(accessToken(TOKEN_VALUE, 15)));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertTrue(exception.getMessage().contains("Cloud SQL IAM"));
    }

    @Test
    void testGetPasswordReturnsFreshCharacterArrayEachCall() throws Exception {
        final DatabasePasswordProvider provider = getProvider(configureRunner(
                new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15)))));

        final char[] firstPassword = provider.getPassword(requestContext());
        firstPassword[0] = 'X';
        final char[] secondPassword = provider.getPassword(requestContext());

        assertNotSame(firstPassword, secondPassword);
        assertArrayEquals(TOKEN_VALUE.toCharArray(), secondPassword);
    }

    @Test
    void testControllerServiceRegistrationContainsProvider() throws IOException {
        final String resourcePath = "META-INF/services/org.apache.nifi.controller.ControllerService";
        try (InputStream inputStream = GcpCloudSqlIamDatabasePasswordProvider.class.getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull(inputStream);
            final String registeredServices = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(registeredServices.contains(GcpCloudSqlIamDatabasePasswordProvider.class.getName()));
        }
    }

    @Test
    void testAdditionalDetailsResourceDocumentsSupportedPath() throws IOException {
        final String resourcePath = "docs/%s/additionalDetails.md".formatted(GcpCloudSqlIamDatabasePasswordProvider.class.getName());
        try (InputStream inputStream = GcpCloudSqlIamDatabasePasswordProvider.class.getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull(inputStream);
            final String additionalDetails = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(additionalDetails.contains("Cloud SQL for PostgreSQL"));
            assertTrue(additionalDetails.contains("Cloud SQL for MySQL"));
            assertTrue(additionalDetails.contains("roles/iam.workloadIdentityUser"));
            assertTrue(additionalDetails.contains("sslmode=require"));
            assertTrue(additionalDetails.contains("sslMode=REQUIRED"));
            assertTrue(additionalDetails.contains("DBCP **Verify**"));
            assertFalse(additionalDetails.contains("Database Type"));
            assertFalse(additionalDetails.contains("disabledAuthenticationPlugins"));
            assertFalse(additionalDetails.contains("useSSL"));
        }
    }

    private TestRunner configureRunner(final GoogleCredentials rootCredentials) throws Exception {
        return configureRunner(rootCredentials, true);
    }

    private TestRunner configureRunner(final GoogleCredentials rootCredentials, final boolean enableProvider) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final TestGCPCredentialsService credentialsService = new TestGCPCredentialsService(rootCredentials);
        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentialsService);
        runner.enableControllerService(credentialsService);

        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        runner.addControllerService(PASSWORD_PROVIDER_ID, provider);
        runner.setProperty(provider, GCP_CREDENTIALS_PROVIDER_SERVICE, CREDENTIALS_SERVICE_ID);
        if (enableProvider) {
            runner.enableControllerService(provider);
            runner.assertValid(provider);
        }

        return runner;
    }

    private DatabasePasswordProvider getProvider(final TestRunner runner) {
        return (DatabasePasswordProvider) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService(PASSWORD_PROVIDER_ID);
    }

    private GcpCloudSqlIamDatabasePasswordProvider getProviderImplementation(final TestRunner runner) {
        return (GcpCloudSqlIamDatabasePasswordProvider) getProvider(runner);
    }

    private DatabasePasswordRequestContext requestContext() {
        return DatabasePasswordRequestContext.builder()
                .jdbcUrl(JDBC_URL)
                .databaseUser(DATABASE_USER)
                .driverClassName(DRIVER_CLASS)
                .connectionProperties(Map.of())
                .build();
    }

    private char[] getPasswordAfterStart(final DatabasePasswordProvider provider, final CountDownLatch startLatch) throws InterruptedException {
        startLatch.await(5, TimeUnit.SECONDS);
        return provider.getPassword(requestContext());
    }

    private static AccessToken accessToken(final String tokenValue, final long offsetMinutes) {
        return tokenValue == null ? null : new AccessToken(tokenValue, java.util.Date.from(Instant.now().plusSeconds(offsetMinutes * 60)));
    }

    private static IOException ioException(final String message) {
        return new IOException(message);
    }

    private static void assertVerificationResult(final ConfigVerificationResult result, final String stepName,
                                                 final ConfigVerificationResult.Outcome outcome, final String explanationFragment) {
        assertEquals(stepName, result.getVerificationStepName());
        assertEquals(outcome, result.getOutcome());
        assertTrue(result.getExplanation().contains(explanationFragment), result::getExplanation);
    }

    private static void assertNoLogMessagesContain(final MockComponentLog logger, final String value) {
        final List<LogMessage> logMessages = new ArrayList<>();
        logMessages.addAll(logger.getInfoMessages());
        logMessages.addAll(logger.getWarnMessages());
        logMessages.addAll(logger.getErrorMessages());

        for (final LogMessage logMessage : logMessages) {
            final String rawMessage = logMessage.getMsg();
            assertFalse(rawMessage != null && rawMessage.contains(value));
            final Object[] args = logMessage.getArgs();
            final String formattedMessage = MessageFormatter.arrayFormat(rawMessage, args == null ? new Object[0] : args).getMessage();
            assertFalse(formattedMessage != null && formattedMessage.contains(value));
            if (args != null) {
                for (final Object arg : args) {
                    final String argValue = arg == null ? null : arg.toString();
                    assertFalse(argValue != null && argValue.contains(value));
                }
            }
            assertThrowableChainDoesNotContain(logMessage.getThrowable(), value, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
        }
    }

    private static void assertThrowableChainDoesNotContain(final Throwable throwable, final String value, final Set<Throwable> visited) {
        if (throwable == null || !visited.add(throwable)) {
            return;
        }

        final String message = throwable.getMessage();
        assertFalse(message != null && message.contains(value));

        for (final Throwable suppressed : throwable.getSuppressed()) {
            assertThrowableChainDoesNotContain(suppressed, value, visited);
        }

        assertThrowableChainDoesNotContain(throwable.getCause(), value, visited);
    }

    private static GoogleCredentials getScopedCredentials(final GcpCloudSqlIamDatabasePasswordProvider provider) throws ReflectiveOperationException {
        final Field field = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredField("scopedCredentials");
        field.setAccessible(true);
        return (GoogleCredentials) field.get(provider);
    }

    private static void setScopedCredentials(final GcpCloudSqlIamDatabasePasswordProvider provider, final GoogleCredentials credentials)
            throws ReflectiveOperationException {
        final Field field = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredField("scopedCredentials");
        field.setAccessible(true);
        field.set(provider, credentials);
    }

    @SuppressWarnings("unchecked")
    private static List<PropertyDescriptor> getSupportedPropertyDescriptors(final GcpCloudSqlIamDatabasePasswordProvider provider)
            throws ReflectiveOperationException {
        final Method method = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredMethod("getSupportedPropertyDescriptors");
        method.setAccessible(true);
        return (List<PropertyDescriptor>) method.invoke(provider);
    }

    private static final class TestGCPCredentialsService extends AbstractControllerService implements GCPCredentialsService {
        private final GoogleCredentials googleCredentials;

        private TestGCPCredentialsService(final GoogleCredentials googleCredentials) {
            this.googleCredentials = googleCredentials;
        }

        @Override
        public GoogleCredentials getGoogleCredentials() {
            return googleCredentials;
        }
    }

    private static final class RootGoogleCredentials extends GoogleCredentials {
        private final AtomicInteger createScopedCount = new AtomicInteger();
        private final GoogleCredentials[] scopedCredentials;
        private final RuntimeException createScopedException;
        private final AtomicInteger scopedCredentialIndex = new AtomicInteger();
        private volatile List<String> lastRequestedScopes = List.of();

        private RootGoogleCredentials(final GoogleCredentials scopedCredentials) {
            this.scopedCredentials = new GoogleCredentials[]{scopedCredentials};
            this.createScopedException = null;
        }

        private RootGoogleCredentials(final RuntimeException createScopedException) {
            this.scopedCredentials = new GoogleCredentials[0];
            this.createScopedException = createScopedException;
        }

        private RootGoogleCredentials(final GoogleCredentials firstScopedCredentials, final GoogleCredentials secondScopedCredentials) {
            this.scopedCredentials = new GoogleCredentials[]{firstScopedCredentials, secondScopedCredentials};
            this.createScopedException = null;
        }

        @Override
        public GoogleCredentials createScoped(final java.util.Collection<String> scopes) {
            createScopedCount.incrementAndGet();
            lastRequestedScopes = List.copyOf(scopes);
            if (createScopedException != null) {
                throw createScopedException;
            }

            final int index = Math.min(scopedCredentialIndex.getAndIncrement(), scopedCredentials.length - 1);
            return scopedCredentials.length == 0 ? null : scopedCredentials[index];
        }

        private int getCreateScopedCount() {
            return createScopedCount.get();
        }

        private List<String> getLastRequestedScopes() {
            return lastRequestedScopes;
        }
    }

    private static class TestScopedGoogleCredentials extends GoogleCredentials {
        private final AtomicInteger refreshAccessTokenCount = new AtomicInteger();
        private volatile AccessToken refreshedAccessToken;
        private volatile IOException refreshException;

        private TestScopedGoogleCredentials(final AccessToken initialAccessToken) {
            super(initialAccessToken);
        }

        @Override
        public AccessToken refreshAccessToken() throws IOException {
            refreshAccessTokenCount.incrementAndGet();
            if (refreshException != null) {
                throw refreshException;
            }
            return refreshedAccessToken;
        }

        protected void setRefreshedAccessToken(final AccessToken refreshedAccessToken) {
            this.refreshedAccessToken = refreshedAccessToken;
        }

        protected void setRefreshException(final IOException refreshException) {
            this.refreshException = refreshException;
        }

        protected int getRefreshAccessTokenCount() {
            return refreshAccessTokenCount.get();
        }
    }

    private static ImpersonatedCredentials impersonatedCredentials(final AccessToken accessToken) throws IOException {
        final ImpersonatedCredentials credentials = mock(ImpersonatedCredentials.class);
        when(credentials.refreshAccessToken()).thenReturn(accessToken);
        return credentials;
    }

    private static ImpersonatedCredentials impersonatedCredentials(final IOException exception) throws IOException {
        final ImpersonatedCredentials credentials = mock(ImpersonatedCredentials.class);
        when(credentials.refreshAccessToken()).thenThrow(exception);
        return credentials;
    }

    private static IdentityPoolCredentials identityPoolCredentials(final AccessToken accessToken) throws IOException {
        final IdentityPoolCredentials credentials = mock(IdentityPoolCredentials.class);
        when(credentials.refreshAccessToken()).thenReturn(accessToken);
        return credentials;
    }

    private static final class BlockingScopedGoogleCredentials extends TestScopedGoogleCredentials {
        private final CountDownLatch refreshEnteredLatch = new CountDownLatch(1);
        private final CountDownLatch releaseRefreshLatch = new CountDownLatch(1);

        private BlockingScopedGoogleCredentials() {
            super(null);
        }

        @Override
        public AccessToken refreshAccessToken() throws IOException {
            refreshEnteredLatch.countDown();
            try {
                if (!releaseRefreshLatch.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for refresh release");
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for refresh release", e);
            }
            return super.refreshAccessToken();
        }

        private boolean awaitRefreshEntry() throws InterruptedException {
            return refreshEnteredLatch.await(5, TimeUnit.SECONDS);
        }

        private void releaseRefresh() {
            releaseRefreshLatch.countDown();
        }
    }
}
