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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
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
import java.util.stream.Stream;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.DATABASE_TYPE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.FAILED_PASSWORD_MESSAGE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.GCP_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.MALFORMED_MYSQL_JDBC_URL_MESSAGE;
import static org.apache.nifi.processors.gcp.cloudsql.GcpCloudSqlIamDatabasePasswordProvider.MALFORMED_SSLMODE_MESSAGE;
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
    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String DATABASE_USER = "service-account@test-project.iam";
    private static final String MYSQL_DATABASE_USER = "service-account";
    private static final String JDBC_URL = "jdbc:postgresql://example:5432/database?sslmode=require";
    private static final String MYSQL_JDBC_URL = "jdbc:mysql://example:3306/database?sslMode=REQUIRED";
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
    void testDatabaseTypeDescriptorSupportsPostgreSqlAndMySqlAndDefaultsToPostgreSql() {
        final PropertyDescriptor descriptor = DATABASE_TYPE;

        assertEquals(CloudSqlDatabaseType.POSTGRESQL.getValue(), descriptor.getDefaultValue());
        assertTrue(descriptor.isRequired());
        assertEquals("Database Type", descriptor.getName());
        assertEquals(List.of(CloudSqlDatabaseType.POSTGRESQL.getValue(), CloudSqlDatabaseType.MYSQL.getValue()), descriptor.getAllowableValues().stream()
                .map(AllowableValue::getValue)
                .toList());
    }

    @Test
    void testOnEnabledCachesScopedCredentialAndReusesIt() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        assertEquals(1, rootCredentials.getCreateScopedCount());
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
        assertEquals(CloudSqlDatabaseType.POSTGRESQL, getDatabaseType(provider));
        assertSame(scopedCredentials, getScopedCredentials(provider));

        assertEquals(TOKEN_VALUE, new String(provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()))));
        assertEquals(TOKEN_VALUE, new String(provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()))));

        assertEquals(1, rootCredentials.getCreateScopedCount());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testOnDisabledClearsCachedCredentialAndDatabaseType() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        runner.disableControllerService(provider);

        assertNull(getScopedCredentials(provider));
        assertNull(getDatabaseType(provider));

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testOnEnabledCachesScopedCredentialForMySqlAndUsesMySqlValidation() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials, true, CloudSqlDatabaseType.MYSQL);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        assertEquals(1, rootCredentials.getCreateScopedCount());
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
        assertEquals(CloudSqlDatabaseType.MYSQL, getDatabaseType(provider));
        assertSame(scopedCredentials, getScopedCredentials(provider));

        assertEquals(TOKEN_VALUE, new String(provider.getPassword(requestContext(MYSQL_JDBC_URL, MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, Map.of()))));
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testOnDisabledClearsCachedCredentialAndDatabaseTypeForMySql() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials, true, CloudSqlDatabaseType.MYSQL);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        runner.disableControllerService(provider);

        assertNull(getScopedCredentials(provider));
        assertNull(getDatabaseType(provider));

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(MYSQL_JDBC_URL, MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
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
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL,
                "Resolved Database Type PostgreSQL");
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL,
                "created a Cloud SQL scoped ImpersonatedCredentials instance. Target service account impersonation is active.");
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL,
                "Acquired a non-empty Cloud SQL IAM access token for PostgreSQL");
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL,
                "verifies live subject token exchange, Google STS, and target service account impersonation");
        assertEquals(2, rootCredentials.getCreateScopedCount());
        Mockito.verify(scopedCredentials).refreshAccessToken();
    }

    @Test
    void testVerifyIdentityPoolCredentialsRequiresImpersonation() throws Exception {
        final IdentityPoolCredentials scopedCredentials = identityPoolCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, FAILED, "Resolved Database Type PostgreSQL");
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, FAILED, VERIFY_IMPERSONATION_REQUIRED);
        Mockito.verify(scopedCredentials, Mockito.never()).refreshAccessToken();
    }

    @Test
    void testOnEnabledRejectsIdentityPoolCredentialsBeforePublishingState() throws Exception {
        final IdentityPoolCredentials scopedCredentials = identityPoolCredentials(accessToken(TOKEN_VALUE, 15));
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue credentialsPropertyValue = mock(PropertyValue.class);
        final PropertyValue databaseTypePropertyValue = mock(PropertyValue.class);
        final GCPCredentialsService credentialsService = mock(GCPCredentialsService.class);

        when(context.getProperty(DATABASE_TYPE)).thenReturn(databaseTypePropertyValue);
        when(databaseTypePropertyValue.asAllowableValue(CloudSqlDatabaseType.class)).thenReturn(CloudSqlDatabaseType.POSTGRESQL);
        when(context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)).thenReturn(credentialsPropertyValue);
        when(credentialsPropertyValue.asControllerService(GCPCredentialsService.class)).thenReturn(credentialsService);
        when(credentialsService.getGoogleCredentials()).thenReturn(new RootGoogleCredentials(scopedCredentials));

        final InitializationException exception = assertThrows(InitializationException.class,
                () -> provider.onEnabled(context));

        assertEquals(VERIFY_IMPERSONATION_REQUIRED, exception.getMessage());
        assertNull(getScopedCredentials(provider));
        assertNull(getDatabaseType(provider));
        Mockito.verify(scopedCredentials, Mockito.never()).refreshAccessToken();
    }

    @Test
    void testGetPasswordRejectsIdentityPoolCredentialsBeforeRefresh() throws Exception {
        final IdentityPoolCredentials scopedCredentials = identityPoolCredentials(accessToken(TOKEN_VALUE, -15));
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        setDatabaseType(provider, CloudSqlDatabaseType.POSTGRESQL);
        setScopedCredentials(provider, scopedCredentials);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        Mockito.verify(scopedCredentials, Mockito.never()).refreshAccessToken();
    }

    @Test
    void testInvalidDatabaseTypePropertyIsRejectedByValidation() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final TestGCPCredentialsService credentialsService = new TestGCPCredentialsService(new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15))));
        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentialsService);
        runner.enableControllerService(credentialsService);

        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        runner.addControllerService(PASSWORD_PROVIDER_ID, provider);
        runner.setProperty(provider, GCP_CREDENTIALS_PROVIDER_SERVICE, CREDENTIALS_SERVICE_ID);
        runner.setProperty(provider, DATABASE_TYPE, "SQLSERVER");

        runner.assertNotValid(provider);
    }

    @Test
    void testVerifyNullCredentialsFails() throws Exception {
        final TestRunner runner = configureRunner(null, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, FAILED, VERIFY_CREDENTIALS_UNAVAILABLE);
    }

    @Test
    void testVerifyScopedCredentialCreationReturningNullFails() throws Exception {
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials((GoogleCredentials) null);
        final TestRunner runner = configureRunner(rootCredentials, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE);
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
    }

    @Test
    void testVerifyScopedCredentialCreationFailureIsSanitized() throws Exception {
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(new IllegalStateException(LEAK_SENTINEL));
        final TestRunner runner = configureRunner(rootCredentials, false);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE);
        assertFalse(results.getFirst().getExplanation().contains(LEAK_SENTINEL));
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyRefreshIOExceptionIsSanitized() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(ioException(LEAK_SENTINEL, "com.google.auth.oauth2.ImpersonatedCredentials"));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL,
                "created a Cloud SQL scoped ImpersonatedCredentials instance. Target service account impersonation is active.");
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_ACQUISITION_FAILED);
        assertFalse(results.get(1).getExplanation().contains(LEAK_SENTINEL));
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyRefreshRuntimeFailureIsSanitized() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(new IllegalStateException(LEAK_SENTINEL));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_ACQUISITION_FAILED);
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyNullAccessTokenFails() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials((AccessToken) null);
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_MISSING);
    }

    @Test
    void testVerifyBlankAccessTokenFails() throws Exception {
        final ImpersonatedCredentials scopedCredentials = impersonatedCredentials(accessToken(" ", 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, FAILED, VERIFY_TOKEN_MISSING);
    }

    @Test
    void testVerifyUsesFreshScopedCredentialWithoutMutatingEnabledState() throws Exception {
        final TestScopedGoogleCredentials enabledScopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestScopedGoogleCredentials verificationScopedCredentials = new TestScopedGoogleCredentials(accessToken(REFRESHED_TOKEN_VALUE, 15));
        verificationScopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(enabledScopedCredentials, verificationScopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());
        final char[] password = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));

        assertEquals(2, rootCredentials.getCreateScopedCount());
        assertEquals(0, enabledScopedCredentials.getRefreshAccessTokenCount());
        assertEquals(1, verificationScopedCredentials.getRefreshAccessTokenCount());
        assertFalse(results.get(1).getExplanation().contains("subject token exchange"));
        assertTrue(results.get(1).getExplanation().contains("Cloud SQL IAM token acquisition for the current principal"));
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL,
                "Use DBCP Verify for the end-to-end database check.");
        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testVerifyUsesSqlServiceLoginScope() throws Exception {
        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue credentialsPropertyValue = mock(PropertyValue.class);
        final PropertyValue databaseTypePropertyValue = mock(PropertyValue.class);
        final GCPCredentialsService credentialsService = mock(GCPCredentialsService.class);
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15)));

        when(context.getProperty(DATABASE_TYPE)).thenReturn(databaseTypePropertyValue);
        when(databaseTypePropertyValue.asAllowableValue(CloudSqlDatabaseType.class)).thenReturn(CloudSqlDatabaseType.POSTGRESQL);
        when(context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)).thenReturn(credentialsPropertyValue);
        when(credentialsPropertyValue.asControllerService(GCPCredentialsService.class)).thenReturn(credentialsService);
        when(credentialsService.getGoogleCredentials()).thenReturn(rootCredentials);

        final List<ConfigVerificationResult> results = provider.verify(context, mock(ComponentLog.class), Map.of());

        assertEquals(2, results.size());
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), rootCredentials.getLastRequestedScopes());
        assertVerificationResult(results.getFirst(), VERIFY_SCOPE_STEP, SUCCESSFUL, "Resolved Database Type PostgreSQL");
    }

    @ParameterizedTest(name = "verify wording for {0}")
    @MethodSource("verifySuccessContexts")
    void testVerifySuccessWordingIsGenericForSelectedDatabaseType(final CloudSqlDatabaseType databaseType,
                                                                  final GoogleCredentials scopedCredentials,
                                                                  final String scopeMessage,
                                                                  final String tokenMessage) throws Exception {
        if (scopedCredentials instanceof TestScopedGoogleCredentials testScopedGoogleCredentials) {
            testScopedGoogleCredentials.setRefreshedAccessToken(accessToken(TOKEN_VALUE, 15));
        }
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, databaseType);
        final GcpCloudSqlIamDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL, "Resolved Database Type %s".formatted(databaseType.getDisplayName()));
        assertVerificationResult(results.get(0), VERIFY_SCOPE_STEP, SUCCESSFUL, scopeMessage);
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL,
                "Acquired a non-empty Cloud SQL IAM access token for %s".formatted(databaseType.getDisplayName()));
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL, tokenMessage);
        assertVerificationResult(results.get(1), VERIFY_TOKEN_STEP, SUCCESSFUL,
                "does not connect to the selected database. Use DBCP Verify for the end-to-end database check.");
    }

    @Test
    void testChainedControllerServiceResolution() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final RootGoogleCredentials rootCredentials = new RootGoogleCredentials(scopedCredentials);
        final TestRunner runner = configureRunner(rootCredentials);

        final DatabasePasswordProvider provider = getProvider(runner);
        final char[] password = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testFreshTokenDoesNotRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));

        final DatabasePasswordProvider provider = getProvider(runner);
        final char[] password = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testExpiredTokenRefreshes() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));

        final DatabasePasswordProvider provider = getProvider(runner);
        final char[] password = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));

        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), password);
        assertEquals(1, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testConcurrentGetPasswordPerformsSingleRefresh() throws Exception {
        final BlockingScopedGoogleCredentials scopedCredentials = new BlockingScopedGoogleCredentials();
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

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
    void testNullRequestContextRejected() throws Exception {
        final TestRunner runner = configureRunner(new RootGoogleCredentials(new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15))));
        final DatabasePasswordProvider provider = getProvider(runner);

        final NullPointerException exception = assertThrows(NullPointerException.class, () -> provider.getPassword(null));

        assertEquals("Database Password Request Context required", exception.getMessage());
    }

    @Test
    void testBlankDatabaseUserRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, " ", Map.of())));

        assertEquals("Database Username must be configured for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testBlankMySqlDatabaseUserRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(MYSQL_JDBC_URL, " ", MYSQL_DRIVER_CLASS, Map.of())));

        assertEquals("Database Username must be configured for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "accepted sslmode {0} from URL")
    @MethodSource("acceptedUrlSslModes")
    void testAcceptedUrlSslModes(final String sslMode) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database?sslmode=%s".formatted(sslMode),
                DATABASE_USER,
                Map.of("sslmode", "disable")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testAcceptedCaseInsensitiveUrlSslModeNameAndDecodedValue() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database?SslMode=verify%2Dfull",
                DATABASE_USER,
                Map.of("sslmode", "disable")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @ParameterizedTest(name = "accepted sslmode {0} from connection properties")
    @MethodSource("acceptedPropertySslModes")
    void testAcceptedConnectionPropertySslModes(final String sslMode) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database",
                DATABASE_USER,
                Map.of("sslmode", sslMode)
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testAcceptedCaseInsensitiveConnectionPropertyName() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database",
                DATABASE_USER,
                Map.of("SSLMODE", "require")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testMissingSslModeRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext("jdbc:postgresql://example:5432/database", DATABASE_USER, Map.of())));

        assertEquals("PostgreSQL sslmode must be configured for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "rejected sslmode {0}")
    @MethodSource("rejectedSslModeContexts")
    void testRejectedSslModes(final String jdbcUrl, final Map<String, String> connectionProperties, final String expectedSslMode) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(jdbcUrl, DATABASE_USER, connectionProperties)));

        assertEquals("PostgreSQL sslmode [%s] is not supported for Cloud SQL IAM authentication".formatted(expectedSslMode), exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testUrlSslModeTakesPrecedenceOverConnectionProperties() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database?sslmode=require",
                DATABASE_USER,
                Map.of("sslmode", "disable")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testDuplicateUrlSslModeLastInsecureValueRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:postgresql://example:5432/database?sslmode=require&sslmode=disable",
                        DATABASE_USER,
                        Map.of("sslmode", "verify-full")
                )));

        assertEquals("PostgreSQL sslmode [disable] is not supported for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testDuplicateUrlSslModeLastSecureValueAccepted() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:postgresql://example:5432/database?sslmode=disable&sslmode=require",
                DATABASE_USER,
                Map.of("sslmode", "disable")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testMalformedUrlEncodedSslModeValueRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:postgresql://example:5432/database?sslmode=%GG",
                        DATABASE_USER,
                        Map.of("sslmode", "require")
                )));

        assertEquals(MALFORMED_SSLMODE_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMalformedUrlEncodedSslModeNameRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:postgresql://example:5432/database?sslmo%G=require",
                        DATABASE_USER,
                        Map.of("sslmode", "require")
                )));

        assertEquals(MALFORMED_SSLMODE_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "accepted MySQL sslMode {0} from URL")
    @MethodSource("acceptedMySqlUrlSslModes")
    void testAcceptedMySqlUrlSslModes(final String sslMode) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:mysql://example:3306/database?sslMode=%s".formatted(sslMode),
                MYSQL_DATABASE_USER,
                MYSQL_DRIVER_CLASS,
                Map.of()
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @ParameterizedTest(name = "accepted MySQL sslMode {0} from connection properties")
    @MethodSource("acceptedMySqlPropertySslModes")
    void testAcceptedMySqlConnectionPropertySslModes(final String sslMode) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:mysql://example:3306/database",
                MYSQL_DATABASE_USER,
                MYSQL_DRIVER_CLASS,
                Map.of("sslMode", sslMode)
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testRejectedMySqlCaseInsensitiveUrlSslModeNameAndDecodedValueBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?SslMode=verify%5Fidentity",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("sslMode", "VERIFY_CA")
                )));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testRejectedMySqlCaseInsensitiveConnectionPropertyNameBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("SSLMODE", "required")
                )));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlConnectionPropertiesSslModeTakesPrecedenceOverJdbcUrlBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMode=REQUIRED",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("sslMode", "DISABLED")
                )));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlDuplicateUrlSslModeLastInsecureValueRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMode=REQUIRED&sslMode=DISABLED",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of()
                )));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlDuplicateUrlSslModeLastSecureValueAccepted() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:mysql://example:3306/database?sslMode=PREFERRED&sslMode=VERIFY_CA",
                MYSQL_DATABASE_USER,
                MYSQL_DRIVER_CLASS,
                Map.of()
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testMySqlExactConnectionPropertySslModeOverridesMissingJdbcUrlSslMode() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] password = provider.getPassword(requestContext(
                "jdbc:mysql://example:3306/database",
                MYSQL_DATABASE_USER,
                MYSQL_DRIVER_CLASS,
                Map.of("sslMode", "VERIFY_CA")
        ));

        assertArrayEquals(TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testMissingMySqlSslModeRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext("jdbc:mysql://example:3306/database", MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, Map.of())));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "rejected MySQL sslMode case {0}")
    @MethodSource("rejectedMySqlSslModeContexts")
    void testRejectedMySqlSslModes(final String jdbcUrl, final Map<String, String> connectionProperties) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(jdbcUrl, MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, connectionProperties)));

        assertEquals("MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMalformedUrlEncodedMySqlSslModeValueRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMode=%GG",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("sslMode", "REQUIRED")
                )));

        assertEquals(MALFORMED_MYSQL_JDBC_URL_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMalformedUrlEncodedMySqlPropertyNameRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMo%G=REQUIRED",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of()
                )));

        assertEquals(MALFORMED_MYSQL_JDBC_URL_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "rejected MySQL driver/url case {0}")
    @MethodSource("rejectedMySqlDriverAndUrlContexts")
    void testRejectedMySqlDriverAndJdbcUrlBeforeRefresh(final String jdbcUrl,
                                                        final String driverClassName,
                                                        final String expectedMessage) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(jdbcUrl, MYSQL_DATABASE_USER, driverClassName, Map.of("sslMode", "REQUIRED"))));

        assertEquals(expectedMessage, exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "rejected MySQL URL credentials case {0}")
    @MethodSource("rejectedMySqlUrlCredentialContexts")
    void testMySqlUrlCredentialsRejectedBeforeRefresh(final String jdbcUrl,
                                                      final String expectedMessage) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(jdbcUrl, MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, Map.of("sslMode", "REQUIRED"))));

        assertEquals(expectedMessage, exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlDisabledClearPasswordPluginInConnectionPropertiesRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        MYSQL_JDBC_URL,
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("disabledAuthenticationPlugins", " sha256_password , MYSQL_CLEAR_PASSWORD ")
                )));

        assertEquals("MySQL disabledAuthenticationPlugins must not disable the clear-password authentication plugin required for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlDisabledClearPasswordPluginRejectedWhenConfiguredOnlyInJdbcUrl() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMode=REQUIRED&disabledAuthenticationPlugins=mysql_clear_password",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of()
                )));

        assertEquals("MySQL disabledAuthenticationPlugins must not disable the clear-password authentication plugin required for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlDisabledClearPasswordPluginInJdbcUrlRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        "jdbc:mysql://example:3306/database?sslMode=REQUIRED&disabledAuthenticationPlugins=com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin",
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("disabledAuthenticationPlugins", "sha256_password")
                )));

        assertEquals("MySQL disabledAuthenticationPlugins must not disable the clear-password authentication plugin required for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @ParameterizedTest(name = "rejected MySQL legacy TLS property {0}")
    @MethodSource("rejectedMySqlLegacyTlsPropertyContexts")
    void testMySqlLegacyTlsPropertiesRejectedBeforeRefresh(final String jdbcUrl,
                                                           final Map<String, String> connectionProperties) throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(jdbcUrl, MYSQL_DATABASE_USER, MYSQL_DRIVER_CLASS, connectionProperties)));

        assertEquals("MySQL legacy TLS properties useSSL, requireSSL, and verifyServerCertificate are not supported for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testMySqlConnectionPropertyUserRejectedBeforeRefresh() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, -15));
        scopedCredentials.setRefreshedAccessToken(accessToken(REFRESHED_TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials), true, CloudSqlDatabaseType.MYSQL);
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(
                        MYSQL_JDBC_URL,
                        MYSQL_DATABASE_USER,
                        MYSQL_DRIVER_CLASS,
                        Map.of("user", "override-user")
                )));

        assertEquals("MySQL DBCP connection properties must not define user for Cloud SQL IAM authentication", exception.getMessage());
        assertEquals(0, scopedCredentials.getRefreshAccessTokenCount());
    }

    @Test
    void testNullAccessTokenRejected() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshedAccessToken(null);
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
    }

    @Test
    void testBlankAccessTokenRejected() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshedAccessToken(accessToken(" ", 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
    }

    @Test
    void testArbitraryRefreshFailureIsSanitized() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        scopedCredentials.setRefreshException(ioException(LEAK_SENTINEL, "org.example.CustomCredentials"));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
        assertFalse(exception.getMessage().contains(LEAK_SENTINEL));
        assertNoLogMessagesContain(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testUnknownGoogleAuthRefreshMessageIsSanitized() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        final IOException ioException = ioException(LEAK_SENTINEL, "com.google.auth.oauth2.ImpersonatedCredentials");
        scopedCredentials.setRefreshException(ioException);
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testGoogleAuthRefreshFailurePreservesCause() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(null);
        final IOException ioException = ioException("Error requesting access token", "com.google.auth.oauth2.ImpersonatedCredentials");
        scopedCredentials.setRefreshException(ioException);
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class,
                () -> provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of())));

        assertEquals(FAILED_PASSWORD_MESSAGE, exception.getMessage());
        assertSame(ioException, exception.getCause());
    }

    @Test
    void testGetPasswordReturnsFreshCharacterArrayEachCall() throws Exception {
        final TestScopedGoogleCredentials scopedCredentials = new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15));
        final TestRunner runner = configureRunner(new RootGoogleCredentials(scopedCredentials));
        final DatabasePasswordProvider provider = getProvider(runner);

        final char[] firstPassword = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));
        firstPassword[0] = 'X';
        final char[] secondPassword = provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));

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
            assertTrue(additionalDetails.contains("Database Type"));
            assertTrue(additionalDetails.contains("supports Cloud SQL for PostgreSQL and Cloud SQL for MySQL"));
            assertTrue(additionalDetails.contains("roles/iam.workloadIdentityUser"));
            assertTrue(additionalDetails.contains("must be configured with **Target Service Account**"));
            assertTrue(additionalDetails.contains("jdbc:postgresql://<HOST>:5432/<DATABASE>?sslmode=require"));
            assertTrue(additionalDetails.contains("jdbc:mysql://<HOST>:3306/<DATABASE>?sslMode=REQUIRED"));
            assertTrue(additionalDetails.contains("com.mysql.cj.jdbc.Driver"));
            assertTrue(additionalDetails.contains("**Verify** checks that NiFi can obtain a token"));
            assertTrue(additionalDetails.contains("DBCP **Verify** checks the"));
        }
    }

    private static Stream<Arguments> acceptedUrlSslModes() {
        return Stream.of(
                Arguments.of("prefer"),
                Arguments.of("require"),
                Arguments.of("verify-ca"),
                Arguments.of("verify-full")
        );
    }

    private static Stream<Arguments> acceptedPropertySslModes() {
        return acceptedUrlSslModes();
    }

    private static Stream<Arguments> acceptedMySqlUrlSslModes() {
        return Stream.of(
                Arguments.of("REQUIRED"),
                Arguments.of("VERIFY_CA"),
                Arguments.of("VERIFY_IDENTITY")
        );
    }

    private static Stream<Arguments> acceptedMySqlPropertySslModes() {
        return acceptedMySqlUrlSslModes();
    }

    private static Stream<Arguments> rejectedSslModeContexts() {
        return Stream.of(
                Arguments.of("jdbc:postgresql://example:5432/database?sslmode=disable", Map.of("sslmode", "require"), "disable"),
                Arguments.of("jdbc:postgresql://example:5432/database?sslmode=allow", Map.of("sslmode", "verify-full"), "allow"),
                Arguments.of("jdbc:postgresql://example:5432/database", Map.of("sslmode", "disable"), "disable"),
                Arguments.of("jdbc:postgresql://example:5432/database", Map.of("sslmode", "allow"), "allow")
        );
    }

    private static Stream<Arguments> rejectedMySqlSslModeContexts() {
        return Stream.of(
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED", Map.of("sslMode", "DISABLED")),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=VERIFY_CA", Map.of("sslMode", "PREFERRED")),
                Arguments.of("jdbc:mysql://example:3306/database", Map.of("sslMode", "DISABLED")),
                Arguments.of("jdbc:mysql://example:3306/database", Map.of("sslMode", "PREFERRED"))
        );
    }

    private static Stream<Arguments> rejectedMySqlDriverAndUrlContexts() {
        return Stream.of(
                Arguments.of(MYSQL_JDBC_URL, POSTGRES_DRIVER_CLASS,
                        "MySQL driver class must be configured as com.mysql.cj.jdbc.Driver for Cloud SQL IAM authentication"),
                Arguments.of("jdbc:mariadb://example:3306/database?sslMode=REQUIRED", MYSQL_DRIVER_CLASS,
                        "MySQL JDBC URL must use the standard single-host jdbc:mysql:// format for Cloud SQL IAM authentication"),
                Arguments.of("jdbc:mysql://example:3306,demo:3307/database?sslMode=REQUIRED", MYSQL_DRIVER_CLASS,
                        "MySQL JDBC URL must use the standard single-host jdbc:mysql:// format for Cloud SQL IAM authentication")
        );
    }

    private static Stream<Arguments> rejectedMySqlLegacyTlsPropertyContexts() {
        return Stream.of(
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED&useSSL=false", Map.of()),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED&requireSSL=true", Map.of()),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED&verifyServerCertificate=false", Map.of()),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED", Map.of("useSSL", "false")),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED", Map.of("requireSSL", "true")),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED", Map.of("verifyServerCertificate", "false"))
        );
    }

    private static Stream<Arguments> rejectedMySqlUrlCredentialContexts() {
        return Stream.of(
                Arguments.of("jdbc:mysql://iam-user@example:3306/database?sslMode=REQUIRED",
                        "MySQL JDBC URL must not define user or password for Cloud SQL IAM authentication"),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED&user=override-user",
                        "MySQL JDBC URL must not define user or password for Cloud SQL IAM authentication"),
                Arguments.of("jdbc:mysql://example:3306/database?sslMode=REQUIRED&password=override-password",
                        "MySQL JDBC URL must not define user or password for Cloud SQL IAM authentication")
        );
    }

    private static Stream<Arguments> verifySuccessContexts() throws IOException {
        return Stream.of(
                Arguments.of(
                        CloudSqlDatabaseType.POSTGRESQL,
                        new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15)),
                        "created a Cloud SQL scoped TestScopedGoogleCredentials instance.",
                        "Cloud SQL IAM token acquisition for the current principal"
                ),
                Arguments.of(
                        CloudSqlDatabaseType.MYSQL,
                        new TestScopedGoogleCredentials(accessToken(TOKEN_VALUE, 15)),
                        "created a Cloud SQL scoped TestScopedGoogleCredentials instance.",
                        "Cloud SQL IAM token acquisition for the current principal"
                )
        );
    }

    private TestRunner configureRunner(final GoogleCredentials rootCredentials) throws Exception {
        return configureRunner(rootCredentials, true);
    }

    private TestRunner configureRunner(final GoogleCredentials rootCredentials, final boolean enableProvider) throws Exception {
        return configureRunner(rootCredentials, enableProvider, null);
    }

    private TestRunner configureRunner(final GoogleCredentials rootCredentials, final boolean enableProvider,
                                       final CloudSqlDatabaseType databaseType) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final TestGCPCredentialsService credentialsService = new TestGCPCredentialsService(rootCredentials);
        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentialsService);
        runner.enableControllerService(credentialsService);

        final GcpCloudSqlIamDatabasePasswordProvider provider = new GcpCloudSqlIamDatabasePasswordProvider();
        runner.addControllerService(PASSWORD_PROVIDER_ID, provider);
        runner.setProperty(provider, GCP_CREDENTIALS_PROVIDER_SERVICE, CREDENTIALS_SERVICE_ID);
        if (databaseType != null) {
            runner.setProperty(provider, DATABASE_TYPE, databaseType.getValue());
        }
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

    private DatabasePasswordRequestContext requestContext(final String jdbcUrl, final String databaseUser, final Map<String, String> connectionProperties) {
        return requestContext(jdbcUrl, databaseUser, POSTGRES_DRIVER_CLASS, connectionProperties);
    }

    private DatabasePasswordRequestContext requestContext(final String jdbcUrl, final String databaseUser,
                                                          final String driverClassName, final Map<String, String> connectionProperties) {
        return DatabasePasswordRequestContext.builder()
                .jdbcUrl(jdbcUrl)
                .databaseUser(databaseUser)
                .driverClassName(driverClassName)
                .connectionProperties(connectionProperties)
                .build();
    }

    private char[] getPasswordAfterStart(final DatabasePasswordProvider provider, final CountDownLatch startLatch) throws InterruptedException {
        startLatch.await(5, TimeUnit.SECONDS);
        return provider.getPassword(requestContext(JDBC_URL, DATABASE_USER, Map.of()));
    }

    private static AccessToken accessToken(final String tokenValue, final long offsetMinutes) {
        return tokenValue == null ? null : new AccessToken(tokenValue, java.util.Date.from(Instant.now().plusSeconds(offsetMinutes * 60)));
    }

    private static IOException ioException(final String message, final String className) {
        final IOException ioException = new IOException(message);
        ioException.setStackTrace(new StackTraceElement[]{new StackTraceElement(className, "refreshAccessToken", "Source.java", 1)});
        return ioException;
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

    private static CloudSqlDatabaseType getDatabaseType(final GcpCloudSqlIamDatabasePasswordProvider provider) throws ReflectiveOperationException {
        final Field field = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredField("databaseType");
        field.setAccessible(true);
        return (CloudSqlDatabaseType) field.get(provider);
    }

    private static void setScopedCredentials(final GcpCloudSqlIamDatabasePasswordProvider provider, final GoogleCredentials credentials) throws ReflectiveOperationException {
        final Field field = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredField("scopedCredentials");
        field.setAccessible(true);
        field.set(provider, credentials);
    }

    private static void setDatabaseType(final GcpCloudSqlIamDatabasePasswordProvider provider, final CloudSqlDatabaseType configuredDatabaseType) throws ReflectiveOperationException {
        final Field field = GcpCloudSqlIamDatabasePasswordProvider.class.getDeclaredField("databaseType");
        field.setAccessible(true);
        field.set(provider, configuredDatabaseType);
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
        private volatile RuntimeException runtimeException;

        private TestScopedGoogleCredentials(final AccessToken initialAccessToken) {
            super(initialAccessToken);
        }

        @Override
        public AccessToken refreshAccessToken() throws IOException {
            refreshAccessTokenCount.incrementAndGet();
            if (refreshException != null) {
                throw refreshException;
            }
            if (runtimeException != null) {
                throw runtimeException;
            }
            return refreshedAccessToken;
        }

        protected void setRefreshedAccessToken(final AccessToken refreshedAccessToken) {
            this.refreshedAccessToken = refreshedAccessToken;
        }

        protected void setRefreshException(final IOException refreshException) {
            this.refreshException = refreshException;
        }

        protected void setRuntimeException(final RuntimeException runtimeException) {
            this.runtimeException = runtimeException;
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

    private static ImpersonatedCredentials impersonatedCredentials(final RuntimeException exception) throws IOException {
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
