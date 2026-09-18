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
package org.apache.nifi.services.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.processor.exception.ProcessException;
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
import org.slf4j.helpers.MessageFormatter;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AzureEntraDatabasePasswordProviderTest {

    private static final String CREDENTIALS_SERVICE_ID = "azureCredentials";
    private static final String PASSWORD_PROVIDER_ID = "azureEntraPasswordProvider";
    private static final String DRIVER_CLASS = "org.postgresql.Driver";
    private static final String DATABASE_USER = "entra-user@example.com";
    private static final String JDBC_URL = "jdbc:postgresql://example.postgres.database.azure.com:5432/database";
    private static final String TOKEN_VALUE = "entra-database-token";
    private static final String REFRESHED_TOKEN_VALUE = "refreshed-entra-database-token";
    private static final String LEAK_SENTINEL = "sentinel-entra-secret-value";
    private static final Duration SHORT_TOKEN_ACQUISITION_TIMEOUT = Duration.ofMillis(50);

    private ExecutorService executorService;

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void testSupportedPropertyDescriptorsContainOnlyCredentialsServiceAndCredentialsPropertyIsRequired() throws Exception {
        final AzureEntraDatabasePasswordProvider provider = new AzureEntraDatabasePasswordProvider();
        final List<PropertyDescriptor> descriptors = provider.getSupportedPropertyDescriptors();

        assertEquals(1, descriptors.size());
        assertEquals(AzureEntraDatabasePasswordProvider.AZURE_CREDENTIALS_SERVICE, descriptors.get(0));
        assertTrue(descriptors.get(0).isRequired());
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        runner.addControllerService(PASSWORD_PROVIDER_ID, provider);
        runner.assertNotValid(provider);
    }

    @Test
    void testGetPasswordUsesCurrentCredentialForEveryCall() throws Exception {
        final TestAzureCredentialsService credentialsService = new TestAzureCredentialsService(
                new StaticTokenCredential(validToken(TOKEN_VALUE)),
                new StaticTokenCredential(validToken(REFRESHED_TOKEN_VALUE))
        );
        final DatabasePasswordProvider provider = getProvider(configureRunner(credentialsService));

        final char[] firstPassword = provider.getPassword(requestContext());
        final char[] firstPasswordCopy = firstPassword.clone();
        firstPassword[0] = 'X';
        final char[] secondPassword = provider.getPassword(requestContext());

        assertEquals(2, credentialsService.getGetCredentialsCount());
        assertNotSame(firstPassword, secondPassword);
        assertArrayEquals(TOKEN_VALUE.toCharArray(), firstPasswordCopy);
        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), secondPassword);
    }

    @Test
    void testGetPasswordRequestsExactOssRdbmsScope() throws Exception {
        final RecordingTokenCredential credential = new RecordingTokenCredential(Mono.just(validToken(TOKEN_VALUE)));
        final DatabasePasswordProvider provider = getProvider(configureRunner(new TestAzureCredentialsService(credential)));

        provider.getPassword(requestContext());

        assertEquals(List.of(AzureEntraDatabasePasswordProvider.OSS_RDBMS_SCOPE), credential.getLastRequestedScopes());
    }

    @Test
    void testVerifyBeforeEnableUsesCurrentCredentialAndLeavesEnabledStateUntouched() throws Exception {
        final RecordingTokenCredential credential = new RecordingTokenCredential(Mono.just(validToken(TOKEN_VALUE)));
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(credential), false);
        final AzureEntraDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, credential.getGetTokenCount());
        assertEquals(2, results.size());
        assertVerificationResult(results.get(0), AzureEntraDatabasePasswordProvider.VERIFY_CREDENTIALS_STEP, SUCCESSFUL,
                "Resolved Azure credentials service and current TokenCredential.");
        assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, SUCCESSFUL,
                "DBCP Verify");

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));
        assertEquals(AzureEntraDatabasePasswordProvider.FAILED_CREDENTIAL_RESOLUTION_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testVerifyUsesFreshCredentialWithoutMutatingEnabledState() throws Exception {
        final RecordingTokenCredential verifyCredential = new RecordingTokenCredential(Mono.just(validToken(TOKEN_VALUE)));
        final RecordingTokenCredential passwordCredential = new RecordingTokenCredential(Mono.just(validToken(REFRESHED_TOKEN_VALUE)));
        final TestAzureCredentialsService credentialsService = new TestAzureCredentialsService(verifyCredential, passwordCredential);
        final TestRunner runner = configureRunner(credentialsService);
        final AzureEntraDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());
        final char[] password = provider.getPassword(requestContext());

        assertEquals(2, credentialsService.getGetCredentialsCount());
        assertEquals(1, verifyCredential.getGetTokenCount());
        assertEquals(1, passwordCredential.getGetTokenCount());
        assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, SUCCESSFUL, "DBCP Verify");
        assertArrayEquals(REFRESHED_TOKEN_VALUE.toCharArray(), password);
    }

    @Test
    void testVerifyCredentialsResolutionFailureIsSanitized() throws Exception {
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(new IllegalStateException(LEAK_SENTINEL)), false);
        final AzureEntraDatabasePasswordProvider provider = getProviderImplementation(runner);

        final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), AzureEntraDatabasePasswordProvider.VERIFY_CREDENTIALS_STEP, FAILED,
                AzureEntraDatabasePasswordProvider.VERIFY_CREDENTIALS_UNAVAILABLE);
        assertFalse(results.get(0).getExplanation().contains(LEAK_SENTINEL));
        assertNoSensitiveLogging(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyNullCredentialFailsCredentialsStep() throws Exception {
        final TestRunner runner = configureRunner(new TestAzureCredentialsService((TokenCredential) null), false);

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(1, results.size());
        assertVerificationResult(results.get(0), AzureEntraDatabasePasswordProvider.VERIFY_CREDENTIALS_STEP, FAILED,
                AzureEntraDatabasePasswordProvider.VERIFY_CREDENTIALS_UNAVAILABLE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidAccessTokens")
    void testVerifyInvalidAccessTokenFailsTokenStep(final String testName, final AccessToken accessToken) throws Exception {
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(new StaticTokenCredential(accessToken)), false);

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, FAILED,
                AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_ACQUISITION_FAILED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptedAccessTokens")
    void testVerifyAcceptsAccessTokensWithoutFutureExpiration(final String testName, final AccessToken accessToken,
                                                              final String expectedPassword) throws Exception {
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(new StaticTokenCredential(accessToken)), false);

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, SUCCESSFUL,
                "DBCP Verify");
    }

    @Test
    void testVerifyTokenAcquisitionTimeoutFailsTokenStep() throws Exception {
        final AzureEntraDatabasePasswordProvider provider = new AzureEntraDatabasePasswordProvider(SHORT_TOKEN_ACQUISITION_TIMEOUT);
        final TestRunner runner = configureRunner(provider,
                new TestAzureCredentialsService(new RecordingTokenCredential(Mono.never())), false);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            final List<ConfigVerificationResult> results = runner.verify(provider, Map.of());

            assertEquals(2, results.size());
            assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, FAILED,
                    AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_ACQUISITION_FAILED);
        });
    }

    @Test
    void testVerifyTokenAcquisitionFailureIsSanitized() throws Exception {
        final TestRunner runner = configureRunner(
                new TestAzureCredentialsService(new RecordingTokenCredential(Mono.error(new IllegalStateException(LEAK_SENTINEL)))),
                false
        );

        final List<ConfigVerificationResult> results = runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(2, results.size());
        assertVerificationResult(results.get(1), AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_STEP, FAILED,
                AzureEntraDatabasePasswordProvider.VERIFY_TOKEN_ACQUISITION_FAILED);
        assertFalse(results.get(1).getExplanation().contains(LEAK_SENTINEL));
        assertNoSensitiveLogging(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testVerifyUsesExactOssRdbmsScope() throws Exception {
        final RecordingTokenCredential credential = new RecordingTokenCredential(Mono.just(validToken(TOKEN_VALUE)));
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(credential), false);

        runner.verify(getProviderImplementation(runner), Map.of());

        assertEquals(List.of(AzureEntraDatabasePasswordProvider.OSS_RDBMS_SCOPE), credential.getLastRequestedScopes());
    }

    @Test
    void testDisabledCallsFailBeforeEnable() throws Exception {
        final DatabasePasswordProvider provider = getProvider(configureRunner(
                new TestAzureCredentialsService(new StaticTokenCredential(validToken(TOKEN_VALUE))), false));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertEquals(AzureEntraDatabasePasswordProvider.FAILED_CREDENTIAL_RESOLUTION_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testOnDisabledClearsRetainedCredentialsServiceAndFailsClosed() throws Exception {
        final TestRunner runner = configureRunner(new TestAzureCredentialsService(new StaticTokenCredential(validToken(TOKEN_VALUE))));
        final AzureEntraDatabasePasswordProvider provider = getProviderImplementation(runner);

        runner.disableControllerService(provider);

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));
        assertEquals(AzureEntraDatabasePasswordProvider.FAILED_CREDENTIAL_RESOLUTION_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidAccessTokens")
    void testGetPasswordRejectsInvalidAccessToken(final String testName, final AccessToken accessToken) throws Exception {
        final DatabasePasswordProvider provider = getProvider(configureRunner(
                new TestAzureCredentialsService(new StaticTokenCredential(accessToken))));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertEquals(AzureEntraDatabasePasswordProvider.FAILED_TOKEN_ACQUISITION_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptedAccessTokens")
    void testGetPasswordPassesThroughAccessTokensWithoutFutureExpiration(final String testName, final AccessToken accessToken,
                                                                         final String expectedPassword) throws Exception {
        final DatabasePasswordProvider provider = getProvider(configureRunner(
                new TestAzureCredentialsService(new StaticTokenCredential(accessToken))));

        final char[] password = provider.getPassword(requestContext());

        assertArrayEquals(expectedPassword.toCharArray(), password);
    }

    @Test
    void testGetPasswordTokenAcquisitionTimeoutFailsClosed() throws Exception {
        final AzureEntraDatabasePasswordProvider provider = new AzureEntraDatabasePasswordProvider(SHORT_TOKEN_ACQUISITION_TIMEOUT);
        final DatabasePasswordProvider configuredProvider = getProvider(configureRunner(provider,
                new TestAzureCredentialsService(new RecordingTokenCredential(Mono.never()))));

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            final ProcessException exception = assertThrows(ProcessException.class, () -> configuredProvider.getPassword(requestContext()));

            assertEquals(AzureEntraDatabasePasswordProvider.FAILED_TOKEN_ACQUISITION_MESSAGE, exception.getMessage());
            assertNull(exception.getCause());
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sanitizedPasswordFailures")
    void testPasswordGenerationFailuresAreSanitized(final String testName,
                                                    final Supplier<TestAzureCredentialsService> credentialsServiceSupplier,
                                                    final String expectedMessage) throws Exception {
        final TestRunner runner = configureRunner(credentialsServiceSupplier.get());
        final DatabasePasswordProvider provider = getProvider(runner);

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertEquals(expectedMessage, exception.getMessage());
        assertNull(exception.getCause());
        assertFalse(exception.getMessage().contains(LEAK_SENTINEL));
        assertNoSensitiveLogging(runner.getControllerServiceLogger(PASSWORD_PROVIDER_ID), LEAK_SENTINEL);
    }

    @Test
    void testNullCredentialRejectedForPasswordGeneration() throws Exception {
        final DatabasePasswordProvider provider = getProvider(configureRunner(new TestAzureCredentialsService((TokenCredential) null)));

        final ProcessException exception = assertThrows(ProcessException.class, () -> provider.getPassword(requestContext()));

        assertEquals(AzureEntraDatabasePasswordProvider.FAILED_CREDENTIAL_RESOLUTION_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConcurrentGetPasswordUsesSeparateCredentialResolutionPerCall() throws Exception {
        final BlockingTokenCredential firstCredential = new BlockingTokenCredential(validToken(TOKEN_VALUE));
        final BlockingTokenCredential secondCredential = new BlockingTokenCredential(validToken(REFRESHED_TOKEN_VALUE));
        final TestAzureCredentialsService credentialsService = new TestAzureCredentialsService(firstCredential, secondCredential);
        final DatabasePasswordProvider provider = getProvider(configureRunner(credentialsService));

        executorService = Executors.newFixedThreadPool(2);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final Future<char[]> first = executorService.submit(() -> getPasswordAfterStart(provider, startLatch));
        final Future<char[]> second = executorService.submit(() -> getPasswordAfterStart(provider, startLatch));

        startLatch.countDown();
        assertTrue(firstCredential.awaitGetTokenEntry());
        assertTrue(secondCredential.awaitGetTokenEntry());
        firstCredential.releaseGetToken();
        secondCredential.releaseGetToken();

        final List<String> passwords = List.of(
                new String(first.get(5, TimeUnit.SECONDS)),
                new String(second.get(5, TimeUnit.SECONDS))
        );

        assertTrue(passwords.contains(TOKEN_VALUE));
        assertTrue(passwords.contains(REFRESHED_TOKEN_VALUE));
        assertEquals(2, credentialsService.getGetCredentialsCount());
        assertEquals(1, firstCredential.getGetTokenCount());
        assertEquals(1, secondCredential.getGetTokenCount());
    }

    @Test
    void testControllerServiceRegistrationContainsProvider() throws IOException {
        final String resourcePath = "META-INF/services/org.apache.nifi.controller.ControllerService";
        try (InputStream inputStream = AzureEntraDatabasePasswordProvider.class.getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull(inputStream);
            final String registeredServices = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(registeredServices.contains(AzureEntraDatabasePasswordProvider.class.getName()));
        }
    }

    private TestRunner configureRunner(final TestAzureCredentialsService credentialsService) throws Exception {
        return configureRunner(new AzureEntraDatabasePasswordProvider(), credentialsService, true);
    }

    private TestRunner configureRunner(final TestAzureCredentialsService credentialsService, final boolean enableProvider) throws Exception {
        return configureRunner(new AzureEntraDatabasePasswordProvider(), credentialsService, enableProvider);
    }

    private TestRunner configureRunner(final AzureEntraDatabasePasswordProvider provider,
                                       final TestAzureCredentialsService credentialsService) throws Exception {
        return configureRunner(provider, credentialsService, true);
    }

    private TestRunner configureRunner(final AzureEntraDatabasePasswordProvider provider,
                                       final TestAzureCredentialsService credentialsService,
                                       final boolean enableProvider) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentialsService);
        runner.enableControllerService(credentialsService);

        runner.addControllerService(PASSWORD_PROVIDER_ID, provider);
        runner.setProperty(provider, AzureEntraDatabasePasswordProvider.AZURE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_ID);
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

    private AzureEntraDatabasePasswordProvider getProviderImplementation(final TestRunner runner) {
        return (AzureEntraDatabasePasswordProvider) getProvider(runner);
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

    private static Stream<Arguments> invalidAccessTokens() {
        return Stream.of(
                Arguments.of("null access token", null),
                Arguments.of("null token string", token(null, OffsetDateTime.now().plusMinutes(15))),
                Arguments.of("blank token string", validToken("  "))
        );
    }

    private static Stream<Arguments> acceptedAccessTokens() {
        return Stream.of(
                Arguments.of("expired token", expiredToken(TOKEN_VALUE), TOKEN_VALUE),
                Arguments.of("null expiration", token(REFRESHED_TOKEN_VALUE, null), REFRESHED_TOKEN_VALUE)
        );
    }

    private static Stream<Arguments> sanitizedPasswordFailures() {
        return Stream.of(
                Arguments.of("credentials resolution failure",
                        (Supplier<TestAzureCredentialsService>) () -> new TestAzureCredentialsService(new IllegalStateException(LEAK_SENTINEL)),
                        AzureEntraDatabasePasswordProvider.FAILED_CREDENTIAL_RESOLUTION_MESSAGE),
                Arguments.of("token acquisition failure",
                        (Supplier<TestAzureCredentialsService>) () -> new TestAzureCredentialsService(
                                new RecordingTokenCredential(Mono.error(new IllegalStateException(LEAK_SENTINEL)))),
                        AzureEntraDatabasePasswordProvider.FAILED_TOKEN_ACQUISITION_MESSAGE)
        );
    }

    private static AccessToken validToken(final String tokenValue) {
        return token(tokenValue, OffsetDateTime.now().plusMinutes(15));
    }

    private static AccessToken expiredToken(final String tokenValue) {
        return token(tokenValue, OffsetDateTime.now().minusMinutes(15));
    }

    private static AccessToken token(final String tokenValue, final OffsetDateTime expiresAt) {
        return new AccessToken(tokenValue, expiresAt);
    }

    private static void assertVerificationResult(final ConfigVerificationResult result, final String stepName,
                                                 final ConfigVerificationResult.Outcome outcome, final String explanationFragment) {
        assertEquals(stepName, result.getVerificationStepName());
        assertEquals(outcome, result.getOutcome());
        assertTrue(result.getExplanation().contains(explanationFragment), result::getExplanation);
    }

    private static void assertNoSensitiveLogging(final MockComponentLog logger, final String value) {
        final List<LogMessage> logMessages = new ArrayList<>();
        logMessages.addAll(logger.getDebugMessages());
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
                    assertFalse(arg instanceof Throwable);
                }
            }
            assertNull(logMessage.getThrowable());
        }
    }

    private static final class TestAzureCredentialsService extends AbstractControllerService implements AzureCredentialsService {
        private final List<TokenCredential> credentials;
        private final RuntimeException credentialsException;
        private final AtomicInteger getCredentialsCount = new AtomicInteger();

        private TestAzureCredentialsService(final TokenCredential... credentials) {
            this.credentials = new ArrayList<>(Arrays.asList(credentials));
            this.credentialsException = null;
        }

        private TestAzureCredentialsService(final RuntimeException credentialsException) {
            this.credentials = List.of();
            this.credentialsException = credentialsException;
        }

        @Override
        public TokenCredential getCredentials() {
            final int callIndex = getCredentialsCount.getAndIncrement();
            if (credentialsException != null) {
                throw credentialsException;
            }

            if (credentials.isEmpty()) {
                return null;
            }

            return credentials.get(Math.min(callIndex, credentials.size() - 1));
        }

        private int getGetCredentialsCount() {
            return getCredentialsCount.get();
        }
    }

    private static class RecordingTokenCredential implements TokenCredential {
        private final Mono<AccessToken> tokenMono;
        private final AtomicInteger getTokenCount = new AtomicInteger();
        private final List<List<String>> requestedScopes = new CopyOnWriteArrayList<>();

        private RecordingTokenCredential(final Mono<AccessToken> tokenMono) {
            this.tokenMono = tokenMono;
        }

        @Override
        public Mono<AccessToken> getToken(final TokenRequestContext request) {
            getTokenCount.incrementAndGet();
            requestedScopes.add(List.copyOf(request.getScopes()));
            return tokenMono;
        }

        private int getGetTokenCount() {
            return getTokenCount.get();
        }

        private List<String> getLastRequestedScopes() {
            return requestedScopes.isEmpty() ? List.of() : requestedScopes.get(requestedScopes.size() - 1);
        }
    }

    private static final class StaticTokenCredential extends RecordingTokenCredential {
        private StaticTokenCredential(final AccessToken accessToken) {
            super(Mono.justOrEmpty(accessToken));
        }
    }

    private static final class BlockingTokenCredential implements TokenCredential {
        private final AccessToken accessToken;
        private final CountDownLatch getTokenEnteredLatch = new CountDownLatch(1);
        private final CountDownLatch releaseGetTokenLatch = new CountDownLatch(1);
        private final AtomicInteger getTokenCount = new AtomicInteger();

        private BlockingTokenCredential(final AccessToken accessToken) {
            this.accessToken = accessToken;
        }

        @Override
        public Mono<AccessToken> getToken(final TokenRequestContext request) {
            getTokenCount.incrementAndGet();
            return Mono.fromCallable(() -> {
                getTokenEnteredLatch.countDown();
                if (!releaseGetTokenLatch.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for token release");
                }
                return accessToken;
            });
        }

        private boolean awaitGetTokenEntry() throws InterruptedException {
            return getTokenEnteredLatch.await(5, TimeUnit.SECONDS);
        }

        private void releaseGetToken() {
            releaseGetTokenLatch.countDown();
        }

        private int getGetTokenCount() {
            return getTokenCount.get();
        }
    }
}
