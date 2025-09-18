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

package org.apache.nifi.processors.snowflake;

import org.apache.nifi.key.service.StandardPrivateKeyService;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processors.snowflake.util.SnowflakeAttributes;
import org.apache.nifi.processors.snowflake.util.SnowflakeInternalStageType;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool;
import org.apache.nifi.snowflake.service.StandardSnowflakeIngestManagerProviderService;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormat;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormat;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.nifi.processors.snowflake.GetSnowflakeIngestStatus.REL_FAILURE;
import static org.apache.nifi.processors.snowflake.GetSnowflakeIngestStatus.REL_RETRY;
import static org.apache.nifi.processors.snowflake.GetSnowflakeIngestStatus.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SnowflakeIngestIT {

    private static final String ENV_CONNECTION_URL = "NIFI_SNOWFLAKE_URL";
    private static final String ENV_USERNAME = "NIFI_SNOWFLAKE_USER";
    private static final String ENV_PASSWORD = "NIFI_SNOWFLAKE_PASSWORD";
    private static final String ENV_WAREHOUSE = "NIFI_SNOWFLAKE_WAREHOUSE";
    private static final String ENV_PRIVATE_KEY_PATH = "NIFI_SNOWFLAKE_PRIVATE_KEY_PATH";
    private static final String ENV_PRIVATE_KEY_PASSPHRASE = "NIFI_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE";
    private static final String ENV_ROLE = "NIFI_SNOWFLAKE_ROLE";

    private static final String TEST_DATABASE = "NIFI_TEST_DATABASE";
    private static final String TEST_SCHEMA = "NIFI_TEST_SCHEMA";
    private static final String TEST_TABLE = "NIFI_TEST_TABLE";
    private static final String TEST_STAGE = "NIFI_CSV_STAGE";
    private static final String TEST_PIPE = "NIFI_CSV_PIPE";

    private static final byte[] CSV_CONTENT = "id,value\n1,foo\n".getBytes(StandardCharsets.UTF_8);

    private static final int MAX_INGEST_ATTEMPTS = 10;
    private static final Duration INGEST_POLL_INTERVAL = Duration.ofSeconds(2);

    private SnowflakeEnvironment environment;

    @BeforeAll
    void setUpSuite() throws Exception {
        final ValidationResult validationResult = ValidationResult.fromSystemEnv();
        assumeTrue(validationResult.environment().isPresent(), validationResult.message());
        environment = validationResult.environment().get();

        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        initializeSnowflakeObjects();
    }

    @BeforeEach
    void prepareTestData() throws SQLException {
        if (environment == null) {
            return;
        }
        truncateTable();
        clearStage();
    }

    @AfterEach
    void cleanupTestData() throws SQLException {
        if (environment == null) {
            return;
        }
        truncateTable();
        clearStage();
    }

    @AfterAll
    void tearDownSuite() throws SQLException {
        if (environment == null) {
            return;
        }
        dropSnowflakeObjects();
    }

    @Test
    void testSnowflakeIngestFlow() throws Exception {
        final MockFlowFile stagedFlowFile = stageFlowFile();
        final MockFlowFile ingestRequest = startIngest(stagedFlowFile);
        waitForIngestCompletion(ingestRequest);
        verifyTableContents();
    }

    private MockFlowFile stageFlowFile() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutSnowflakeInternalStage.class);
        runner.setValidateExpressionUsage(false);
        configureConnectionProvider(runner, "snowflake-connection");
        runner.setProperty(PutSnowflakeInternalStage.SNOWFLAKE_CONNECTION_PROVIDER, "snowflake-connection");
        runner.setProperty(PutSnowflakeInternalStage.INTERNAL_STAGE_TYPE, SnowflakeInternalStageType.NAMED.getValue());
        runner.setProperty(PutSnowflakeInternalStage.DATABASE, TEST_DATABASE);
        runner.setProperty(PutSnowflakeInternalStage.SCHEMA, TEST_SCHEMA);
        runner.setProperty(PutSnowflakeInternalStage.INTERNAL_STAGE, TEST_STAGE);

        runner.enqueue(CSV_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSnowflakeInternalStage.REL_SUCCESS, 1);
        final MockFlowFile result = runner.getFlowFilesForRelationship(PutSnowflakeInternalStage.REL_SUCCESS).get(0);
        final String stagedFilePath = result.getAttribute(SnowflakeAttributes.ATTRIBUTE_STAGED_FILE_PATH);
        assertTrue(stagedFilePath != null && !stagedFilePath.isEmpty(), "Staged file path attribute missing");
        return result;
    }

    private MockFlowFile startIngest(final MockFlowFile stagedFlowFile) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(StartSnowflakeIngest.class);
        runner.setValidateExpressionUsage(false);
        configureIngestManager(runner, "snowflake-ingest");
        runner.setProperty(StartSnowflakeIngest.INGEST_MANAGER_PROVIDER, "snowflake-ingest");

        runner.enqueue(stagedFlowFile.getData(), new HashMap<>(stagedFlowFile.getAttributes()));
        runner.run();

        runner.assertAllFlowFilesTransferred(StartSnowflakeIngest.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(StartSnowflakeIngest.REL_SUCCESS).get(0);
    }

    private void waitForIngestCompletion(final MockFlowFile flowFile) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(GetSnowflakeIngestStatus.class);
        runner.setValidateExpressionUsage(false);
        configureIngestManager(runner, "snowflake-ingest-status");
        runner.setProperty(GetSnowflakeIngestStatus.INGEST_MANAGER_PROVIDER, "snowflake-ingest-status");

        final byte[] data = flowFile.getData();
        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

        for (int attempt = 0; attempt < MAX_INGEST_ATTEMPTS; attempt++) {
            runner.enqueue(data, new HashMap<>(attributes));
            runner.run();

            if (!runner.getFlowFilesForRelationship(REL_SUCCESS).isEmpty()) {
                runner.clearTransferState();
                return;
            }

            if (!runner.getFlowFilesForRelationship(REL_FAILURE).isEmpty()) {
                fail("Snowflake ingest reported failure");
            }

            if (!runner.getFlowFilesForRelationship(REL_RETRY).isEmpty()) {
                runner.clearTransferState();
                Thread.sleep(INGEST_POLL_INTERVAL.toMillis());
                continue;
            }

            runner.clearTransferState();
            Thread.sleep(INGEST_POLL_INTERVAL.toMillis());
        }

        fail("Snowflake ingest did not complete within the expected time");
    }

    private void verifyTableContents() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutSnowflakeInternalStage.class);
        runner.setValidateExpressionUsage(false);
        final SnowflakeComputingConnectionPool connectionService = configureConnectionProvider(runner, "snowflake-connection");
        try (Connection connection = connectionService.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT ID, VALUE FROM " + fullTableName())) {
            assertTrue(resultSet.next(), "Expected row in test table");
            assertEquals(1, resultSet.getInt("ID"));
            assertEquals("foo", resultSet.getString("VALUE"));
            assertTrue(!resultSet.next(), "Unexpected additional rows in test table");
        }
    }

    private SnowflakeComputingConnectionPool configureConnectionProvider(final TestRunner runner, final String identifier) throws Exception {
        final SnowflakeComputingConnectionPool connectionService = new SnowflakeComputingConnectionPool();
        runner.addControllerService(identifier, connectionService);
        runner.setProperty(connectionService, SnowflakeComputingConnectionPool.CONNECTION_URL_FORMAT, ConnectionUrlFormat.FULL_URL.getValue());
        runner.setProperty(connectionService, SnowflakeComputingConnectionPool.SNOWFLAKE_URL, environment.jdbcUrl());
        runner.setProperty(connectionService, SnowflakeComputingConnectionPool.SNOWFLAKE_USER, environment.username());
        runner.setProperty(connectionService, SnowflakeComputingConnectionPool.SNOWFLAKE_PASSWORD, environment.password());
        runner.setProperty(connectionService, SnowflakeComputingConnectionPool.SNOWFLAKE_WAREHOUSE, environment.warehouse());
        runner.setProperty(connectionService, SnowflakeProperties.DATABASE, TEST_DATABASE);
        runner.setProperty(connectionService, SnowflakeProperties.SCHEMA, TEST_SCHEMA);
        environment.role().ifPresent(role -> runner.setProperty(connectionService, "role", role));
        runner.enableControllerService(connectionService);
        return connectionService;
    }

    private void configureIngestManager(final TestRunner runner, final String identifier) throws Exception {
        final StandardSnowflakeIngestManagerProviderService ingestService = new StandardSnowflakeIngestManagerProviderService();
        runner.addControllerService(identifier, ingestService);

        final PrivateKeyService privateKeyService = configurePrivateKeyService(runner, identifier + "-pk");
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.FULL_URL.getValue());
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.HOST_URL, environment.connectionHost());
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.USER_NAME, environment.username());
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.DATABASE, TEST_DATABASE);
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.SCHEMA, TEST_SCHEMA);
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.PIPE, TEST_PIPE);
        runner.setProperty(ingestService, StandardSnowflakeIngestManagerProviderService.PRIVATE_KEY_SERVICE, privateKeyService.getIdentifier());
        runner.enableControllerService(ingestService);
    }

    private PrivateKeyService configurePrivateKeyService(final TestRunner runner, final String identifier) throws Exception {
        final StandardPrivateKeyService privateKeyService = new StandardPrivateKeyService();
        runner.addControllerService(identifier, privateKeyService);
        runner.setProperty(privateKeyService, StandardPrivateKeyService.KEY_FILE, environment.privateKeyPath().toString());
        runner.setProperty(privateKeyService, StandardPrivateKeyService.KEY_PASSWORD, environment.privateKeyPassphrase());
        runner.enableControllerService(privateKeyService);
        return privateKeyService;
    }

    private void initializeSnowflakeObjects() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + TEST_DATABASE);
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_DATABASE + "." + TEST_SCHEMA);
            statement.execute("CREATE TABLE IF NOT EXISTS " + fullTableName() + " (id INTEGER, value STRING)");
            statement.execute("""
                    CREATE STAGE IF NOT EXISTS %s.%s.%s
                        FILE_FORMAT = (
                            TYPE = 'CSV'
                            FIELD_DELIMITER = ','
                            SKIP_HEADER = 1
                            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                            ESCAPE_UNENCLOSED_FIELD = NONE
                        )
                    """.formatted(TEST_DATABASE, TEST_SCHEMA, TEST_STAGE));
            statement.execute("""
                    CREATE OR REPLACE PIPE %s.%s.%s
                        AUTO_INGEST = FALSE
                    AS
                        COPY INTO %s.%s.%s
                        FROM @%s.%s.%s
                        FILE_FORMAT = (
                            TYPE = 'CSV'
                            FIELD_DELIMITER = ','
                            SKIP_HEADER = 1
                            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                            ESCAPE_UNENCLOSED_FIELD = NONE
                        )
                    """.formatted(TEST_DATABASE, TEST_SCHEMA, TEST_PIPE,
                    TEST_DATABASE, TEST_SCHEMA, TEST_TABLE,
                    TEST_DATABASE, TEST_SCHEMA, TEST_STAGE));
        }

        waitForPipeReady();
    }

    private void truncateTable() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE IF EXISTS " + fullTableName());
        }
    }

    private void clearStage() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            final boolean hasResultSet = statement.execute("REMOVE @" + fullStageName());
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet != null && resultSet.next()) {
                        // consume result set
                    }
                }
            }
        }
    }

    private void dropSnowflakeObjects() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP PIPE IF EXISTS " + fullPipeName());
            statement.execute("DROP STAGE IF EXISTS " + fullStageName());
            statement.execute("DROP TABLE IF EXISTS " + fullTableName());
            statement.execute("DROP SCHEMA IF EXISTS " + TEST_DATABASE + "." + TEST_SCHEMA + " CASCADE");
            statement.execute("DROP DATABASE IF EXISTS " + TEST_DATABASE);
        }
    }

    private void waitForPipeReady() throws SQLException {
        final String pipeName = fullPipeName();
        final String query = "SELECT SYSTEM$PIPE_STATUS('" + pipeName + "')";
        final int attempts = 10;
        final long delayMillis = 1_000L;

        for (int attempt = 0; attempt < attempts; attempt++) {
            try (Connection connection = openConnection(); Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
                if (resultSet.next()) {
                    final String status = resultSet.getString(1);
                    if (status != null && !status.isEmpty()) {
                        return;
                    }
                }
            }

            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Snowflake pipe readiness", interruptedException);
            }
        }
        throw new IllegalStateException("Snowflake pipe " + pipeName + " not ready after waiting");
    }

    private Connection openConnection() throws SQLException {
        final Properties properties = new Properties();
        properties.put("user", environment.username());
        properties.put("password", environment.password());
        properties.put("warehouse", environment.warehouse());
        environment.role().ifPresent(role -> properties.put("role", role));
        return DriverManager.getConnection(environment.jdbcUrl(), properties);
    }

    private String fullTableName() {
        return TEST_DATABASE + "." + TEST_SCHEMA + "." + TEST_TABLE;
    }

    private String fullStageName() {
        return TEST_DATABASE + "." + TEST_SCHEMA + "." + TEST_STAGE;
    }

    private String fullPipeName() {
        return TEST_DATABASE + "." + TEST_SCHEMA + "." + TEST_PIPE;
    }

    private record SnowflakeEnvironment(String connectionHost,
                                         String username,
                                         String password,
                                         String warehouse,
                                         Path privateKeyPath,
                                         String privateKeyPassphrase,
                                         Optional<String> role) {
        String jdbcUrl() {
            final String url = connectionHost.startsWith("jdbc:snowflake://")
                    ? connectionHost
                    : "jdbc:snowflake://" + connectionHost;
            return url;
        }
    }

    private record ValidationResult(Optional<SnowflakeEnvironment> environment, String message) {
        static ValidationResult fromSystemEnv() {
            final Map<String, String> env = System.getenv();
            final List<String> issues = new ArrayList<>();

            final String connectionHost = readEnv(env, ENV_CONNECTION_URL, issues);
            final String username = readEnv(env, ENV_USERNAME, issues);
            final String password = readEnv(env, ENV_PASSWORD, issues);
            final String warehouse = readEnv(env, ENV_WAREHOUSE, issues);
            final String privateKeyPathValue = readEnv(env, ENV_PRIVATE_KEY_PATH, issues);
            final String privateKeyPassphrase = readEnv(env, ENV_PRIVATE_KEY_PASSPHRASE, issues);
            final Optional<String> role = Optional.ofNullable(clean(env.get(ENV_ROLE))).filter(value -> !value.isEmpty());

            Path privateKeyPath = null;
            if (privateKeyPathValue != null) {
                privateKeyPath = Path.of(privateKeyPathValue).toAbsolutePath();
                if (!Files.isRegularFile(privateKeyPath)) {
                    issues.add("Private key file not found at " + privateKeyPath);
                }
            }

            if (!issues.isEmpty()) {
                return new ValidationResult(Optional.empty(), String.join(", ", issues));
            }

            final SnowflakeEnvironment environment = new SnowflakeEnvironment(
                    connectionHost,
                    username,
                    password,
                    warehouse,
                    privateKeyPath,
                    privateKeyPassphrase,
                    role
            );
            return new ValidationResult(Optional.of(environment), "Snowflake integration tests enabled");
        }

        private static String readEnv(final Map<String, String> env, final String key, final List<String> issues) {
            final String value = clean(env.get(key));
            if (value == null || value.isEmpty()) {
                issues.add("Environment variable " + key + " is required");
                return null;
            }
            return value;
        }

        private static String clean(final String value) {
            return value == null ? null : value.trim();
        }
    }
}
