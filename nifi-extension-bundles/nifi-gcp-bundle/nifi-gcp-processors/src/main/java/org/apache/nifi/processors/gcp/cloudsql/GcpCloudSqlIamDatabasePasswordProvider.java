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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

@Tags({"gcp", "cloud sql", "postgresql", "mysql", "iam", "jdbc", "password"})
@CapabilityDescription("""
        Generates Google Cloud SQL IAM authentication tokens for Cloud SQL database connections.
        PostgreSQL and MySQL are supported.
        The generated access token replaces the database user password so that NiFi does not need to store long-lived credentials inside DBCP services.
        """)
public class GcpCloudSqlIamDatabasePasswordProvider extends AbstractControllerService implements DatabasePasswordProvider, VerifiableControllerService {

    static final String SQLSERVICE_LOGIN_SCOPE = "https://www.googleapis.com/auth/sqlservice.login";
    static final String FAILED_PASSWORD_MESSAGE = "Failed to generate Cloud SQL IAM database password";
    static final String POSTGRESQL_SSLMODE_PROPERTY = "sslmode";
    static final String MALFORMED_SSLMODE_MESSAGE = "PostgreSQL sslmode in JDBC URL is malformed for Cloud SQL IAM authentication";
    static final String VERIFY_DATABASE_TYPE_STEP = "Resolve Database Type";
    static final String VERIFY_SCOPE_STEP = "Resolve Cloud SQL scoped credentials";
    static final String VERIFY_TOKEN_STEP = "Acquire Cloud SQL IAM access token";
    static final String VERIFY_DATABASE_TYPE_UNSUPPORTED = "Configured Database Type is not supported for Cloud SQL IAM authentication.";
    static final String VERIFY_CREDENTIALS_UNAVAILABLE = "Configured GCP Credentials Provider Service did not return Google credentials.";
    static final String VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE = "Failed to create Cloud SQL scoped credentials from the configured provider.";
    static final String VERIFY_IMPERSONATION_REQUIRED = "Target service account impersonation is required for Workload Identity Federation Cloud SQL authentication.";
    static final String VERIFY_TOKEN_ACQUISITION_FAILED = "Failed to acquire a Cloud SQL IAM access token from the scoped credential.";
    static final String VERIFY_TOKEN_MISSING = "Scoped credential refresh did not return a non-empty Cloud SQL IAM access token.";
    static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql://";
    static final String MYSQL_SSL_MODE_PROPERTY = "sslMode";
    static final String MYSQL_USER_PROPERTY = "user";
    static final String MYSQL_PASSWORD_PROPERTY = "password";
    static final String MYSQL_DISABLED_AUTHENTICATION_PLUGINS_PROPERTY = "disabledAuthenticationPlugins";
    static final String MYSQL_USE_SSL_PROPERTY = "useSSL";
    static final String MYSQL_REQUIRE_SSL_PROPERTY = "requireSSL";
    static final String MYSQL_VERIFY_SERVER_CERTIFICATE_PROPERTY = "verifyServerCertificate";
    static final String MALFORMED_MYSQL_JDBC_URL_MESSAGE = "MySQL JDBC URL properties are malformed for Cloud SQL IAM authentication";
    static final String MYSQL_JDBC_URL_REQUIRED_MESSAGE = "MySQL JDBC URL must use the standard single-host jdbc:mysql:// format for Cloud SQL IAM authentication";
    static final String MYSQL_DRIVER_CLASS_REQUIRED_MESSAGE = "MySQL driver class must be configured as com.mysql.cj.jdbc.Driver for Cloud SQL IAM authentication";
    static final String MYSQL_SSL_MODE_REQUIRED_MESSAGE = "MySQL sslMode must be configured as REQUIRED, VERIFY_CA, or VERIFY_IDENTITY for Cloud SQL IAM authentication";
    static final String MYSQL_URL_CREDENTIALS_UNSUPPORTED_MESSAGE = "MySQL JDBC URL must not define user or password for Cloud SQL IAM authentication";
    static final String MYSQL_CONNECTION_PROPERTIES_USER_UNSUPPORTED_MESSAGE = "MySQL DBCP connection properties must not define user for Cloud SQL IAM authentication";
    static final String MYSQL_DISABLED_CLEAR_PASSWORD_UNSUPPORTED_MESSAGE =
            "MySQL disabledAuthenticationPlugins must not disable the clear-password authentication plugin required for Cloud SQL IAM authentication";
    static final String MYSQL_LEGACY_TLS_PROPERTIES_UNSUPPORTED_MESSAGE =
            "MySQL legacy TLS properties useSSL, requireSSL, and verifyServerCertificate are not supported for Cloud SQL IAM authentication";
    private static final List<String> SAFE_GOOGLE_AUTH_IO_MESSAGES = List.of(
            "Unable to refresh sourceCredentials",
            "Error requesting access token",
            "Unexpected error refreshing access token",
            "Error parsing expireTime:"
    );

    private static final Set<String> ACCEPTED_POSTGRESQL_SSL_MODES = Set.of("prefer", "require", "verify-ca", "verify-full");
    private static final Set<String> ACCEPTED_MYSQL_SSL_MODES = Set.of("REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY");
    private static final Set<String> DISABLED_MYSQL_CLEAR_PASSWORD_PLUGIN_NAMES = Set.of(
            "mysql_clear_password",
            "com.mysql.cj.protocol.a.authentication.mysqlclearpasswordplugin"
    );
    private static final Set<String> LEGACY_MYSQL_TLS_PROPERTIES = Set.of(
            MYSQL_USE_SSL_PROPERTY,
            MYSQL_REQUIRE_SSL_PROPERTY,
            MYSQL_VERIFY_SERVER_CERTIFICATE_PROPERTY
    );

    static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("GCP Credentials Provider Service")
            .description("Controller Service that provides the Google credentials used to request Cloud SQL IAM authentication tokens.")
            .identifiesControllerService(GCPCredentialsService.class)
            .required(true)
            .build();

    static final PropertyDescriptor DATABASE_TYPE = new PropertyDescriptor.Builder()
            .name("Database Type")
            .description("Cloud SQL database engine to authenticate. PostgreSQL and MySQL are supported.")
            .required(true)
            .allowableValues(CloudSqlDatabaseType.class)
            .defaultValue(CloudSqlDatabaseType.POSTGRESQL)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            DATABASE_TYPE
    );

    private volatile GoogleCredentials scopedCredentials;
    private volatile CloudSqlDatabaseType databaseType;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final CloudSqlDatabaseType configuredDatabaseType = resolveEnabledDatabaseType(context);
        final GoogleCredentials createdScopedCredentials = createSqlLoginScopedCredentials(resolveGoogleCredentials(context));
        if (createdScopedCredentials == null) {
            throw new InitializationException(FAILED_PASSWORD_MESSAGE);
        }
        rejectIdentityPoolCredentialsOnEnable(createdScopedCredentials);

        databaseType = configuredDatabaseType;
        scopedCredentials = createdScopedCredentials;
    }

    @OnDisabled
    public void onDisabled() {
        scopedCredentials = null;
        databaseType = null;
    }

    @Override
    public char[] getPassword(final DatabasePasswordRequestContext requestContext) {
        Objects.requireNonNull(requestContext, "Database Password Request Context required");

        final GoogleCredentials credentials = scopedCredentials;
        final CloudSqlDatabaseType configuredDatabaseType = databaseType;
        if (credentials == null || configuredDatabaseType == null) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }

        validateRequest(requestContext, configuredDatabaseType);
        rejectIdentityPoolCredentialsOnPasswordGeneration(credentials);

        final AccessToken accessToken = refreshAccessToken(credentials);
        if (!hasTokenValue(accessToken)) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }

        return accessToken.getTokenValue().toCharArray();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(2);
        final CloudSqlDatabaseType configuredDatabaseType;

        try {
            configuredDatabaseType = resolveConfiguredDatabaseType(context);
        } catch (final IllegalArgumentException e) {
            results.add(buildVerificationResult(VERIFY_DATABASE_TYPE_STEP, Outcome.FAILED, VERIFY_DATABASE_TYPE_UNSUPPORTED));
            return results;
        }

        final GoogleCredentials googleCredentials;

        try {
            googleCredentials = resolveGoogleCredentials(context);
        } catch (final RuntimeException e) {
            results.add(buildVerificationResult(VERIFY_SCOPE_STEP, Outcome.FAILED, VERIFY_CREDENTIALS_UNAVAILABLE));
            return results;
        }

        if (googleCredentials == null) {
            results.add(buildVerificationResult(VERIFY_SCOPE_STEP, Outcome.FAILED, VERIFY_CREDENTIALS_UNAVAILABLE));
            return results;
        }

        final GoogleCredentials scopedVerificationCredentials;
        try {
            scopedVerificationCredentials = createSqlLoginScopedCredentials(googleCredentials);
        } catch (final RuntimeException e) {
            results.add(buildVerificationResult(VERIFY_SCOPE_STEP, Outcome.FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE));
            return results;
        }

        if (scopedVerificationCredentials == null) {
            results.add(buildVerificationResult(VERIFY_SCOPE_STEP, Outcome.FAILED, VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE));
            return results;
        }

        final ConfigVerificationResult scopedCredentialResult = describeScopedCredential(scopedVerificationCredentials, configuredDatabaseType);
        results.add(scopedCredentialResult);
        if (scopedCredentialResult.getOutcome() == Outcome.FAILED) {
            return results;
        }

        final AccessToken accessToken;
        try {
            accessToken = scopedVerificationCredentials.refreshAccessToken();
        } catch (final IOException | RuntimeException e) {
            results.add(buildVerificationResult(VERIFY_TOKEN_STEP, Outcome.FAILED, VERIFY_TOKEN_ACQUISITION_FAILED));
            return results;
        }

        if (!hasTokenValue(accessToken)) {
            results.add(buildVerificationResult(VERIFY_TOKEN_STEP, Outcome.FAILED, VERIFY_TOKEN_MISSING));
            return results;
        }

        results.add(buildTokenVerificationResult(scopedVerificationCredentials, configuredDatabaseType));
        return results;
    }

    private CloudSqlDatabaseType resolveEnabledDatabaseType(final ConfigurationContext context) throws InitializationException {
        try {
            return resolveConfiguredDatabaseType(context);
        } catch (final IllegalArgumentException e) {
            throw new InitializationException(VERIFY_DATABASE_TYPE_UNSUPPORTED, e);
        }
    }

    private CloudSqlDatabaseType resolveConfiguredDatabaseType(final ConfigurationContext context) {
        final PropertyValue propertyValue = context.getProperty(DATABASE_TYPE);
        final CloudSqlDatabaseType configuredDatabaseType = propertyValue.asAllowableValue(CloudSqlDatabaseType.class);
        if (configuredDatabaseType == null) {
            throw new IllegalArgumentException("Database Type must be configured");
        }

        return configuredDatabaseType;
    }

    private AccessToken refreshAccessToken(final GoogleCredentials credentials) {
        try {
            credentials.refreshIfExpired();
        } catch (final IOException e) {
            if (isSafeGoogleAuthRefreshException(e)) {
                throw new ProcessException(FAILED_PASSWORD_MESSAGE, e);
            }
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        } catch (final RuntimeException e) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }

        return credentials.getAccessToken();
    }

    private boolean isSafeGoogleAuthRefreshException(final IOException exception) {
        final String message = exception.getMessage();
        final boolean knownSafeMessage = message != null && SAFE_GOOGLE_AUTH_IO_MESSAGES.stream()
                .anyMatch(message::startsWith);
        if (!knownSafeMessage) {
            return false;
        }

        for (final StackTraceElement stackTraceElement : exception.getStackTrace()) {
            if (stackTraceElement.getClassName().startsWith("com.google.auth.oauth2.")) {
                return true;
            }
        }

        return false;
    }

    private GoogleCredentials resolveGoogleCredentials(final ConfigurationContext context) {
        final GCPCredentialsService credentialsService = context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(GCPCredentialsService.class);
        if (credentialsService == null) {
            return null;
        }

        return credentialsService.getGoogleCredentials();
    }

    private GoogleCredentials createSqlLoginScopedCredentials(final GoogleCredentials googleCredentials) {
        if (googleCredentials == null) {
            return null;
        }

        return googleCredentials.createScoped(List.of(SQLSERVICE_LOGIN_SCOPE));
    }

    private void rejectIdentityPoolCredentialsOnEnable(final GoogleCredentials credentials) throws InitializationException {
        if (credentials instanceof IdentityPoolCredentials) {
            throw new InitializationException(VERIFY_IMPERSONATION_REQUIRED);
        }
    }

    private void rejectIdentityPoolCredentialsOnPasswordGeneration(final GoogleCredentials credentials) {
        if (credentials instanceof IdentityPoolCredentials) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }
    }

    private ConfigVerificationResult describeScopedCredential(final GoogleCredentials scopedVerificationCredentials,
                                                             final CloudSqlDatabaseType configuredDatabaseType) {
        if (scopedVerificationCredentials instanceof ImpersonatedCredentials) {
            return buildVerificationResult(
                    VERIFY_SCOPE_STEP,
                    Outcome.SUCCESSFUL,
                    ("Resolved Database Type %s, resolved Google credentials from the configured provider, and created "
                            + "a Cloud SQL scoped ImpersonatedCredentials instance. Target service account "
                            + "impersonation is active.")
                            .formatted(configuredDatabaseType.getDisplayName())
            );
        }

        if (scopedVerificationCredentials instanceof IdentityPoolCredentials) {
            return buildVerificationResult(
                    VERIFY_SCOPE_STEP,
                    Outcome.FAILED,
                    "Resolved Database Type %s, but %s"
                            .formatted(configuredDatabaseType.getDisplayName(), VERIFY_IMPERSONATION_REQUIRED)
            );
        }

        return buildVerificationResult(
                VERIFY_SCOPE_STEP,
                Outcome.SUCCESSFUL,
                "Resolved Database Type %s, resolved Google credentials from the configured provider, and created a Cloud SQL scoped %s instance."
                        .formatted(configuredDatabaseType.getDisplayName(), scopedVerificationCredentials.getClass().getSimpleName())
        );
    }

    private ConfigVerificationResult buildTokenVerificationResult(final GoogleCredentials scopedVerificationCredentials,
                                                                 final CloudSqlDatabaseType configuredDatabaseType) {
        if (scopedVerificationCredentials instanceof ImpersonatedCredentials) {
            return buildVerificationResult(
                    VERIFY_TOKEN_STEP,
                    Outcome.SUCCESSFUL,
                    ("Acquired a non-empty Cloud SQL IAM access token for %s from the scoped credential. This verifies live "
                            + "subject token exchange, Google STS, and target service account impersonation, but does "
                            + "not connect to the selected database. Use DBCP Verify for the end-to-end database check.")
                            .formatted(configuredDatabaseType.getDisplayName())
            );
        }

        return buildVerificationResult(
                VERIFY_TOKEN_STEP,
                Outcome.SUCCESSFUL,
                ("Acquired a non-empty Cloud SQL IAM access token for %s from the scoped credential. This verifies live "
                        + "Cloud SQL IAM token acquisition for the current principal, but does not connect to the selected database. Use "
                        + "DBCP Verify for the end-to-end database check.")
                        .formatted(configuredDatabaseType.getDisplayName())
        );
    }

    private boolean hasTokenValue(final AccessToken accessToken) {
        return accessToken != null && StringUtils.isNotBlank(accessToken.getTokenValue());
    }

    private ConfigVerificationResult buildVerificationResult(final String stepName, final Outcome outcome, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(stepName)
                .outcome(outcome)
                .explanation(explanation)
                .build();
    }

    private void validateRequest(final DatabasePasswordRequestContext requestContext, final CloudSqlDatabaseType configuredDatabaseType) {
        final Consumer<DatabasePasswordRequestContext> validator = switch (configuredDatabaseType) {
            case POSTGRESQL -> this::validatePostgresqlRequest;
            case MYSQL -> this::validateMySqlRequest;
        };
        validator.accept(requestContext);
    }

    private void validatePostgresqlRequest(final DatabasePasswordRequestContext requestContext) {
        validatePostgresqlDatabaseUser(requestContext.getDatabaseUser());
        validatePostgresqlSslMode(requestContext);
    }

    private void validatePostgresqlDatabaseUser(final String databaseUser) {
        if (StringUtils.isBlank(databaseUser)) {
            throw new ProcessException("Database Username must be configured for Cloud SQL IAM authentication");
        }
    }

    private void validatePostgresqlSslMode(final DatabasePasswordRequestContext requestContext) {
        final String sslMode = resolvePostgresqlSslMode(requestContext);
        if (sslMode == null) {
            throw new ProcessException("PostgreSQL sslmode must be configured for Cloud SQL IAM authentication");
        }

        if (!ACCEPTED_POSTGRESQL_SSL_MODES.contains(sslMode)) {
            throw new ProcessException("PostgreSQL sslmode [%s] is not supported for Cloud SQL IAM authentication".formatted(sslMode));
        }
    }

    private String resolvePostgresqlSslMode(final DatabasePasswordRequestContext requestContext) {
        final Optional<String> jdbcUrlSslMode = findJdbcUrlPropertyIgnoreCase(requestContext.getJdbcUrl(), POSTGRESQL_SSLMODE_PROPERTY, MALFORMED_SSLMODE_MESSAGE);
        if (jdbcUrlSslMode.isPresent()) {
            return normalizePostgresqlSslMode(jdbcUrlSslMode.get());
        }

        final String propertySslMode = findConnectionPropertyIgnoreCase(requestContext.getConnectionProperties(), POSTGRESQL_SSLMODE_PROPERTY).orElse(null);
        return normalizePostgresqlSslMode(propertySslMode);
    }

    private void validateMySqlRequest(final DatabasePasswordRequestContext requestContext) {
        validateMySqlDatabaseUser(requestContext.getDatabaseUser());
        validateMySqlDriverClassName(requestContext.getDriverClassName());
        validateMySqlJdbcUrl(requestContext.getJdbcUrl());
        validateMySqlUrlCredentials(requestContext.getJdbcUrl());
        validateMySqlConnectionPropertyUser(requestContext.getConnectionProperties());
        validateMySqlLegacyTlsProperties(requestContext);
        validateMySqlSslMode(requestContext);
        validateMySqlAuthenticationPlugins(requestContext);
    }

    private void validateMySqlDatabaseUser(final String databaseUser) {
        if (StringUtils.isBlank(databaseUser)) {
            throw new ProcessException("Database Username must be configured for Cloud SQL IAM authentication");
        }
    }

    private void validateMySqlDriverClassName(final String driverClassName) {
        if (!MYSQL_DRIVER_CLASS_NAME.equals(StringUtils.trimToEmpty(driverClassName))) {
            throw new ProcessException(MYSQL_DRIVER_CLASS_REQUIRED_MESSAGE);
        }
    }

    private void validateMySqlJdbcUrl(final String jdbcUrl) {
        final String trimmedJdbcUrl = StringUtils.trimToEmpty(jdbcUrl);
        if (!trimmedJdbcUrl.startsWith(MYSQL_JDBC_URL_PREFIX)) {
            throw new ProcessException(MYSQL_JDBC_URL_REQUIRED_MESSAGE);
        }

        validateMySqlJdbcUrlEncoding(trimmedJdbcUrl);

        final URI mysqlUri;
        try {
            mysqlUri = URI.create(trimmedJdbcUrl.substring("jdbc:".length()));
        } catch (final IllegalArgumentException e) {
            throw new ProcessException(MYSQL_JDBC_URL_REQUIRED_MESSAGE);
        }

        final String rawAuthority = mysqlUri.getRawAuthority();
        if (!"mysql".equalsIgnoreCase(mysqlUri.getScheme())
                || StringUtils.isBlank(rawAuthority)
                || StringUtils.isBlank(mysqlUri.getHost())
                || rawAuthority.contains(",")) {
            throw new ProcessException(MYSQL_JDBC_URL_REQUIRED_MESSAGE);
        }

        if (mysqlUri.getRawUserInfo() != null) {
            throw new ProcessException(MYSQL_URL_CREDENTIALS_UNSUPPORTED_MESSAGE);
        }
    }

    private void validateMySqlJdbcUrlEncoding(final String jdbcUrl) {
        final int queryStart = jdbcUrl.indexOf('?');
        if (queryStart >= 0 && queryStart < jdbcUrl.length() - 1) {
            final String query = jdbcUrl.substring(queryStart + 1);
            for (final String parameter : query.split("&")) {
                final int delimiterIndex = parameter.indexOf('=');
                final String rawName = delimiterIndex >= 0 ? parameter.substring(0, delimiterIndex) : parameter;
                final String rawValue = delimiterIndex >= 0 ? parameter.substring(delimiterIndex + 1) : "";
                urlDecode(rawName, MALFORMED_MYSQL_JDBC_URL_MESSAGE);
                urlDecode(rawValue, MALFORMED_MYSQL_JDBC_URL_MESSAGE);
            }
        }
    }

    private void validateMySqlUrlCredentials(final String jdbcUrl) {
        if (findJdbcUrlPropertyIgnoreCase(jdbcUrl, MYSQL_USER_PROPERTY, MALFORMED_MYSQL_JDBC_URL_MESSAGE).isPresent()
                || findJdbcUrlPropertyIgnoreCase(jdbcUrl, MYSQL_PASSWORD_PROPERTY, MALFORMED_MYSQL_JDBC_URL_MESSAGE).isPresent()) {
            throw new ProcessException(MYSQL_URL_CREDENTIALS_UNSUPPORTED_MESSAGE);
        }
    }

    private void validateMySqlConnectionPropertyUser(final Map<String, String> connectionProperties) {
        if (findConnectionPropertyIgnoreCase(connectionProperties, MYSQL_USER_PROPERTY).isPresent()) {
            throw new ProcessException(MYSQL_CONNECTION_PROPERTIES_USER_UNSUPPORTED_MESSAGE);
        }
    }

    private void validateMySqlLegacyTlsProperties(final DatabasePasswordRequestContext requestContext) {
        for (final String propertyName : LEGACY_MYSQL_TLS_PROPERTIES) {
            if (findJdbcUrlPropertyIgnoreCase(requestContext.getJdbcUrl(), propertyName, MALFORMED_MYSQL_JDBC_URL_MESSAGE).isPresent()
                    || findConnectionPropertyIgnoreCase(requestContext.getConnectionProperties(), propertyName).isPresent()) {
                throw new ProcessException(MYSQL_LEGACY_TLS_PROPERTIES_UNSUPPORTED_MESSAGE);
            }
        }
    }

    private void validateMySqlSslMode(final DatabasePasswordRequestContext requestContext) {
        if (hasNonCanonicalConnectionProperty(requestContext.getConnectionProperties(), MYSQL_SSL_MODE_PROPERTY)
                || hasNonCanonicalJdbcUrlProperty(requestContext.getJdbcUrl(), MYSQL_SSL_MODE_PROPERTY, MALFORMED_MYSQL_JDBC_URL_MESSAGE)) {
            throw new ProcessException(MYSQL_SSL_MODE_REQUIRED_MESSAGE);
        }

        final String sslMode = resolveExactMySqlSslMode(requestContext)
                .map(this::normalizeMySqlSslMode)
                .orElse(null);
        if (sslMode == null || !ACCEPTED_MYSQL_SSL_MODES.contains(sslMode)) {
            throw new ProcessException(MYSQL_SSL_MODE_REQUIRED_MESSAGE);
        }
    }

    private void validateMySqlAuthenticationPlugins(final DatabasePasswordRequestContext requestContext) {
        validateMySqlDisabledAuthenticationPlugins(
                findConnectionPropertyIgnoreCase(requestContext.getConnectionProperties(), MYSQL_DISABLED_AUTHENTICATION_PLUGINS_PROPERTY)
        );
        validateMySqlDisabledAuthenticationPlugins(
                findJdbcUrlPropertyIgnoreCase(requestContext.getJdbcUrl(), MYSQL_DISABLED_AUTHENTICATION_PLUGINS_PROPERTY, MALFORMED_MYSQL_JDBC_URL_MESSAGE)
        );
    }

    private void validateMySqlDisabledAuthenticationPlugins(final Optional<String> disabledAuthenticationPlugins) {
        if (disabledAuthenticationPlugins.isEmpty()) {
            return;
        }

        for (final String disabledAuthenticationPlugin : disabledAuthenticationPlugins.get().split(",")) {
            final String normalizedPlugin = StringUtils.trimToEmpty(disabledAuthenticationPlugin).toLowerCase(Locale.ROOT);
            if (DISABLED_MYSQL_CLEAR_PASSWORD_PLUGIN_NAMES.contains(normalizedPlugin)) {
                throw new ProcessException(MYSQL_DISABLED_CLEAR_PASSWORD_UNSUPPORTED_MESSAGE);
            }
        }
    }

    private Optional<String> resolveExactMySqlSslMode(final DatabasePasswordRequestContext requestContext) {
        final Optional<String> connectionProperty = findConnectionPropertyExact(requestContext.getConnectionProperties(), MYSQL_SSL_MODE_PROPERTY);
        if (connectionProperty.isPresent()) {
            return connectionProperty;
        }

        final Optional<String> jdbcUrlProperty = findJdbcUrlPropertyExact(requestContext.getJdbcUrl(), MYSQL_SSL_MODE_PROPERTY, MALFORMED_MYSQL_JDBC_URL_MESSAGE);
        if (jdbcUrlProperty.isPresent()) {
            return jdbcUrlProperty;
        }

        return Optional.empty();
    }

    private Optional<String> findJdbcUrlPropertyExact(final String jdbcUrl, final String propertyName, final String malformedPropertyMessage) {
        return findJdbcUrlProperty(jdbcUrl, propertyName, malformedPropertyMessage, true);
    }

    private Optional<String> findJdbcUrlPropertyIgnoreCase(final String jdbcUrl, final String propertyName, final String malformedPropertyMessage) {
        return findJdbcUrlProperty(jdbcUrl, propertyName, malformedPropertyMessage, false);
    }

    private Optional<String> findJdbcUrlProperty(final String jdbcUrl, final String propertyName, final String malformedPropertyMessage,
                                                 final boolean exactMatch) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return Optional.empty();
        }

        final String normalizedJdbcUrl = jdbcUrl.startsWith("jdbc:") ? jdbcUrl.substring(5) : jdbcUrl;
        final int queryStart = normalizedJdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == normalizedJdbcUrl.length() - 1) {
            return Optional.empty();
        }

        final String query = normalizedJdbcUrl.substring(queryStart + 1);
        String lastValue = null;
        for (final String parameter : query.split("&")) {
            if (parameter.isEmpty()) {
                continue;
            }

            final int delimiterIndex = parameter.indexOf('=');
            final String decodedName = urlDecode(delimiterIndex >= 0 ? parameter.substring(0, delimiterIndex) : parameter, malformedPropertyMessage);
            if (!propertyNamesMatch(propertyName, decodedName, exactMatch)) {
                continue;
            }

            final String rawValue = delimiterIndex >= 0 ? parameter.substring(delimiterIndex + 1) : "";
            lastValue = urlDecode(rawValue, malformedPropertyMessage);
        }

        return Optional.ofNullable(lastValue);
    }

    private Optional<String> findConnectionPropertyExact(final Map<String, String> connectionProperties, final String propertyName) {
        return Optional.ofNullable(connectionProperties.get(propertyName));
    }

    private Optional<String> findConnectionPropertyIgnoreCase(final Map<String, String> connectionProperties, final String propertyName) {
        for (final Map.Entry<String, String> entry : connectionProperties.entrySet()) {
            if (propertyName.equalsIgnoreCase(entry.getKey())) {
                return Optional.ofNullable(entry.getValue());
            }
        }

        return Optional.empty();
    }

    private boolean hasNonCanonicalJdbcUrlProperty(final String jdbcUrl, final String propertyName, final String malformedPropertyMessage) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return false;
        }

        final String normalizedJdbcUrl = jdbcUrl.startsWith("jdbc:") ? jdbcUrl.substring(5) : jdbcUrl;
        final int queryStart = normalizedJdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == normalizedJdbcUrl.length() - 1) {
            return false;
        }

        final String query = normalizedJdbcUrl.substring(queryStart + 1);
        for (final String parameter : query.split("&")) {
            if (parameter.isEmpty()) {
                continue;
            }

            final int delimiterIndex = parameter.indexOf('=');
            final String decodedName = urlDecode(delimiterIndex >= 0 ? parameter.substring(0, delimiterIndex) : parameter, malformedPropertyMessage);
            if (propertyName.equalsIgnoreCase(decodedName) && !propertyName.equals(decodedName)) {
                return true;
            }
        }

        return false;
    }

    private boolean hasNonCanonicalConnectionProperty(final Map<String, String> connectionProperties, final String propertyName) {
        for (final String configuredPropertyName : connectionProperties.keySet()) {
            if (propertyName.equalsIgnoreCase(configuredPropertyName) && !propertyName.equals(configuredPropertyName)) {
                return true;
            }
        }

        return false;
    }

    private boolean propertyNamesMatch(final String expectedPropertyName, final String configuredPropertyName, final boolean exactMatch) {
        return exactMatch ? expectedPropertyName.equals(configuredPropertyName) : expectedPropertyName.equalsIgnoreCase(configuredPropertyName);
    }

    private String normalizePostgresqlSslMode(final String sslMode) {
        return sslMode == null ? null : StringUtils.trimToEmpty(sslMode).toLowerCase(Locale.ROOT);
    }

    private String normalizeMySqlSslMode(final String sslMode) {
        return sslMode == null ? null : StringUtils.trimToEmpty(sslMode).toUpperCase(Locale.ROOT);
    }

    private String urlDecode(final String value, final String malformedPropertyMessage) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (final IllegalArgumentException e) {
            throw new ProcessException(malformedPropertyMessage);
        }
    }
}
