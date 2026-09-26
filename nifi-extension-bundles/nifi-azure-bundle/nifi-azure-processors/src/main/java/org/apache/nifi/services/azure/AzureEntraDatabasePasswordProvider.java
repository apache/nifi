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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.api.DatabaseCredentialPlacement;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Tags({"azure", "microsoft entra", "jdbc", "database password", "sql server", "authentication"})
@CapabilityDescription("""
        Acquires short-lived Microsoft Entra database credentials for JDBC authentication. Depending on the configured database target,
        the credential is supplied to DBCP as either a database password or a JDBC access token.
        """)
public class AzureEntraDatabasePasswordProvider extends AbstractControllerService
        implements DatabasePasswordProvider, VerifiableControllerService {

    static final String OSS_RDBMS_SCOPE = "https://ossrdbms-aad.database.windows.net/.default";
    static final String SQL_SERVER_SCOPE = "https://database.windows.net/.default";
    static final String FAILED_CREDENTIAL_RESOLUTION_MESSAGE = "Failed to resolve Azure credentials for Microsoft Entra database password.";
    static final String FAILED_TOKEN_ACQUISITION_MESSAGE = "Failed to acquire a valid Microsoft Entra database access token.";
    static final String VERIFY_CREDENTIALS_STEP = "Resolve Azure credentials";
    static final String VERIFY_TOKEN_STEP = "Acquire Microsoft Entra database access token";
    static final String VERIFY_CREDENTIALS_UNAVAILABLE = "Configured Azure Credentials Service did not return Azure credentials.";
    static final String VERIFY_TOKEN_ACQUISITION_FAILED = "Failed to acquire a valid Microsoft Entra database access token.";
    static final Duration DEFAULT_TOKEN_ACQUISITION_TIMEOUT = Duration.ofMinutes(2);

    static final PropertyDescriptor AZURE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("Azure Credentials Service")
            .description("Controller Service that provides the Azure credentials used to request Microsoft Entra database access tokens.")
            .identifiesControllerService(AzureCredentialsService.class)
            .required(true)
            .build();

    static final PropertyDescriptor DATABASE_TARGET = new PropertyDescriptor.Builder()
            .name("Database Target")
            .description("Selects the Microsoft Entra token scope and JDBC credential placement used for the target database.")
            .allowableValues(DatabaseTarget.class)
            .defaultValue(DatabaseTarget.AZURE_OSS_RDBMS)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AZURE_CREDENTIALS_SERVICE,
            DATABASE_TARGET
    );

    private final Duration tokenAcquisitionTimeout;
    private volatile AzureCredentialsService azureCredentialsService;
    private volatile DatabaseTarget databaseTarget;

    enum DatabaseTarget implements DescribedValue {
        AZURE_OSS_RDBMS(OSS_RDBMS_SCOPE, DatabaseCredentialPlacement.PASSWORD, "Azure OSS Database",
                "Azure Database for PostgreSQL and Azure Database for MySQL. Requests the Azure OSS RDBMS scope and supplies the token as the JDBC password."),
        SQL_SERVER(SQL_SERVER_SCOPE, DatabaseCredentialPlacement.ACCESS_TOKEN, "Microsoft SQL Server",
                "Requests the SQL Server scope and supplies the token using the JDBC accessToken property.");

        private final String scope;
        private final DatabaseCredentialPlacement credentialPlacement;
        private final String displayName;
        private final String description;

        DatabaseTarget(final String scope, final DatabaseCredentialPlacement credentialPlacement, final String displayName, final String description) {
            this.scope = scope;
            this.credentialPlacement = credentialPlacement;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        private String getScope() {
            return scope;
        }

        private DatabaseCredentialPlacement getCredentialPlacement() {
            return credentialPlacement;
        }
    }

    public AzureEntraDatabasePasswordProvider() {
        this(DEFAULT_TOKEN_ACQUISITION_TIMEOUT);
    }

    AzureEntraDatabasePasswordProvider(final Duration tokenAcquisitionTimeout) {
        this.tokenAcquisitionTimeout = requirePositiveDuration(tokenAcquisitionTimeout);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        azureCredentialsService = resolveAzureCredentialsService(context);
        databaseTarget = getDatabaseTarget(context);
    }

    @OnDisabled
    public void onDisabled() {
        azureCredentialsService = null;
        databaseTarget = null;
    }

    @Override
    public DatabaseCredentialPlacement getDatabaseCredentialPlacement() {
        final DatabaseTarget configuredDatabaseTarget = databaseTarget;
        return configuredDatabaseTarget == null ? DatabaseCredentialPlacement.PASSWORD : configuredDatabaseTarget.getCredentialPlacement();
    }

    @Override
    public char[] getPassword(final DatabasePasswordRequestContext requestContext) {
        Objects.requireNonNull(requestContext, "Database Password Request Context required");

        final AzureCredentialsService configuredCredentialsService = azureCredentialsService;
        final DatabaseTarget configuredDatabaseTarget = databaseTarget == null ? DatabaseTarget.AZURE_OSS_RDBMS : databaseTarget;
        if (configuredCredentialsService == null) {
            throw new ProcessException(FAILED_CREDENTIAL_RESOLUTION_MESSAGE);
        }

        final TokenCredential credential;
        try {
            credential = configuredCredentialsService.getCredentials();
        } catch (final RuntimeException e) {
            throw new ProcessException(FAILED_CREDENTIAL_RESOLUTION_MESSAGE);
        }

        if (credential == null) {
            throw new ProcessException(FAILED_CREDENTIAL_RESOLUTION_MESSAGE);
        }

        final AccessToken accessToken;
        try {
            accessToken = credential.getToken(createTokenRequestContext(configuredDatabaseTarget)).block(tokenAcquisitionTimeout);
        } catch (final RuntimeException e) {
            throw new ProcessException(FAILED_TOKEN_ACQUISITION_MESSAGE);
        }

        if (!isValidAccessToken(accessToken)) {
            throw new ProcessException(FAILED_TOKEN_ACQUISITION_MESSAGE);
        }

        return accessToken.getToken().toCharArray();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(2);

        final TokenCredential verificationCredential = resolveVerificationCredential(context, verificationLogger);
        if (verificationCredential == null) {
            results.add(buildVerificationResult(VERIFY_CREDENTIALS_STEP, Outcome.FAILED, VERIFY_CREDENTIALS_UNAVAILABLE));
            return results;
        }

        results.add(buildVerificationResult(
                VERIFY_CREDENTIALS_STEP,
                Outcome.SUCCESSFUL,
                "Resolved Azure credentials service and current TokenCredential."
        ));
        results.add(verifyAccessToken(verificationCredential, verificationLogger, getDatabaseTarget(context)));
        return results;
    }

    private TokenCredential resolveVerificationCredential(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final AzureCredentialsService verificationCredentialsService = resolveAzureCredentialsService(context);
        if (verificationCredentialsService == null) {
            return null;
        }

        try {
            return verificationCredentialsService.getCredentials();
        } catch (final RuntimeException e) {
            verificationLogger.error(VERIFY_CREDENTIALS_UNAVAILABLE);
            return null;
        }
    }

    private ConfigVerificationResult verifyAccessToken(final TokenCredential credential, final ComponentLog verificationLogger,
                                                       final DatabaseTarget databaseTarget) {
        final AccessToken accessToken;
        try {
            accessToken = credential.getToken(createTokenRequestContext(databaseTarget)).block(tokenAcquisitionTimeout);
        } catch (final RuntimeException e) {
            verificationLogger.error(VERIFY_TOKEN_ACQUISITION_FAILED);
            return buildVerificationResult(VERIFY_TOKEN_STEP, Outcome.FAILED, VERIFY_TOKEN_ACQUISITION_FAILED);
        }

        if (!isValidAccessToken(accessToken)) {
            verificationLogger.error(VERIFY_TOKEN_ACQUISITION_FAILED);
            return buildVerificationResult(VERIFY_TOKEN_STEP, Outcome.FAILED, VERIFY_TOKEN_ACQUISITION_FAILED);
        }

        return buildVerificationResult(
                VERIFY_TOKEN_STEP,
                Outcome.SUCCESSFUL,
                "Acquired a Microsoft Entra database access token. Use DBCP Verify to validate database connectivity."
        );
    }

    private AzureCredentialsService resolveAzureCredentialsService(final ConfigurationContext context) {
        return context.getProperty(AZURE_CREDENTIALS_SERVICE).asControllerService(AzureCredentialsService.class);
    }

    private DatabaseTarget getDatabaseTarget(final ConfigurationContext context) {
        return context.getProperty(DATABASE_TARGET).asAllowableValue(DatabaseTarget.class);
    }

    private TokenRequestContext createTokenRequestContext(final DatabaseTarget databaseTarget) {
        return new TokenRequestContext().addScopes(databaseTarget.getScope());
    }

    private Duration requirePositiveDuration(final Duration tokenAcquisitionTimeout) {
        final Duration configuredTimeout = Objects.requireNonNull(tokenAcquisitionTimeout, "Token acquisition timeout required");
        if (configuredTimeout.isZero() || configuredTimeout.isNegative()) {
            throw new IllegalArgumentException("Token acquisition timeout must be positive");
        }
        return configuredTimeout;
    }

    private boolean isValidAccessToken(final AccessToken accessToken) {
        return accessToken != null && StringUtils.isNotBlank(accessToken.getToken());
    }

    private ConfigVerificationResult buildVerificationResult(final String stepName, final Outcome outcome, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(stepName)
                .outcome(outcome)
                .explanation(explanation)
                .build();
    }
}
