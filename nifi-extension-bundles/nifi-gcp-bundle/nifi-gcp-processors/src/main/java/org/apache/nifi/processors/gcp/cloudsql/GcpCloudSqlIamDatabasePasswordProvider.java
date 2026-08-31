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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Tags({"gcp", "cloud sql", "postgresql", "mysql", "iam", "jdbc", "password"})
@CapabilityDescription("""
        Generates Google Cloud SQL IAM authentication tokens for Cloud SQL database connections.
        PostgreSQL and MySQL are supported.
        The generated access token replaces the database user password so that NiFi does not need to store long-lived credentials inside DBCP services.
        """)
public class GcpCloudSqlIamDatabasePasswordProvider extends AbstractControllerService implements DatabasePasswordProvider, VerifiableControllerService {

    static final String SQLSERVICE_LOGIN_SCOPE = "https://www.googleapis.com/auth/sqlservice.login";
    static final String FAILED_PASSWORD_MESSAGE = "Failed to generate Cloud SQL IAM database password";
    static final String VERIFY_SCOPE_STEP = "Resolve GCP credentials";
    static final String VERIFY_TOKEN_STEP = "Acquire Cloud SQL IAM access token";
    static final String VERIFY_CREDENTIALS_UNAVAILABLE = "Configured GCP Credentials Provider Service did not return Google credentials.";
    static final String VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE = "Failed to apply the Cloud SQL login scope to the configured Google credentials.";
    static final String VERIFY_IMPERSONATION_REQUIRED = "Target service account impersonation is required for Workload Identity Federation Cloud SQL authentication.";
    static final String VERIFY_TOKEN_ACQUISITION_FAILED = "Failed to acquire a Cloud SQL IAM access token.";
    static final String VERIFY_TOKEN_MISSING = "Cloud SQL IAM access token was empty.";

    static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("GCP Credentials Provider Service")
            .description("Controller Service that provides the Google credentials used to request Cloud SQL IAM authentication tokens.")
            .identifiesControllerService(GCPCredentialsService.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE
    );

    private volatile GoogleCredentials scopedCredentials;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        scopedCredentials = requireScopedCredentials(context);
    }

    @OnDisabled
    public void onDisabled() {
        scopedCredentials = null;
    }

    @Override
    public char[] getPassword(final DatabasePasswordRequestContext requestContext) {
        Objects.requireNonNull(requestContext, "Database Password Request Context required");

        final GoogleCredentials credentials = scopedCredentials;
        if (credentials == null) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }

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

        final ConfigVerificationResult scopedCredentialResult = describeScopedCredential(scopedVerificationCredentials);
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

        results.add(buildTokenVerificationResult());
        return results;
    }

    private GoogleCredentials requireScopedCredentials(final ConfigurationContext context) throws InitializationException {
        final GoogleCredentials googleCredentials;
        try {
            googleCredentials = resolveGoogleCredentials(context);
        } catch (final RuntimeException e) {
            throw new InitializationException(VERIFY_CREDENTIALS_UNAVAILABLE, e);
        }

        if (googleCredentials == null) {
            throw new InitializationException(VERIFY_CREDENTIALS_UNAVAILABLE);
        }

        final GoogleCredentials credentials;
        try {
            credentials = createSqlLoginScopedCredentials(googleCredentials);
        } catch (final RuntimeException e) {
            throw new InitializationException(VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE, e);
        }

        if (credentials == null) {
            throw new InitializationException(VERIFY_SCOPED_CREDENTIALS_UNAVAILABLE);
        }

        rejectIdentityPoolCredentialsOnEnable(credentials);
        return credentials;
    }

    private AccessToken refreshAccessToken(final GoogleCredentials credentials) {
        try {
            credentials.refreshIfExpired();
        } catch (final IOException | RuntimeException e) {
            throw new ProcessException(FAILED_PASSWORD_MESSAGE);
        }

        return credentials.getAccessToken();
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

    private ConfigVerificationResult describeScopedCredential(final GoogleCredentials scopedVerificationCredentials) {
        if (scopedVerificationCredentials instanceof ImpersonatedCredentials) {
            return buildVerificationResult(
                    VERIFY_SCOPE_STEP,
                    Outcome.SUCCESSFUL,
                    "Resolved GCP credentials and Cloud SQL login scope. Target service account impersonation is active."
            );
        }

        if (scopedVerificationCredentials instanceof IdentityPoolCredentials) {
            return buildVerificationResult(
                    VERIFY_SCOPE_STEP,
                    Outcome.FAILED,
                    VERIFY_IMPERSONATION_REQUIRED
            );
        }

        return buildVerificationResult(
                VERIFY_SCOPE_STEP,
                Outcome.SUCCESSFUL,
                "Resolved GCP credentials and Cloud SQL login scope."
        );
    }

    private ConfigVerificationResult buildTokenVerificationResult() {
        return buildVerificationResult(
                VERIFY_TOKEN_STEP,
                Outcome.SUCCESSFUL,
                "Acquired a Cloud SQL IAM access token. Use DBCP Verify to validate the database connection."
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
}
