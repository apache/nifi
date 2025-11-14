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
package org.apache.nifi.processors.gcp.credentials.factory;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Shared definitions of properties that specify various GCP credentials.
 */
public final class CredentialPropertyDescriptors {

    private CredentialPropertyDescriptors() { }

    public static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .required(true)
            .allowableValues(AuthenticationStrategy.class)
            .defaultValue(AuthenticationStrategy.APPLICATION_DEFAULT.getValue())
            .description("Specifies how NiFi authenticates to Google Cloud. Depending on the strategy, additional properties might be required.")
            .build();

    /**
     * Specifies use of Application Default Credentials
     *
     * @see <a href="https://developers.google.com/identity/protocols/application-default-credentials">
     *     Google Application Default Credentials
     *     </a>
     */
    public static final PropertyDescriptor LEGACY_USE_APPLICATION_DEFAULT_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("Use Application Default Credentials")
            .build();

    public static final PropertyDescriptor LEGACY_USE_COMPUTE_ENGINE_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("Use Compute Engine Credentials")
            .build();

    /**
     * Specifies use of Service Account Credentials
     *
     * @see <a href="https://cloud.google.com/iam/docs/service-accounts">
     *     Google Service Accounts
     *     </a>
     */
    public static final PropertyDescriptor SERVICE_ACCOUNT_JSON_FILE = new PropertyDescriptor.Builder()
            .name("Service Account JSON File")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue())
            .description("Path to a file containing a Service Account key file in JSON format.")
            .build();

    public static final PropertyDescriptor SERVICE_ACCOUNT_JSON = new PropertyDescriptor.Builder()
            .name("Service Account JSON")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(JsonValidator.INSTANCE)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue())
            .description("The raw JSON containing a Service Account keyfile.")
            .sensitive(true)
            .build();

    private static final String DEFAULT_WORKLOAD_IDENTITY_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private static final String DEFAULT_WORKLOAD_IDENTITY_TOKEN_ENDPOINT = "https://sts.googleapis.com/v1/token";
    private static final String DEFAULT_WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
    private static final String SUBJECT_TOKEN_TYPE_ID_TOKEN = "urn:ietf:params:oauth:token-type:id_token";
    private static final String SUBJECT_TOKEN_TYPE_ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token";

    public static final PropertyDescriptor WORKLOAD_IDENTITY_AUDIENCE = new PropertyDescriptor.Builder()
            .name("Audience")
            .description("The audience corresponding to the target Workload Identity Provider, typically the full resource name.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue())
            .build();

    public static final PropertyDescriptor WORKLOAD_IDENTITY_SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("OAuth2 scopes requested for the exchanged access token. Multiple scopes can be separated by space or comma.")
            .required(true)
            .defaultValue(DEFAULT_WORKLOAD_IDENTITY_SCOPE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue())
            .build();

    public static final PropertyDescriptor WORKLOAD_IDENTITY_TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("STS Token Endpoint")
            .description("Google Security Token Service endpoint used for token exchange.")
            .required(true)
            .defaultValue(DEFAULT_WORKLOAD_IDENTITY_TOKEN_ENDPOINT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue())
            .build();

    public static final PropertyDescriptor WORKLOAD_IDENTITY_SUBJECT_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("Subject Token Provider")
            .description("Controller Service that retrieves the external workload identity token to exchange.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue())
            .build();

    public static final PropertyDescriptor WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE = new PropertyDescriptor.Builder()
            .name("Subject Token Type")
            .description("The type of token returned by the Subject Token Provider.")
            .required(true)
            .defaultValue(DEFAULT_WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE)
            .allowableValues(
                    DEFAULT_WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE,
                    SUBJECT_TOKEN_TYPE_ID_TOKEN,
                    SUBJECT_TOKEN_TYPE_ACCESS_TOKEN
            )
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue())
            .build();

    public static final PropertyDescriptor DELEGATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Delegation Strategy")
            .required(true)
            .defaultValue(DelegationStrategy.SERVICE_ACCOUNT)
            .allowableValues(DelegationStrategy.class)
            .description("The Delegation Strategy determines which account is used when calls are made with the GCP Credential.")
            .dependsOn(AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue(),
                    AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue())
            .build();

    public static final PropertyDescriptor DELEGATION_USER = new PropertyDescriptor.Builder()
            .name("Delegation User")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(DELEGATION_STRATEGY, DelegationStrategy.DELEGATED_ACCOUNT)
            .dependsOn(AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue(),
                    AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue())
            .description("This user will be impersonated by the service account for api calls. " +
                    "API calls made using this credential will appear as if they are coming from delegate user with the delegate user's access. " +
                    "Any scopes supplied from processors to this credential must have domain-wide delegation setup with the service account.")
            .build();
}
