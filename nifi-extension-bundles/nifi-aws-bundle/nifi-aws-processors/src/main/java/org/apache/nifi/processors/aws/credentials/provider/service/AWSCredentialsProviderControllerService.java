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
package org.apache.nifi.processors.aws.credentials.provider.service;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AccessKeyPairCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AnonymousCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ExplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.FileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ImplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.NamedProfileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.WebIdentityCredentialsStrategy;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of AwsCredentialsProviderService interface
 *
 * @see AwsCredentialsProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors. " +
        "Uses default credentials without configuration. " +
        "Default credentials support EC2 instance profile/role, default user profile, environment variables, etc. " +
        "Additional options include access key / secret key pairs, credentials file, named profile, assume role credentials, " +
        "and OAuth2 OIDC Web Identity-based temporary credentials using the same Assume Role properties.")
@Tags({ "aws", "credentials", "provider" })
@Restricted(
    restrictions = {
        @Restriction(
            requiredPermission = RequiredPermission.ACCESS_ENVIRONMENT_CREDENTIALS,
            explanation = "The default configuration can read environment variables and system properties for credentials"
        )
    }
)
public class AWSCredentialsProviderControllerService extends AbstractControllerService implements AwsCredentialsProviderService {

    // Obsolete property names
    private static final String OBSOLETE_PROXY_HOST = "assume-role-proxy-host";
    private static final String OBSOLETE_PROXY_PORT = "assume-role-proxy-port";
    private static final String OBSOLETE_ASSUME_ROLE_STS_SIGNER_OVERRIDE_1 = "assume-role-sts-signer-override";
    private static final String OBSOLETE_ASSUME_ROLE_STS_SIGNER_OVERRIDE_2 = "Assume Role STS Signer Override";
    private static final String OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME_1 = "custom-signer-class-name";
    private static final String OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME_2 = "Custom Signer Class Name";
    private static final String OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION_1 = "custom-signer-module-location";
    private static final String OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION_2 = "Custom Signer Module Location";

    public static final PropertyDescriptor USE_DEFAULT_CREDENTIALS = new PropertyDescriptor.Builder()
        .name("Use Default Credentials")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .sensitive(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .description("If true, uses the Default Credential chain, including EC2 instance profiles or roles, " +
            "environment variables, default user credentials, etc.")
        .build();

    public static final PropertyDescriptor PROFILE_NAME = new PropertyDescriptor.Builder()
        .name("Profile Name")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS profile name for credentials from the profile configuration file.")
        .build();

    public static final PropertyDescriptor CREDENTIALS_FILE = new PropertyDescriptor.Builder()
        .name("Credentials File")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .description("Path to a file containing AWS access key and secret key in properties file format.")
        .build();

    public static final PropertyDescriptor ACCESS_KEY_ID = new PropertyDescriptor.Builder()
        .name("Access Key ID")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
        .name("Secret Access Key")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor USE_ANONYMOUS_CREDENTIALS = new PropertyDescriptor.Builder()
        .name("Use Anonymous Credentials")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .sensitive(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .description("If true, uses Anonymous credentials")
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_ARN = new PropertyDescriptor.Builder()
        .name("Assume Role ARN")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS Role ARN for cross account access. This is used in conjunction with Assume Role Session Name and other Assume Role properties.")
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_NAME = new PropertyDescriptor.Builder()
        .name("Assume Role Session Name")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS Role Session Name for cross account access. This is used in conjunction with Assume Role ARN.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_REGION = new PropertyDescriptor.Builder()
        .name("Assume Role STS Region")
        .description("The AWS Security Token Service (STS) region")
        .dependsOn(ASSUME_ROLE_ARN)
        .allowableValues(getAvailableRegions())
        .defaultValue(createAllowableValue(Region.US_WEST_2).getValue())
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_EXTERNAL_ID = new PropertyDescriptor.Builder()
        .name("Assume Role External ID")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("External ID for cross-account access. This is used in conjunction with Assume Role ARN.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("Assume Role SSL Context Service")
        .description("SSL Context Service used when connecting to the STS Endpoint.")
        .identifiesControllerService(SSLContextProvider.class)
        .required(false)
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
        .name("Assume Role Proxy Configuration Service")
        .identifiesControllerService(ProxyConfigurationService.class)
        .required(false)
        .description("Proxy configuration for cross-account access, if needed within your environment. This will configure a proxy to request for temporary access keys into another AWS account.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_ENDPOINT = new PropertyDescriptor.Builder()
        .name("Assume Role STS Endpoint Override")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The default AWS Security Token Service (STS) endpoint (\"sts.amazonaws.com\") works for " +
            "all accounts that are not for China (Beijing) region or GovCloud. You only need to set " +
            "this property to \"sts.cn-north-1.amazonaws.com.cn\" when you are requesting session credentials " +
            "for services in China(Beijing) region or to \"sts.us-gov-west-1.amazonaws.com\" for GovCloud.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor MAX_SESSION_TIME = new PropertyDescriptor.Builder()
        .name("Assume Role Session Time")
        .description("Session time for role based session (between 900 and 3600 seconds). This is used in conjunction with Assume Role ARN.")
        .defaultValue("3600")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .sensitive(false)
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
        .name("OAuth2 Access Token Provider")
        .description("Controller Service providing OAuth2/OIDC tokens to exchange for AWS temporary credentials using STS AssumeRoleWithWebIdentity.")
        .identifiesControllerService(OAuth2AccessTokenProvider.class)
        .required(false)
        .dependsOn(ASSUME_ROLE_ARN)
        .build();


    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        USE_DEFAULT_CREDENTIALS,
        ACCESS_KEY_ID,
        SECRET_KEY,
        CREDENTIALS_FILE,
        PROFILE_NAME,
        USE_ANONYMOUS_CREDENTIALS,
        ASSUME_ROLE_ARN,
        ASSUME_ROLE_NAME,
        MAX_SESSION_TIME,
        ASSUME_ROLE_EXTERNAL_ID,
        ASSUME_ROLE_SSL_CONTEXT_SERVICE,
        ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE,
        ASSUME_ROLE_STS_REGION,
        ASSUME_ROLE_STS_ENDPOINT,
        OAUTH2_ACCESS_TOKEN_PROVIDER
    );

    private volatile AwsCredentialsProvider credentialsProvider;

    private final List<CredentialsStrategy> strategies = List.of(
        // Primary Credential Strategies
        new WebIdentityCredentialsStrategy(),
        new ExplicitDefaultCredentialsStrategy(),
        new AccessKeyPairCredentialsStrategy(),
        new FileCredentialsStrategy(),
        new NamedProfileCredentialsStrategy(),
        new AnonymousCredentialsStrategy(),

        // Implicit Default is the catch-all primary strategy
        new ImplicitDefaultCredentialsStrategy(),

        // Derived Credential Strategies
        new AssumeRoleCredentialsStrategy());


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        ProxyServiceMigration.migrateProxyProperties(config, ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE, OBSOLETE_PROXY_HOST, OBSOLETE_PROXY_PORT, null, null);

        config.renameProperty("default-credentials", USE_DEFAULT_CREDENTIALS.getName());
        config.renameProperty("profile-name", PROFILE_NAME.getName());
        config.renameProperty("Access Key", ACCESS_KEY_ID.getName());
        config.renameProperty("Secret Key", SECRET_KEY.getName());
        config.renameProperty("anonymous-credentials", USE_ANONYMOUS_CREDENTIALS.getName());
        config.renameProperty("assume-role-sts-region", ASSUME_ROLE_STS_REGION.getName());
        config.renameProperty("assume-role-external-id", ASSUME_ROLE_EXTERNAL_ID.getName());
        config.renameProperty("assume-role-ssl-context-service", ASSUME_ROLE_SSL_CONTEXT_SERVICE.getName());
        config.renameProperty("assume-role-proxy-configuration-service", ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE.getName());
        config.renameProperty("assume-role-sts-endpoint", ASSUME_ROLE_STS_ENDPOINT.getName());
        config.renameProperty("Session Time", MAX_SESSION_TIME.getName());

        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_SIGNER_OVERRIDE_1);
        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_SIGNER_OVERRIDE_2);
        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME_1);
        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME_2);
        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION_1);
        config.removeProperty(OBSOLETE_ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION_2);
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        return credentialsProvider;
    }

    private AwsCredentialsProvider createCredentialsProvider(final PropertyContext context) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(context);
        final AwsCredentialsProvider primaryCredentialsProvider = primaryStrategy.getAwsCredentialsProvider(context);
        AwsCredentialsProvider derivedCredentialsProvider = null;

        for (final CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(context)) {
                derivedCredentialsProvider = strategy.getDerivedAwsCredentialsProvider(context, primaryCredentialsProvider);
                break;
            }
        }

        return derivedCredentialsProvider == null ? primaryCredentialsProvider : derivedCredentialsProvider;
    }

    private CredentialsStrategy selectPrimaryStrategy(final PropertyContext propertyContext) {
        for (final CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(propertyContext)) {
                return strategy;
            }
        }
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final CredentialsStrategy selectedStrategy = selectPrimaryStrategy(validationContext);
        final List<ValidationResult> validationFailureResults = new ArrayList<>();

        for (CredentialsStrategy strategy : strategies) {
            final Collection<ValidationResult> strategyValidationFailures = strategy.validate(validationContext,
                selectedStrategy);
            if (strategyValidationFailures != null) {
                validationFailureResults.addAll(strategyValidationFailures);
            }
        }

        final boolean oauth2Configured = validationContext.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet();
        if (oauth2Configured) {
            final boolean roleArnSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();
            final boolean roleNameSet = validationContext.getProperty(ASSUME_ROLE_NAME).isSet();
            if (!roleArnSet || !roleNameSet) {
                validationFailureResults.add(new ValidationResult.Builder()
                        .subject(ASSUME_ROLE_ARN.getDisplayName())
                        .valid(false)
                        .explanation("Web Identity (OIDC) requires both '" + ASSUME_ROLE_ARN.getDisplayName() + "' and '" + ASSUME_ROLE_NAME.getDisplayName() + "' to be set")
                        .build());
            }
        }

        if (validationContext.getProperty(ASSUME_ROLE_ARN).isSet()) {
            final Integer maxSessionTime = validationContext.getProperty(MAX_SESSION_TIME).asInteger();
            if (maxSessionTime != null && (maxSessionTime < 900 || maxSessionTime > 3600)) {
                validationFailureResults.add(new ValidationResult.Builder()
                        .subject(MAX_SESSION_TIME.getDisplayName())
                        .valid(false)
                        .explanation(MAX_SESSION_TIME.getDisplayName() + " must be between 900 and 3600 seconds")
                        .build());
            }
        }

        return validationFailureResults;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        credentialsProvider = createCredentialsProvider(context);
        getLogger().debug("Using credentials provider: {}", credentialsProvider.getClass());
    }

    public static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Region region : Region.regions()) {
            if (region.isGlobalRegion()) {
                continue;
            }
            values.add(createAllowableValue(region));
        }
        return values.toArray(new AllowableValue[0]);
    }

    public static AllowableValue createAllowableValue(final Region region) {
        return new AllowableValue(region.id(), region.metadata().description(), "AWS Region Code : " + region.id());
    }

    @Override
    public String toString() {
        return "AWSCredentialsProviderControllerService[id=" + getIdentifier() + "]";
    }
}
