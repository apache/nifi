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
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_STS_ENDPOINT;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;


/**
 * Supports AWS credentials via Assume Role.  Assume Role is a derived credential strategy, requiring a primary
 * credential to retrieve and periodically refresh temporary credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html">
 *     STSAssumeRoleCredentialsProvider</a>
 */
public class AssumeRoleCredentialsStrategy extends AbstractCredentialsStrategy {

    public AssumeRoleCredentialsStrategy() {
        super("Assume Role", new PropertyDescriptor[] {
                ASSUME_ROLE_ARN,
                ASSUME_ROLE_NAME,
                MAX_SESSION_TIME
        });
    }

    @Override
    public boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties) {
        return false;
    }

    @Override
    public boolean canCreateDerivedCredential(Map<PropertyDescriptor, String> properties) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        if (assumeRoleArn != null && !assumeRoleArn.isEmpty()
                && assumeRoleName != null && !assumeRoleName.isEmpty()) {
            return true;
        }
        return false;
    }

    public boolean proxyVariablesValidForAssumeRole(Map<PropertyDescriptor, String> properties){
        final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
        final String assumeRoleProxyPort = properties.get(ASSUME_ROLE_PROXY_PORT);
        if (assumeRoleProxyHost != null && !assumeRoleProxyHost.isEmpty()
                && assumeRoleProxyPort != null && !assumeRoleProxyPort.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        final boolean assumeRoleArnIsSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();
        final boolean assumeRoleNameIsSet = validationContext.getProperty(ASSUME_ROLE_NAME).isSet();
        final Integer maxSessionTime = validationContext.getProperty(MAX_SESSION_TIME).asInteger();
        final boolean assumeRoleExternalIdIsSet = validationContext.getProperty(ASSUME_ROLE_EXTERNAL_ID).isSet();
        final boolean assumeRoleProxyHostIsSet = validationContext.getProperty(ASSUME_ROLE_PROXY_HOST).isSet();
        final boolean assumeRoleProxyPortIsSet = validationContext.getProperty(ASSUME_ROLE_PROXY_PORT).isSet();
        final boolean assumeRoleSTSEndpointIsSet = validationContext.getProperty(ASSUME_ROLE_STS_ENDPOINT).isSet();

        final Collection<ValidationResult> validationFailureResults  = new ArrayList<ValidationResult>();

        // Both role and arn name are req if present
        if (assumeRoleArnIsSet ^ assumeRoleNameIsSet ) {
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role Arn and Name")
                    .valid(false).explanation("Assume role requires both arn and name to be set").build());
        }

        // Session time only b/w 900 to 3600 sec (see sts session class)
        if ( maxSessionTime < 900 || maxSessionTime > 3600 )
            validationFailureResults.add(new ValidationResult.Builder().valid(false).input(maxSessionTime + "")
                    .explanation(MAX_SESSION_TIME.getDisplayName() +
                            " must be between 900 and 3600 seconds").build());

        // External ID should only be provided with viable Assume Role ARN and Name
        if (assumeRoleExternalIdIsSet && (!assumeRoleArnIsSet || !assumeRoleNameIsSet)) {
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role External ID")
                    .valid(false)
                    .explanation("Assume role requires both arn and name to be set with External ID")
                    .build());
        }

        // STS Endpoint should only be provided with viable Assume Role ARN and Name
        if (assumeRoleSTSEndpointIsSet && (!assumeRoleArnIsSet || !assumeRoleNameIsSet)) {
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role STS Endpoint")
                    .valid(false)
                    .explanation("Assume role requires both arn and name to be set with STS Endpoint")
                    .build());
        }

        // Both proxy host and proxy port are required if present
        if (assumeRoleProxyHostIsSet ^ assumeRoleProxyPortIsSet){
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role Proxy Host and Port")
                    .valid(false)
                    .explanation("Assume role with proxy requires both host and port for the proxy to be set")
                    .build());
        }

        return validationFailureResults;
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider(Map<PropertyDescriptor, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AWSCredentialsProvider getDerivedCredentialsProvider(Map<PropertyDescriptor, String> properties,
                                                                AWSCredentialsProvider primaryCredentialsProvider) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        String rawMaxSessionTime = properties.get(MAX_SESSION_TIME);
        rawMaxSessionTime = (rawMaxSessionTime != null) ? rawMaxSessionTime : MAX_SESSION_TIME.getDefaultValue();
        final Integer maxSessionTime = Integer.parseInt(rawMaxSessionTime.trim());
        final String assumeRoleExternalId = properties.get(ASSUME_ROLE_EXTERNAL_ID);
        final String assumeRoleSTSEndpoint = properties.get(ASSUME_ROLE_STS_ENDPOINT);
        STSAssumeRoleSessionCredentialsProvider.Builder builder;
        ClientConfiguration config = new ClientConfiguration();

        // If proxy variables are set, then create Client Configuration with those values
        if (proxyVariablesValidForAssumeRole(properties)) {
            final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
            final Integer assumeRoleProxyPort = Integer.parseInt(properties.get(ASSUME_ROLE_PROXY_PORT));
            config.withProxyHost(assumeRoleProxyHost);
            config.withProxyPort(assumeRoleProxyPort);
        }

        AWSSecurityTokenService securityTokenService = new AWSSecurityTokenServiceClient(primaryCredentialsProvider, config);
        if (assumeRoleSTSEndpoint != null && !assumeRoleSTSEndpoint.isEmpty()) {
            securityTokenService.setEndpoint(assumeRoleSTSEndpoint);
        }
        builder = new STSAssumeRoleSessionCredentialsProvider
                .Builder(assumeRoleArn, assumeRoleName)
                .withStsClient(securityTokenService)
                .withRoleSessionDurationSeconds(maxSessionTime);

        if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
            builder = builder.withExternalId(assumeRoleExternalId);
        }

        final AWSCredentialsProvider credsProvider = builder.build();

        return credsProvider;
    }
}
