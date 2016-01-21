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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import static org.apache.nifi.processors.aws.AbstractAWSProcessor.ACCESS_KEY;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.SECRET_KEY;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.CREDENTIALS_FILE;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;

/**
 * Implementation of AWSCredentialsProviderService interface
 *
 * @see AWSCredentialsProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors.")
@Tags({ "aws", "credentials","provider" })
public class AWSCredentialsProviderControllerService extends AbstractControllerService implements AWSCredentialsProviderService {

    /**
     * AWS Role Arn used for cross account access
     *
     * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns">AWS ARN</a>
     */
    public static final PropertyDescriptor ASSUME_ROLE_ARN = new PropertyDescriptor.Builder().name("Assume Role ARN")
            .expressionLanguageSupported(false).required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false).description("The AWS Role ARN for cross account access. This is used in conjunction with role name and session timeout").build();

    /**
     * The role name while creating aws role
     */
    public static final PropertyDescriptor ASSUME_ROLE_NAME = new PropertyDescriptor.Builder().name("Assume Role Session Name")
            .expressionLanguageSupported(false).required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false).description("The aws role name for cross account access. This is used in conjunction with role arn and session time out").build();

    /**
     * Max session time for role based credentials. The range is between 900 and 3600 seconds.
     */
    public static final PropertyDescriptor MAX_SESSION_TIME = new PropertyDescriptor.Builder()
            .name("Session Time")
            .description("Session time for role based session (between 900 and 3600 seconds). This is used in conjunction with role arn and name")
            .defaultValue("3600")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ACCESS_KEY);
        props.add(SECRET_KEY);
        props.add(CREDENTIALS_FILE);
        props.add(ASSUME_ROLE_ARN);
        props.add(ASSUME_ROLE_NAME);
        props.add(MAX_SESSION_TIME);

        properties = Collections.unmodifiableList(props);
    }

    private volatile AWSCredentialsProvider credentialsProvider;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider() throws ProcessException {
        return credentialsProvider;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {

        final boolean accessKeySet = validationContext.getProperty(ACCESS_KEY).isSet();
        final boolean secretKeySet = validationContext.getProperty(SECRET_KEY).isSet();
        final boolean assumeRoleArnIsSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();
        final boolean assumeRoleNameIsSet = validationContext.getProperty(ASSUME_ROLE_NAME).isSet();
        final Integer maxSessionTime = validationContext.getProperty(MAX_SESSION_TIME).asInteger();

        final boolean credentialsFileSet = validationContext.getProperty(CREDENTIALS_FILE).isSet();

        final Collection<ValidationResult> validationFailureResults = new ArrayList<>();

        // both keys are required if present
        if ((accessKeySet && !secretKeySet) || (secretKeySet && !accessKeySet)) {
            validationFailureResults.add(new ValidationResult.Builder().input("Access Key").valid(false)
                    .explanation("If setting Secret Key or Access Key, must set both").build());
        }

        // Either keys or creds file is valid
        if ((secretKeySet || accessKeySet) && credentialsFileSet) {
            validationFailureResults.add(new ValidationResult.Builder().input("Access Key").valid(false)
                    .explanation("Cannot set both Credentials File and Secret Key/Access Key").build());
        }

        // Both role and arn name are req if present
        if (assumeRoleArnIsSet ^ assumeRoleNameIsSet ) {
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role Arn and Name")
                    .valid(false).explanation("Assume role requires both arn and name to be set").build());
        }

        // Session time only b/w 900 to 3600 sec (see sts session class)
        if ( maxSessionTime < 900 || maxSessionTime > 3600 )
            validationFailureResults.add(new ValidationResult.Builder().valid(false).input(maxSessionTime + "")
                    .subject(MAX_SESSION_TIME.getDisplayName() +
                            " can have value only between 900 and 3600 seconds").build());

        return validationFailureResults;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = context.getProperty(SECRET_KEY).evaluateAttributeExpressions().getValue();
        final String assumeRoleArn = context.getProperty(ASSUME_ROLE_ARN).getValue();
        final Integer maxSessionTime = context.getProperty(MAX_SESSION_TIME).asInteger();
        final String assumeRoleName = context.getProperty(ASSUME_ROLE_NAME).getValue();
        final String credentialsFile = context.getProperty(CREDENTIALS_FILE).getValue();

        // Create creds provider from file or keys
        if (credentialsFile != null) {
            try {
                getLogger().debug("Creating properties file credentials provider");
                credentialsProvider = new PropertiesFileCredentialsProvider(credentialsFile);
            } catch (final Exception ioe) {
                throw new ProcessException("Could not read Credentials File", ioe);
            }
        }

        if (credentialsProvider == null && accessKey != null && secretKey != null) {
            getLogger().debug("Creating static credentials provider");
            credentialsProvider = new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }

        // If no credentials explicitly provided, then create default one
        if (credentialsProvider == null) {
            getLogger().debug("Creating default credentials provider");
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }

        if (credentialsProvider != null && assumeRoleArn != null && assumeRoleName != null) {
            getLogger().debug("Creating sts assume role session credentials provider");

            credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(assumeRoleArn, assumeRoleName)
                    .withLongLivedCredentialsProvider(credentialsProvider)
                    .withRoleSessionDurationSeconds(maxSessionTime).build();
        }
    }

    @Override
    public String toString() {
        return "AWSCredentialsProviderService[id=" + getIdentifier() + "]";
    }
}
