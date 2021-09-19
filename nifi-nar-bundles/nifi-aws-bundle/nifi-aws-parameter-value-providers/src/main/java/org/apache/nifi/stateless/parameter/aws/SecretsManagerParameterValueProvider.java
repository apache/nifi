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
package org.apache.nifi.stateless.parameter.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ListSecretsRequest;
import com.amazonaws.services.secretsmanager.model.ListSecretsResult;
import com.amazonaws.services.secretsmanager.model.SecretListEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stateless.parameter.AbstractParameterValueProvider;
import org.apache.nifi.stateless.parameter.ParameterValueProvider;
import org.apache.nifi.stateless.parameter.ParameterValueProviderInitializationContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Reads secrets from AWS Secrets Manager to provide parameter values.  Secrets must be created similar to the following AWS cli command: <br/><br/>
 * <code>aws secretsmanager create-secret --name "[ParamContextName]/[ParamName]" --secret-string '[ParamValue]'</code> <br/><br/>
 *
 * A standard configuration for this provider would be: <br/><br/>
 *
 * <code>
 *      nifi.stateless.parameter.provider.AWSSecretsManager.name=AWS Secrets Manager Value Provider
 *      nifi.stateless.parameter.provider.AWSSecretsManager.type=org.apache.nifi.stateless.parameter.aws.SecretsManagerParameterValueProvider
 *      nifi.stateless.parameter.provider.AWSSecretsManager.properties.aws-configuration-file=./conf/bootstrap-aws.conf
 * </code>
 */
public class SecretsManagerParameterValueProvider extends AbstractParameterValueProvider implements ParameterValueProvider {
    private static final String QUALIFIED_SECRET_FORMAT = "%s/%s";
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";
    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";
    private static final String REGION_KEY_PROPS_NAME = "aws.region";

    public static final PropertyDescriptor AWS_CONFIG_FILE = new PropertyDescriptor.Builder()
            .displayName("AWS Configuration File")
            .name("aws-configuration-file")
            .required(false)
            .defaultValue("./conf/bootstrap-aws.conf")
            .description("Location of the bootstrap-aws.conf file that configures the AWS credentials.  If not provided, the default AWS credentials will be used.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private final ObjectReader objectReader = new ObjectMapper().reader();

    private final Set<String> supportedParameterNames = new HashSet<>();

    private List<PropertyDescriptor> descriptors;

    private AWSSecretsManager secretsManager;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected void init(final ParameterValueProviderInitializationContext context) {
        super.init(context);

        this.descriptors = Collections.unmodifiableList(Arrays.asList(AWS_CONFIG_FILE));

        final String awsBootstrapConfFilename = context.getProperty(AWS_CONFIG_FILE).getValue();
        try {
            this.secretsManager = this.configureClient(awsBootstrapConfFilename);
        } catch (final IOException e) {
            throw new IllegalStateException("Could not configure AWS Secrets Manager Client", e);
        }

        cacheSupportedParameterNames();
    }

    private void cacheSupportedParameterNames() {
        supportedParameterNames.clear();
        final ListSecretsResult listSecretsResult = secretsManager.listSecrets(new ListSecretsRequest());
        if (listSecretsResult != null) {
            for (final SecretListEntry entry : listSecretsResult.getSecretList()) {
                supportedParameterNames.add(entry.getName());
            }
        }
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        return supportedParameterNames.contains(getSecretName(contextName, parameterName));
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        final String secretName = getSecretName(contextName, parameterName);
        final GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        final GetSecretValueResult getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest);

        if (getSecretValueResult == null) {
            throw new IllegalArgumentException(String.format("Secret [%s] not found", secretName));
        }
        if (getSecretValueResult.getSecretString() != null) {
            return getSecretValueResult.getSecretString();
        } else {
            throw new IllegalStateException("Binary secrets are not supported");
        }
    }

    private Properties loadProperties(final String propertiesFilename) throws IOException {
        final Properties properties = new Properties();

        try (final InputStream in = new FileInputStream(Paths.get(propertiesFilename).toFile())) {
            properties.load(in);
            return properties;
        }
    }

    AWSSecretsManager configureClient(final String awsBootstrapConfFilename) throws IOException {
        if (awsBootstrapConfFilename == null) {
            return getDefaultClient();
        }
        final Properties properties = loadProperties(awsBootstrapConfFilename);
        final String accessKey = properties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKey = properties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = properties.getProperty(REGION_KEY_PROPS_NAME);

        if (isNotBlank(accessKey) && isNotBlank(secretKey) && isNotBlank(region)) {
            return AWSSecretsManagerClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new BasicAWSCredentialsProvider(accessKey, secretKey))
                    .build();
        } else {
            return getDefaultClient();
        }
    }

    private AWSSecretsManager getDefaultClient() {
        return AWSSecretsManagerClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    private static String getSecretName(final String contextName, final String parameterName) {
        return String.format(QUALIFIED_SECRET_FORMAT, contextName, parameterName);
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().equals("");
    }

    private static class BasicAWSCredentialsProvider implements AWSCredentialsProvider {

        private final String accessKeyId;
        private final String secretAccessKey;

        private BasicAWSCredentialsProvider(final String accessKeyId, final String secretAccessKey) {
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
        }

        @Override
        public AWSCredentials getCredentials() {
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return accessKeyId;
                }

                @Override
                public String getAWSSecretKey() {
                    return secretAccessKey;
                }
            };
        }

        @Override
        public void refresh() {

        }
    }
}
