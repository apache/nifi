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
package org.apache.nifi.services.iceberg.aws;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.iceberg.IcebergFileIOProvider;
import org.apache.nifi.services.iceberg.ProviderContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"s3", "iceberg", "aws"})
@CapabilityDescription("Provides S3 file input and output support for Apache Iceberg tables")
public class S3IcebergFileIOProvider extends AbstractControllerService implements IcebergFileIOProvider {

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating with S3 storage services")
            .required(true)
            .allowableValues(AuthenticationStrategy.class)
            .defaultValue(AuthenticationStrategy.VENDED_CREDENTIALS)
            .build();

    static final PropertyDescriptor ACCESS_KEY_ID = new PropertyDescriptor.Builder()
            .name("Access Key ID")
            .description("Access Key ID for static credential authentication to S3 storage services")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.BASIC_CREDENTIALS,
                    AuthenticationStrategy.SESSION_CREDENTIALS
            )
            .build();

    static final PropertyDescriptor SECRET_ACCESS_KEY = new PropertyDescriptor.Builder()
            .name("Secret Access Key")
            .description("Secret Access Key for static credential authentication to S3 storage services")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.BASIC_CREDENTIALS,
                    AuthenticationStrategy.SESSION_CREDENTIALS
            )
            .build();

    static final PropertyDescriptor SESSION_TOKEN = new PropertyDescriptor.Builder()
            .name("Session Token")
            .description("Session Token for session-based credential authentication to S3 storage services")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.SESSION_CREDENTIALS
            )
            .build();

    static final PropertyDescriptor CLIENT_REGION = new PropertyDescriptor.Builder()
            .name("Client Region")
            .description("Region identifier for Amazon Web Services client access")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.BASIC_CREDENTIALS,
                    AuthenticationStrategy.SESSION_CREDENTIALS
            )
            .build();

    static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor.Builder()
            .name("Endpoint URL")
            .description("""
                Endpoint URL to use instead of the AWS default including scheme, host, port, and path.
                The AWS libraries select an endpoint URL based on the AWS region, but this property overrides
                the selected endpoint URL, allowing use with other S3-compatible endpoints.""")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor PATH_STYLE_ACCESS = new PropertyDescriptor.Builder()
            .name("Path Style Access")
            .description("""
                Path-style access can be enforced by setting this property to true. Set it to true if the configured
                endpoint does not support virtual-hosted-style requests, only path-style requests.""")
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(String.valueOf(S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT))
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AUTHENTICATION_STRATEGY,
            ACCESS_KEY_ID,
            SECRET_ACCESS_KEY,
            SESSION_TOKEN,
            CLIENT_REGION,
            ENDPOINT_URL,
            PATH_STYLE_ACCESS
    );

    private final Map<String, String> standardProperties = new ConcurrentHashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String, String> configuredProperties = getConfiguredProperties(context);
        standardProperties.putAll(configuredProperties);
    }

    @OnDisabled
    public void onDisabled() {
        standardProperties.clear();
    }

    @Override
    public FileIO getFileIO(final ProviderContext providerContext) {
        Objects.requireNonNull(providerContext, "Provider Context required");
        final Map<String, String> contextProperties = providerContext.getProperties();
        Objects.requireNonNull(contextProperties, "Context properties required");

        final Map<String, String> mergedProperties = new HashMap<>(standardProperties);
        mergedProperties.putAll(contextProperties);
        final S3FileIO fileIO = new S3FileIO();
        fileIO.initialize(mergedProperties);
        return fileIO;
    }

    private Map<String, String> getConfiguredProperties(final ConfigurationContext context) {
        final Map<String, String> contextProperties = new HashMap<>();
        final AuthenticationStrategy authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class);
        if (AuthenticationStrategy.BASIC_CREDENTIALS == authenticationStrategy || AuthenticationStrategy.SESSION_CREDENTIALS == authenticationStrategy) {
            final String clientRegion = context.getProperty(CLIENT_REGION).evaluateAttributeExpressions().getValue();
            contextProperties.put(AwsClientProperties.CLIENT_REGION, clientRegion);

            final String accessKeyId = context.getProperty(ACCESS_KEY_ID).getValue();
            final String secretAccessKey = context.getProperty(SECRET_ACCESS_KEY).getValue();
            contextProperties.put(S3FileIOProperties.ACCESS_KEY_ID, accessKeyId);
            contextProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, secretAccessKey);

            if (AuthenticationStrategy.SESSION_CREDENTIALS == authenticationStrategy) {
                final String sessionToken = context.getProperty(SESSION_TOKEN).getValue();
                contextProperties.put(S3FileIOProperties.SESSION_TOKEN, sessionToken);
            }
        }

        if (context.getProperty(ENDPOINT_URL).isSet()) {
            final String endpoint = context.getProperty(ENDPOINT_URL).getValue();
            contextProperties.put(S3FileIOProperties.ENDPOINT, endpoint);
        }
        final String pathStyleAccess = context.getProperty(PATH_STYLE_ACCESS).getValue();
        contextProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, pathStyleAccess);

        // HttpURLConnection Client Type avoids additional dependencies
        contextProperties.put(HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_URLCONNECTION);

        // Write Checksum Verification enabled
        contextProperties.put(S3FileIOProperties.CHECKSUM_ENABLED, Boolean.TRUE.toString());

        final S3FileIOProperties ioProperties = new S3FileIOProperties(contextProperties);
        return ioProperties.properties();
    }
}
