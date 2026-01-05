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
package org.apache.nifi.processors.aws.rds;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.getRegion;
import static org.apache.nifi.processors.aws.region.RegionUtil.isDynamicRegion;

@Tags({"aws", "rds", "iam", "jdbc", "password"})
@CapabilityDescription("""
        Generates Amazon RDS IAM authentication tokens each time a JDBC connection is requested.
        The generated token replaces the database user password so that NiFi does not need to store long-lived credentials inside DBCP services.
        """)
public class AwsRdsIamDatabasePasswordProvider extends AbstractControllerService implements DatabasePasswordProvider {

    static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider Service")
            .description("Controller Service that provides the AWS credentials used to sign IAM authentication requests.")
            .identifiesControllerService(AwsCredentialsProviderService.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            CUSTOM_REGION
    );

    private volatile AwsCredentialsProvider awsCredentialsProvider;
    private volatile RdsUtilities rdsUtilities;
    private volatile Region awsRegion;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (isDynamicRegion(validationContext)) {
            results.add(new ValidationResult.Builder()
                    .subject(REGION.getDisplayName())
                    .valid(false)
                    .explanation("FlowFile or attribute-driven regions are not supported")
                    .build());
        }
        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final AwsCredentialsProviderService credentialsService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AwsCredentialsProviderService.class);
        awsCredentialsProvider = credentialsService.getAwsCredentialsProvider();
        awsRegion = getRegion(context);
        rdsUtilities = createRdsUtilities(awsRegion, awsCredentialsProvider);
    }

    @OnDisabled
    public void onDisabled() {
        awsCredentialsProvider = null;
        rdsUtilities = null;
        awsRegion = null;
    }

    @Override
    public char[] getPassword(final DatabasePasswordRequestContext requestContext) {
        Objects.requireNonNull(requestContext, "Database Password Request Context required");

        final ParsedEndpoint parsedEndpoint = parseEndpoint(requestContext.getJdbcUrl());
        final String hostname = resolveHostname(parsedEndpoint, requestContext.getJdbcUrl());
        final int port = resolvePort(parsedEndpoint);
        final String username = resolveUsername(requestContext.getDatabaseUser());

        final GenerateAuthenticationTokenRequest tokenRequest = GenerateAuthenticationTokenRequest.builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .build();

        final String token;
        try {
            token = rdsUtilities.generateAuthenticationToken(tokenRequest);
        } catch (final RuntimeException e) {
            throw new ProcessException("Failed to generate RDS IAM authentication token", e);
        }

        return token.toCharArray();
    }

    protected RdsUtilities createRdsUtilities(final Region region, final AwsCredentialsProvider credentialsProvider) {
        return RdsUtilities.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
    }

    private String resolveHostname(final ParsedEndpoint parsedEndpoint, final String jdbcUrl) {
        final String hostname = parsedEndpoint.hostname();
        if (StringUtils.isBlank(hostname)) {
            throw new ProcessException("Database Endpoint not configured and JDBC URL [%s] does not contain a hostname".formatted(jdbcUrl));
        }
        return hostname;
    }

    private int resolvePort(final ParsedEndpoint parsedEndpoint) {
        final Integer parsedPort = parsedEndpoint.port();
        if (parsedPort == null) {
            throw new IllegalStateException("Database Endpoint not configured and JDBC URL does not contain a port number");
        }
        return parsedPort;
    }

    private String resolveUsername(final String contextUser) {
        final String username = StringUtils.trimToNull(contextUser);
        if (StringUtils.isBlank(username)) {
            throw new ProcessException("Database Username not configured and referencing DBCP service did not provide a username");
        }
        return username;
    }

    private ParsedEndpoint parseEndpoint(final String jdbcUrl) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return ParsedEndpoint.EMPTY;
        }

        final String normalized = jdbcUrl.startsWith("jdbc:") ? jdbcUrl.substring(5) : jdbcUrl;
        try {
            final URI uri = URI.create(normalized);
            final String host = uri.getHost();
            final int port = uri.getPort();
            return new ParsedEndpoint(host, port >= 0 ? port : null);
        } catch (final IllegalArgumentException e) {
            getLogger().debug("Unable to parse JDBC URL [{}] for hostname and port", jdbcUrl, e);
            return ParsedEndpoint.EMPTY;
        }
    }

    private record ParsedEndpoint(String hostname, Integer port) {
        private static final ParsedEndpoint EMPTY = new ParsedEndpoint(null, null);
    }
}
