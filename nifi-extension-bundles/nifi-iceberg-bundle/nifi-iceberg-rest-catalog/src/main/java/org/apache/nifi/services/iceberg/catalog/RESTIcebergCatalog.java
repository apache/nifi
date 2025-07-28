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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.nifi.services.iceberg.IcebergCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.iceberg.IcebergFileIOProvider;
import org.apache.nifi.services.iceberg.ProviderContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

@SupportsSensitiveDynamicProperties
@Tags({"iceberg", "catalog", "polaris"})
@CapabilityDescription("Provides Apache Iceberg integration with REST Catalogs such as Apache Polaris")
public class RESTIcebergCatalog extends AbstractControllerService implements IcebergCatalog, VerifiableControllerService {
    static final PropertyDescriptor CATALOG_URI = new PropertyDescriptor.Builder()
            .name("Catalog URI")
            .description("Apache Iceberg Catalog REST URI")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor FILE_IO_PROVIDER = new PropertyDescriptor.Builder()
            .name("File IO Provider")
            .description("Provider for Iceberg File Input and Output operations")
            .required(true)
            .identifiesControllerService(IcebergFileIOProvider.class)
            .build();

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating with the Apache Iceberg Catalog over HTTP")
            .required(true)
            .allowableValues(AuthenticationStrategy.class)
            .defaultValue(AuthenticationStrategy.OAUTH2)
            .build();

    static final PropertyDescriptor BEARER_TOKEN = new PropertyDescriptor.Builder()
            .name("Bearer Token")
            .description("Bearer Token for authentication to Apache Iceberg Catalog")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.BEARER)
            .build();

    static final PropertyDescriptor AUTHORIZATION_SERVER_URI = new PropertyDescriptor.Builder()
            .name("Authorization Server URI")
            .description("Authorization Server URI supporting OAuth 2")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2)
            .build();

    static final PropertyDescriptor AUTHORIZATION_GRANT_TYPE = new PropertyDescriptor.Builder()
            .name("Authorization Grant Type")
            .description("OAuth 2.0 Authorization Grant Type for obtaining Access Tokens")
            .required(true)
            .allowableValues(AuthorizationGrantType.class)
            .defaultValue(AuthorizationGrantType.CLIENT_CREDENTIALS)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2)
            .build();

    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("Client ID for OAuth 2 Client Credentials")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHORIZATION_GRANT_TYPE, AuthorizationGrantType.CLIENT_CREDENTIALS)
            .build();

    static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("Client Secret")
            .description("Client Secret for OAuth 2 Client Credentials")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHORIZATION_GRANT_TYPE, AuthorizationGrantType.CLIENT_CREDENTIALS)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN_SCOPES = new PropertyDescriptor.Builder()
            .name("Access Token Scopes")
            .description("Comma-separated list of one or more OAuth 2 scopes requested for Access Tokens")
            .required(true)
            .defaultValue(OAuth2Properties.CATALOG_SCOPE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2)
            .build();

    static final PropertyDescriptor WAREHOUSE_LOCATION = new PropertyDescriptor.Builder()
            .name("Warehouse Location")
            .description("Apache Iceberg Catalog Warehouse location or identifier")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CATALOG_URI,
            FILE_IO_PROVIDER,
            AUTHENTICATION_STRATEGY,
            BEARER_TOKEN,
            AUTHORIZATION_SERVER_URI,
            AUTHORIZATION_GRANT_TYPE,
            CLIENT_ID,
            CLIENT_SECRET,
            ACCESS_TOKEN_SCOPES,
            WAREHOUSE_LOCATION
    );

    private static final String CONFIGURATION_STEP = "Catalog Configuration";

    private static final String INITIALIZED_STATUS = "Initialized";

    private static final String CLIENT_CREDENTIALS_FORMAT = "%s:%s";

    private static final String SPACE_SEPARATOR = " ";

    private RESTSessionCatalog sessionCatalog;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyName) {
        return new PropertyDescriptor.Builder()
                .name(propertyName)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        sessionCatalog = getInitializedCatalog(context);
    }

    @OnDisabled
    public void onDisabled() {
        if (sessionCatalog != null) {
            try {
                sessionCatalog.close();
            } catch (final IOException e) {
                getLogger().warn("Close Catalog failed", e);
            }
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog componentLog, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final RESTSessionCatalog initializedCatalog = getInitializedCatalog(context);
            final String name = initializedCatalog.name();
            componentLog.info("REST Catalog Initialized [{}]", name);

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(CONFIGURATION_STEP)
                    .outcome(SUCCESSFUL)
                    .explanation(INITIALIZED_STATUS)
                    .build()
            );
        } catch (final Exception e) {
            componentLog.warn("Catalog Configuration failed", e);
            final String explanation = getExplanation(e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(CONFIGURATION_STEP)
                    .outcome(FAILED)
                    .explanation(explanation)
                    .build()
            );
        }

        return results;
    }

    @Override
    public Catalog getCatalog() {
        final SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
        return sessionCatalog.asCatalog(sessionContext);
    }

    private RESTSessionCatalog getInitializedCatalog(final ConfigurationContext context) {
        final Map<String, String> properties = new HashMap<>();

        // Set default implementations for Metrics
        properties.put(CatalogProperties.METRICS_REPORTER_IMPL, LoggingMetricsReporter.class.getName());

        final Map<String, String> dynamicProperties = getDynamicProperties(context);
        properties.putAll(dynamicProperties);

        final String catalogUri = context.getProperty(CATALOG_URI).getValue();
        properties.put(CatalogProperties.URI, catalogUri);

        final PropertyValue warehouseLocationProperty = context.getProperty(WAREHOUSE_LOCATION);
        if (warehouseLocationProperty.isSet()) {
            final String warehouseLocation = warehouseLocationProperty.getValue();
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        }

        final AuthenticationStrategy authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class);
        final Function<Map<String, String>, RESTClient> restClientBuilder;

        if (AuthenticationStrategy.BEARER == authenticationStrategy) {
            final String bearerToken = context.getProperty(BEARER_TOKEN).getValue();
            properties.put(OAuth2Properties.TOKEN, bearerToken);
            properties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2);
            restClientBuilder = this::buildStandardRestClient;
        } else {
            final String authorizationServerUri = context.getProperty(AUTHORIZATION_SERVER_URI).getValue();
            properties.put(OAuth2Properties.OAUTH2_SERVER_URI, authorizationServerUri);
            properties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2);

            final String accessTokenScopes = context.getProperty(ACCESS_TOKEN_SCOPES).getValue();
            properties.put(OAuth2Properties.SCOPE, accessTokenScopes);

            final AuthorizationGrantType authorizationGrantType = context.getProperty(AUTHORIZATION_GRANT_TYPE).asAllowableValue(AuthorizationGrantType.class);
            if (AuthorizationGrantType.CLIENT_CREDENTIALS == authorizationGrantType) {
                final String clientId = context.getProperty(CLIENT_ID).getValue();
                final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
                final String clientCredentials = CLIENT_CREDENTIALS_FORMAT.formatted(clientId, clientSecret);
                properties.put(OAuth2Properties.CREDENTIAL, clientCredentials);

                restClientBuilder = configuration -> new CredentialsRefreshRESTClient(getLogger(), buildStandardRestClient(configuration), clientId, clientSecret);
            } else {
                restClientBuilder = this::buildStandardRestClient;
            }
        }

        final IcebergFileIOProvider icebergFileIoProvider = context.getProperty(FILE_IO_PROVIDER).asControllerService(IcebergFileIOProvider.class);
        return getSessionCatalog(restClientBuilder, icebergFileIoProvider, properties);
    }

    private RESTSessionCatalog getSessionCatalog(
            final Function<Map<String, String>, RESTClient> restClientBuilder,
            final IcebergFileIOProvider icebergFileIoProvider,
            final Map<String, String> properties
    ) {
        final RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog(
                restClientBuilder,
                (sessionContext, sessionProperties) -> {
                    final ProviderContext providerContext = () -> sessionProperties;
                    return icebergFileIoProvider.getFileIO(providerContext);
                }
        );

        final String identifier = getIdentifier();
        restSessionCatalog.initialize(identifier, properties);
        return restSessionCatalog;
    }

    private RESTClient buildStandardRestClient(final Map<String, String> configuration) {
        final String uri = configuration.get(CatalogProperties.URI);
        // Set empty Authentication Session to avoid runtime exceptions in BaseHTTPClient class
        return HTTPClient.builder(configuration).uri(uri).withAuthSession(AuthSession.EMPTY).build();
    }

    private Map<String, String> getDynamicProperties(final ConfigurationContext context) {
        final Map<String, String> properties = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = property.getKey();
            if (descriptor.isDynamic()) {
                final String name = descriptor.getName();
                final String value = property.getValue();

                properties.put(name, value);
            }
        }

        return properties;
    }

    private String getExplanation(final Exception e) {
        final StringBuilder explanation = new StringBuilder();
        explanation.append(e.getClass().getSimpleName());

        final String message = e.getMessage();
        if (message != null && !message.isBlank()) {
            explanation.append(SPACE_SEPARATOR);
            explanation.append(message);
        }

        return explanation.toString();
    }
}
