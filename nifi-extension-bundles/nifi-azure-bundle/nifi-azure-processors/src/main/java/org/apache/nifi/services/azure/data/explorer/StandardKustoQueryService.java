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
package org.apache.nifi.services.azure.data.explorer;

import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

@Tags({"Azure", "Data", "Explorer", "ADX", "Kusto"})
@CapabilityDescription("Standard implementation of Kusto Query Service for Azure Data Explorer")
public class StandardKustoQueryService extends AbstractControllerService implements KustoQueryService {

    public static final PropertyDescriptor CLUSTER_URI = new PropertyDescriptor.Builder()
            .name("Cluster URI")
            .displayName("Cluster URI")
            .description("Azure Data Explorer Cluster URI")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .displayName("Authentication Strategy")
            .description("Authentication method for access to Azure Data Explorer")
            .required(true)
            .defaultValue(KustoAuthenticationStrategy.MANAGED_IDENTITY)
            .allowableValues(KustoAuthenticationStrategy.class)
            .build();

    public static final PropertyDescriptor APPLICATION_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Application Client ID")
            .displayName("Application Client ID")
            .description("Azure Data Explorer Application Client Identifier for Authentication")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APPLICATION_TENANT_ID = new PropertyDescriptor.Builder()
            .name("Application Tenant ID")
            .displayName("Application Tenant ID")
            .description("Azure Data Explorer Application Tenant Identifier for Authentication")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.APPLICATION_CREDENTIALS)
            .build();

    public static final PropertyDescriptor APPLICATION_KEY = new PropertyDescriptor.Builder()
            .name("Application Key")
            .displayName("Application Key")
            .description("Azure Data Explorer Application Key for Authentication")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.APPLICATION_CREDENTIALS)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLUSTER_URI,
            AUTHENTICATION_STRATEGY,
            APPLICATION_CLIENT_ID,
            APPLICATION_TENANT_ID,
            APPLICATION_KEY
    );

    private volatile StreamingClient kustoClient;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public static final Pair<String, String> NIFI_SOURCE = Pair.of("processor", "nifi-source");

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws ProcessException, URISyntaxException {
        if (this.kustoClient == null) {
            this.kustoClient = createClient(context);
        }
    }

    @OnStopped
    public final void onStopped() {
        if (this.kustoClient == null) {
            getLogger().debug("Kusto Client not configured");
        } else {
            this.kustoClient = null;
        }
    }

    @Override
    public KustoQueryResponse executeQuery(final String databaseName, final String query)  {
        Objects.requireNonNull(databaseName, "Database Name required");
        Objects.requireNonNull(query, "Query required");

        KustoQueryResponse kustoQueryResponse;

        try {
            final InputStream responseStream = this.kustoClient.executeStreamingQuery(databaseName, query);
            kustoQueryResponse = new KustoQueryResponse(responseStream);
        } catch (final Exception e) {
            getLogger().error("Kusto Query execution failed", e);
            kustoQueryResponse = new KustoQueryResponse(true, e.getMessage());
        }
        return kustoQueryResponse;
    }

    protected StreamingClient createClient(final ConfigurationContext context) throws URISyntaxException {
        final ConnectionStringBuilder connectionStringBuilder = getConnectionStringBuilder(context);
        return ClientFactory.createStreamingClient(connectionStringBuilder);
    }

    @SuppressWarnings("unchecked")
    private ConnectionStringBuilder getConnectionStringBuilder(final ConfigurationContext context) {
        final String clusterUrl = context.getProperty(CLUSTER_URI).getValue();
        final String clientId = context.getProperty(APPLICATION_CLIENT_ID).getValue();
        final KustoAuthenticationStrategy kustoAuthenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(KustoAuthenticationStrategy.class);

        final ConnectionStringBuilder builder = switch (kustoAuthenticationStrategy) {
            case APPLICATION_CREDENTIALS -> {
                final String applicationKey = context.getProperty(APPLICATION_KEY).getValue();
                final String tenantId = context.getProperty(APPLICATION_TENANT_ID).getValue();
                yield ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUrl, clientId, applicationKey, tenantId);
            }
            case MANAGED_IDENTITY -> ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl, clientId);
        };

        builder.setConnectorDetails("Kusto.Nifi.Source", StandardKustoQueryService.class.getPackage().getImplementationVersion(), null, null, false, null, NIFI_SOURCE);
        return builder;
    }
}
