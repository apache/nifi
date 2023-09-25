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

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Tags({"Azure", "ADX", "Kusto", "ingest", "azure"})
@CapabilityDescription("Sends batches of flowfile content or stream flowfile content to an Azure ADX cluster.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "AUTH_STRATEGY", description = "The strategy/method to authenticate against Azure Active Directory, either 'application' or 'managed_identity'."),
        @ReadsAttribute(attribute = "APP_ID", description = "Specifies Azure application id for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "APP_KEY", description = "Specifies Azure application key for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "APP_TENANT", description = "Azure application tenant for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "CLUSTER_URL", description = "Endpoint of ADX cluster. This is required only when streaming data to ADX cluster is enabled."),
})
public class StandardKustoIngestService extends AbstractControllerService implements KustoIngestService {

    public static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor
            .Builder().name("Authentication Strategy")
            .displayName("Authentication Strategy")
            .description("Authentication method for access to Azure Data Explorer")
            .required(true)
            .defaultValue(KustoAuthenticationStrategy.MANAGED_IDENTITY.getValue())
            .allowableValues(KustoAuthenticationStrategy.class)
            .build();

    public static final PropertyDescriptor APPLICATION_CLIENT_ID = new PropertyDescriptor
            .Builder().name("Application Client ID")
            .displayName("Application Client ID")
            .description("Azure Data Explorer Application Client Identifier for Authentication")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APPLICATION_KEY = new PropertyDescriptor
            .Builder().name("Application Key")
            .displayName("Application Key")
            .description("Azure Data Explorer Application Key for Authentication")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.APPLICATION_CREDENTIALS.getValue())
            .build();

    public static final PropertyDescriptor APPLICATION_TENANT_ID = new PropertyDescriptor.Builder()
            .name("Application Tenant ID")
            .displayName("Application Tenant ID")
            .description("Azure Data Explorer Application Tenant Identifier for Authentication")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.APPLICATION_CREDENTIALS.getValue())
            .build();

    public static final PropertyDescriptor CLUSTER_URI = new PropertyDescriptor
            .Builder().name("Cluster URI")
            .displayName("Cluster URI")
            .description("Azure Data Explorer Cluster URI")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            AUTHENTICATION_STRATEGY,
            APPLICATION_CLIENT_ID,
            APPLICATION_KEY,
            APPLICATION_TENANT_ID,
            CLUSTER_URI));

    public static final Pair<String, String> NIFI_SINK = Pair.of("processor", "nifi-sink");

    private volatile QueuedIngestClient queuedIngestClient;

    private volatile ManagedStreamingIngestClient managedStreamingIngestClient;

    private volatile Client executionClient;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws ProcessException, URISyntaxException {

        getLogger().info("Starting Azure ADX Connection Service...");
        final String applicationClientId = context.getProperty(APPLICATION_CLIENT_ID).getValue();
        final String applicationKey = context.getProperty(APPLICATION_KEY).getValue();
        final String applicationTenantId = context.getProperty(APPLICATION_TENANT_ID).getValue();
        final String clusterUri = context.getProperty(CLUSTER_URI).getValue();
        final KustoAuthenticationStrategy kustoAuthenticationStrategy = KustoAuthenticationStrategy.valueOf(context.getProperty(AUTHENTICATION_STRATEGY).getValue());

        if (this.queuedIngestClient == null) {
            this.queuedIngestClient = createKustoQueuedIngestClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
        }

        if (this.managedStreamingIngestClient == null) {
            this.managedStreamingIngestClient = createKustoStreamingIngestClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
        }

        if (this.executionClient == null) {
            this.executionClient = createKustoExecutionClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
        }

    }

    @OnStopped
    public final void onStopped() {
        if (this.queuedIngestClient != null) {
            try {
                this.queuedIngestClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure ADX Queued Ingest Client failed ", e);
            } finally {
                this.queuedIngestClient = null;
            }
        }
        if (this.managedStreamingIngestClient != null) {
            try {
                this.managedStreamingIngestClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure ADX Managed Streaming Ingest Client failed", e);
            } finally {
                this.managedStreamingIngestClient = null;
            }
        }
        if (this.executionClient != null) {
            try {
                this.executionClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure ADX Execution Client failed", e);
            } finally {
                this.executionClient = null;
            }
        }
    }


    protected QueuedIngestClient createKustoQueuedIngestClient(final String clusterUrl,
                                                               final String appId,
                                                               final String appKey,
                                                               final String appTenant,
                                                               final KustoAuthenticationStrategy kustoAuthStrategy) throws URISyntaxException {
        ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy);
        return IngestClientFactory.createClient(ingestConnectionStringBuilder);
    }

    protected ManagedStreamingIngestClient createKustoStreamingIngestClient(final String clusterUrl,
                                                                            final String appId,
                                                                            final String appKey,
                                                                            final String appTenant,
                                                                            final KustoAuthenticationStrategy kustoAuthStrategy) throws URISyntaxException {
        ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy);
        ConnectionStringBuilder streamingConnectionStringBuilder = createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy);
        return IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, streamingConnectionStringBuilder);
    }

    public KustoIngestionResult ingestData(KustoIngestionRequest kustoIngestionRequest) throws URISyntaxException {
        StreamSourceInfo info = new StreamSourceInfo(kustoIngestionRequest.getInputStream());
        //ingest data
        IngestionResult ingestionResult;
        try {
            if (kustoIngestionRequest.isStreamingEnabled()) {
                ingestionResult = managedStreamingIngestClient.ingestFromStream(info, kustoIngestionRequest.getIngestionProperties());
            } else {
                ingestionResult = queuedIngestClient.ingestFromStream(info, kustoIngestionRequest.getIngestionProperties());
            }
            if (kustoIngestionRequest.pollOnIngestionStatus()) {
                List<IngestionStatus> statuses;
                long startTime = System.currentTimeMillis();
                long timeoutMillis = 600 * 1800; // 10 Minutes timeout
                while (true) {
                    // Get the status of the ingestion operation
                    List<IngestionStatus> statuses1 = ingestionResult.getIngestionStatusCollection();
                    if (statuses1.get(0).status == OperationStatus.Succeeded
                            || statuses1.get(0).status == OperationStatus.Failed
                            || statuses1.get(0).status == OperationStatus.PartiallySucceeded) {
                        statuses = statuses1;
                        break;
                    }
                    // Check if the timeout has been exceeded
                    if (System.currentTimeMillis() - startTime >= timeoutMillis) {
                        throw new ProcessException("Timeout exceeded while waiting for ingestion status");
                    }

                    // Sleep for 5 seconds before checking again
                    Thread.sleep(5000);
                }
                return KustoIngestionResult.fromString(statuses.get(0).status.toString());
            } else {
                return KustoIngestionResult.SUCCEEDED;
            }
        } catch (IngestionClientException | IngestionServiceException e) {
            throw new ProcessException("Error occurred while ingesting data into ADX", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //Restore interrupted status
            throw new ProcessException("Error occurred while waiting for ingestion status", e);
        }
    }

    private ConnectionStringBuilder createKustoEngineConnectionString(final String clusterUrl,
                                                                      final String appId,
                                                                      final String appKey,
                                                                      final String appTenant,
                                                                      final KustoAuthenticationStrategy kustoAuthStrategy) {
        final ConnectionStringBuilder builder;
        if (KustoAuthenticationStrategy.APPLICATION_CREDENTIALS == kustoAuthStrategy) {
            builder = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUrl, appId, appKey, appTenant);
        } else {
            builder = ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl, appId);
        }


        builder.setConnectorDetails("Kusto.Nifi.Sink", StandardKustoIngestService.class.getPackage().getImplementationVersion(), null, null, false, null, NIFI_SINK);
        return builder;
    }

    protected Client createKustoExecutionClient(final String clusterUrl,
                                                final String appId,
                                                final String appKey,
                                                final String appTenant,
                                                final KustoAuthenticationStrategy kustoAuthStrategy) throws URISyntaxException {
        return ClientFactory.createClient(createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy));
    }

    @Override
    public KustoQueryResponse executeQuery(String databaseName, String query) {
        Objects.requireNonNull(databaseName, "Database Name required");
        Objects.requireNonNull(query, "Query required");
        KustoQueryResponse kustoQueryResponse;
        try {
            KustoResultSetTable kustoResultSetTable = this.executionClient.execute(databaseName, query).getPrimaryResults();
            kustoQueryResponse = new KustoQueryResponse(kustoResultSetTable);
        } catch (DataServiceException | DataClientException e) {
            getLogger().error("ADX Ingestion : Kusto Query execution failed", e);
            kustoQueryResponse = new KustoQueryResponse(true, e.getMessage());
        }
        return kustoQueryResponse;
    }


}
