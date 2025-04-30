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
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.commons.lang3.StringUtils;
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Tags({"Azure", "Data", "Explorer", "ADX", "Kusto", "ingest", "azure"})
@CapabilityDescription("Sends batches of flowfile content or stream flowfile content to an Azure ADX cluster.")
public class StandardKustoIngestService extends AbstractControllerService implements KustoIngestService {

    public static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .displayName("Authentication Strategy")
            .description("Authentication method for access to Azure Data Explorer")
            .required(true)
            .defaultValue(KustoAuthenticationStrategy.MANAGED_IDENTITY.getValue())
            .allowableValues(KustoAuthenticationStrategy.class)
            .build();

    public static final PropertyDescriptor APPLICATION_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Application Client ID")
            .displayName("Application Client ID")
            .description("Azure Data Explorer Application Client Identifier for Authentication")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APPLICATION_KEY = new PropertyDescriptor.Builder()
            .name("Application Key")
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

    public static final PropertyDescriptor CLUSTER_URI = new PropertyDescriptor.Builder()
            .name("Cluster URI")
            .displayName("Cluster URI")
            .description("Azure Data Explorer Cluster URI")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AUTHENTICATION_STRATEGY,
            APPLICATION_CLIENT_ID,
            APPLICATION_KEY,
            APPLICATION_TENANT_ID,
            CLUSTER_URI
    );

    private static final String STREAMING_POLICY_SHOW_COMMAND = ".show database %s policy streamingingestion";

    private static final String COUNT_TABLE_COMMAND = "%s | count";

    private static final Pair<String, String> NIFI_SINK = Pair.of("processor", StandardKustoIngestService.class.getSimpleName());

    private volatile QueuedIngestClient queuedIngestClient;

    private volatile ManagedStreamingIngestClient managedStreamingIngestClient;

    private volatile Client executionClient;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws ProcessException, URISyntaxException {
        final String applicationClientId = context.getProperty(APPLICATION_CLIENT_ID).getValue();
        final String applicationKey = context.getProperty(APPLICATION_KEY).getValue();
        final String applicationTenantId = context.getProperty(APPLICATION_TENANT_ID).getValue();
        final String clusterUri = context.getProperty(CLUSTER_URI).getValue();
        final KustoAuthenticationStrategy kustoAuthenticationStrategy = KustoAuthenticationStrategy.valueOf(context.getProperty(AUTHENTICATION_STRATEGY).getValue());

        this.queuedIngestClient = createKustoQueuedIngestClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
        this.managedStreamingIngestClient = createKustoStreamingIngestClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
        this.executionClient = createKustoExecutionClient(clusterUri, applicationClientId, applicationKey, applicationTenantId, kustoAuthenticationStrategy);
    }

    @OnStopped
    public final void onStopped() {
        if (this.queuedIngestClient != null) {
            try {
                this.queuedIngestClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure Data Explorer Queued Ingest Client failed", e);
            } finally {
                this.queuedIngestClient = null;
            }
        }
        if (this.managedStreamingIngestClient != null) {
            try {
                this.managedStreamingIngestClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure Data Explorer Managed Streaming Ingest Client failed", e);
            } finally {
                this.managedStreamingIngestClient = null;
            }
        }
        if (this.executionClient != null) {
            this.executionClient = null;
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

    @Override
    public KustoIngestionResult ingestData(final KustoIngestionRequest kustoIngestionRequest) {
        final StreamSourceInfo info = new StreamSourceInfo(kustoIngestionRequest.getInputStream());

        IngestionProperties ingestionProperties = new IngestionProperties(kustoIngestionRequest.getDatabaseName(),
                kustoIngestionRequest.getTableName());

        IngestionMapping.IngestionMappingKind ingestionMappingKind = setDataFormatAndMapping(kustoIngestionRequest.getDataFormat(), ingestionProperties);
        if (StringUtils.isNotEmpty(kustoIngestionRequest.getMappingName()) && ingestionMappingKind != null) {
            ingestionProperties.setIngestionMapping(kustoIngestionRequest.getMappingName(), ingestionMappingKind);
        }

        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setFlushImmediately(false);
        ingestionProperties.setIgnoreFirstRecord(kustoIngestionRequest.isIgnoreFirstRecord());

        final IngestionResult ingestionResult;
        try {
            if (kustoIngestionRequest.isStreamingEnabled()) {
                ingestionResult = managedStreamingIngestClient.ingestFromStream(info, ingestionProperties);
            } else {
                ingestionResult = queuedIngestClient.ingestFromStream(info, ingestionProperties);
            }
            if (kustoIngestionRequest.pollOnIngestionStatus()) {
                long timeoutMillis = kustoIngestionRequest.getIngestionStatusPollingTimeout().toMillis();
                long ingestionStatusPollingInterval = kustoIngestionRequest.getIngestionStatusPollingInterval().toMillis();
                return pollOnIngestionStatus(ingestionResult, timeoutMillis, ingestionStatusPollingInterval);
            } else {
                return KustoIngestionResult.SUCCEEDED;
            }
        } catch (IngestionClientException | IngestionServiceException | URISyntaxException e) {
            throw new ProcessException("Azure Data Explorer Ingest failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //Restore interrupted status
            throw new ProcessException("Azure Data Explorer Ingest interrupted", e);
        }
    }

    private KustoIngestionResult pollOnIngestionStatus(IngestionResult ingestionResult, long timeoutMillis, long ingestionStatusPollingInterval) throws URISyntaxException, InterruptedException {
        List<IngestionStatus> statuses = initializeKustoIngestionStatusAsPending();
        final long startTime = System.currentTimeMillis();
        // Calculate the end time based on the timeout duration
        final long endTime = startTime + timeoutMillis;
        while (System.currentTimeMillis() < endTime) {
            // Get the status of the ingestion operation
            final List<IngestionStatus> currentStatuses = ingestionResult.getIngestionStatusCollection();
            final OperationStatus operationStatus = currentStatuses.getFirst().status;
            if (operationStatus == OperationStatus.Succeeded
                    || operationStatus == OperationStatus.Failed
                    || operationStatus == OperationStatus.PartiallySucceeded) {
                statuses = currentStatuses;
                break;
            }
            // Sleep for before checking again
            Thread.sleep(ingestionStatusPollingInterval);
        }
        // Check if the timeout has been exceeded
        if (System.currentTimeMillis() - startTime >= timeoutMillis) {
            throw new ProcessException(String.format("Timeout of %s exceeded while waiting for ingestion status", ingestionStatusPollingInterval));
        }
        return KustoIngestionResult.fromString(statuses.getFirst().status.toString());
    }

    @Override
    public boolean isStreamingPolicyEnabled(final String databaseName) {
        final String query = String.format(STREAMING_POLICY_SHOW_COMMAND, databaseName);
        final KustoIngestQueryResponse kustoIngestQueryResponse = executeQuery(databaseName, query);

        boolean streamingPolicyEnabled = false;
        if (!kustoIngestQueryResponse.getQueryResult().isEmpty()) {
            final List<String> row = kustoIngestQueryResponse.getQueryResult().get(0);
            if (!row.isEmpty()) {
                final String streamingPolicy = row.get(2);
                if (!streamingPolicy.isEmpty()) {
                    streamingPolicyEnabled = true;
                }
            }
        }
        return streamingPolicyEnabled;
    }

    @Override
    public boolean isTableReadable(final String databaseName, final String tableName) {
        final String query = String.format(COUNT_TABLE_COMMAND, tableName);
        final KustoIngestQueryResponse kustoIngestQueryResponse = executeQuery(databaseName, query);
        return !kustoIngestQueryResponse.isError();
    }

    protected List<IngestionStatus> initializeKustoIngestionStatusAsPending() {
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.status = OperationStatus.Pending;
        return Collections.singletonList(ingestionStatus);
    }

    protected Client createKustoExecutionClient(final String clusterUrl,
                                                final String appId,
                                                final String appKey,
                                                final String appTenant,
                                                final KustoAuthenticationStrategy kustoAuthStrategy) throws URISyntaxException {
        return ClientFactory.createClient(createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy));
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

    private KustoIngestQueryResponse executeQuery(final String databaseName, final String query) {
        Objects.requireNonNull(databaseName, "Database Name required");
        Objects.requireNonNull(query, "Query required");

        KustoIngestQueryResponse kustoIngestQueryResponse;
        try {
            final KustoOperationResult kustoOperationResult = this.executionClient.executeQuery(databaseName, query);
            final KustoResultSetTable kustoResultSetTable = kustoOperationResult.getPrimaryResults();

            final Map<Integer, List<String>> response = new HashMap<>();
            int rowCount = 0;

            // Add the received values to the new ingestion resources
            while (kustoResultSetTable.hasNext()) {
                kustoResultSetTable.next();
                final List<String> rowData = new ArrayList<>();
                for (KustoResultColumn columnName : kustoResultSetTable.getColumns()) {
                    String data = kustoResultSetTable.getString(columnName.getOrdinal());
                    rowData.add(data);
                }
                response.put(rowCount++, rowData);
            }
            kustoIngestQueryResponse = new KustoIngestQueryResponse(response);
        } catch (final DataServiceException | DataClientException e) {
            getLogger().error("Azure Data Explorer Ingest execution failed", e);
            kustoIngestQueryResponse = new KustoIngestQueryResponse(true);
        }
        return kustoIngestQueryResponse;
    }

    private IngestionMapping.IngestionMappingKind setDataFormatAndMapping(String dataFormat, IngestionProperties ingestionProperties) {
        final IngestionProperties.DataFormat dataFormatFound = getDataFormat(dataFormat);
        return switch (dataFormatFound) {
            case AVRO -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
                yield IngestionProperties.DataFormat.AVRO.getIngestionMappingKind();
            }
            case APACHEAVRO -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO);
                yield IngestionProperties.DataFormat.APACHEAVRO.getIngestionMappingKind();
            }
            case CSV -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
                yield IngestionProperties.DataFormat.CSV.getIngestionMappingKind();
            }
            case JSON -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
                yield IngestionProperties.DataFormat.JSON.getIngestionMappingKind();
            }
            case MULTIJSON -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
                yield IngestionProperties.DataFormat.MULTIJSON.getIngestionMappingKind();
            }
            case ORC -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC);
                yield IngestionProperties.DataFormat.ORC.getIngestionMappingKind();
            }
            case PARQUET -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET);
                yield IngestionProperties.DataFormat.PARQUET.getIngestionMappingKind();
            }
            case PSV -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV);
                yield IngestionProperties.DataFormat.PSV.getIngestionMappingKind();
            }
            case SCSV -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV);
                yield IngestionProperties.DataFormat.SCSV.getIngestionMappingKind();
            }
            case SOHSV -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV);
                yield IngestionProperties.DataFormat.SOHSV.getIngestionMappingKind();
            }
            case TSV -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV);
                yield IngestionProperties.DataFormat.TSV.getIngestionMappingKind();
            }
            case TSVE -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE);
                yield IngestionProperties.DataFormat.TSVE.getIngestionMappingKind();
            }
            case TXT -> {
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT);
                yield IngestionProperties.DataFormat.TXT.getIngestionMappingKind();
            }
            default -> null;
        };
    }

    private IngestionProperties.DataFormat getDataFormat(final String dataFormat) {
        final Optional<IngestionProperties.DataFormat> dataFormatFound = Arrays.stream(IngestionProperties.DataFormat.values())
                .filter(value -> value.getKustoValue().equalsIgnoreCase(dataFormat))
                .findFirst();
        return dataFormatFound.orElseThrow(() -> new IllegalArgumentException("Data Format [%s] not supported".formatted(dataFormat)));
    }
}
