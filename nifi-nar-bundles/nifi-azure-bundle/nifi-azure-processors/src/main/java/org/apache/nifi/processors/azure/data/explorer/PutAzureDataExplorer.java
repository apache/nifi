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
package org.apache.nifi.processors.azure.data.explorer;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.data.explorer.KustoIngestDataFormat;
import org.apache.nifi.services.azure.data.explorer.KustoIngestService;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionRequest;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionResult;
import org.apache.nifi.services.azure.data.explorer.KustoQueryResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"Azure", "Kusto", "ADX", "Explorer", "Data"})
@CapabilityDescription("The PutAzureDataExplorer acts as a ADX sink connector which sends flowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint. The data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster.")
public class PutAzureDataExplorer extends AbstractProcessor {

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final String DATABASE = "database";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private transient KustoIngestService service;
    private boolean streamingEnabled;
    private boolean pollOnIngestionStatus;

    public static final AllowableValue IGNORE_FIRST_RECORD_YES = new AllowableValue(
            "YES", "YES",
            "Ignore first record during ingestion");

    public static final AllowableValue IGNORE_FIRST_RECORD_NO = new AllowableValue(
            "NO", "NO",
            "Do not ignore first record during ingestion");

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .displayName("Database Name")
            .description("Azure Data Explorer Database Name for querying")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .displayName("Table Name")
            .description("Azure Data Explorer Table Name for ingesting data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name("Ingest Mapping name")
            .displayName("Ingest Mapping name")
            .description("The name of the mapping responsible for storing the data in the appropriate columns.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IS_STREAMING_ENABLED = new PropertyDescriptor
            .Builder().name("Streaming enabled")
            .displayName("Streaming enabled")
            .description("This property determines whether we want to stream data to ADX.")
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor POLL_ON_INGESTION_STATUS = new PropertyDescriptor
            .Builder().name("Poll on ingestion status")
            .displayName("Whether to Poll on ingestion status")
            .description("This property determines whether we want to poll on ingestion status after an ingestion to ADX is completed")
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name("Kusto Ingest Service")
            .displayName("Kusto Ingest Service")
            .description("Azure Data Explorer Kusto Ingest Service")
            .required(true)
            .identifiesControllerService(KustoIngestService.class)
            .build();

    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .displayName("Data Format")
            .description("The format of the data that is sent to Azure Data Explorer.")
            .required(true)
            .allowableValues(KustoIngestDataFormat.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IGNORE_FIRST_RECORD = new PropertyDescriptor.Builder()
            .name("Ingestion Ignore First Record")
            .displayName("Ingestion Ignore First Record")
            .description("Defines whether ignore first record while ingestion.")
            .required(false)
            .allowableValues(IGNORE_FIRST_RECORD_YES, IGNORE_FIRST_RECORD_NO)
            .defaultValue(IGNORE_FIRST_RECORD_NO.getValue())
            .build();

    static final PropertyDescriptor ROUTE_PARTIALLY_SUCCESSFUL_INGESTION = new PropertyDescriptor.Builder()
            .name("Route partially successful ingestion records")
            .displayName("Route partially successful ingestion records")
            .description("Defines where to route partially successful ingestion records.")
            .required(false)
            .allowableValues("Success", "Failure")
            .defaultValue("Failure")
            .build();

    static final PropertyDescriptor INGESTION_STATUS_POLLING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout for polling on ingestion status")
            .displayName("Timeout for polling on ingestion status in seconds")
            .description("Defines the value of timeout for polling on ingestion status in seconds")
            .required(false)
            .dependsOn(POLL_ON_INGESTION_STATUS, "true")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("600")
            .build();

    static final PropertyDescriptor INGESTION_STATUS_POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Ingestion status polling interval")
            .displayName("Ingestion status polling interval in seconds")
            .description("Defines the value of timeout for polling on ingestion status in seconds.")
            .required(false)
            .dependsOn(POLL_ON_INGESTION_STATUS, "true")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for success")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Relationship for failure")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(ADX_SERVICE);
        descriptorList.add(DATABASE_NAME);
        descriptorList.add(TABLE_NAME);
        descriptorList.add(MAPPING_NAME);
        descriptorList.add(DATA_FORMAT);
        descriptorList.add(IGNORE_FIRST_RECORD);
        descriptorList.add(IS_STREAMING_ENABLED);
        descriptorList.add(POLL_ON_INGESTION_STATUS);
        descriptorList.add(ROUTE_PARTIALLY_SUCCESSFUL_INGESTION);
        descriptorList.add(INGESTION_STATUS_POLLING_TIMEOUT);
        descriptorList.add(INGESTION_STATUS_POLLING_INTERVAL);
        this.descriptors = Collections.unmodifiableList(descriptorList);

        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(SUCCESS);
        relationshipSet.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(ADX_SERVICE).asControllerService(KustoIngestService.class);
        if (checkIfIngestorRoleDoesntExist(context.getProperty(DATABASE_NAME).getValue(), context.getProperty(TABLE_NAME).getValue())) {
            getLogger().error("User might not have ingestor privileges, table validation will be skipped for all table mappings.");
            throw new ProcessException("User might not have ingestor privileges, table validation will be skipped for all table mappings. ");
        }
        streamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        if (streamingEnabled && !checkIfStreamingPolicyIsEnabledInADX(context.getProperty(DATABASE_NAME).getValue(), context.getProperty(DATABASE_NAME).getValue())) {
            getLogger().error("Streaming policy is not enabled in database {}", context.getProperty(DATABASE_NAME).getValue());
            throw new ProcessException("Streaming policy is not enabled in database {}" + context.getProperty(DATABASE_NAME).getValue());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String databaseName = context.getProperty(DATABASE_NAME).getValue();
        String tableName = context.getProperty(TABLE_NAME).getValue();
        String dataFormat = context.getProperty(DATA_FORMAT).getValue();
        String mappingName = context.getProperty(MAPPING_NAME).getValue();
        String routePartiallySuccessfulIngestion = context.getProperty(ROUTE_PARTIALLY_SUCCESSFUL_INGESTION).getValue();
        String ingestionStatusPollingTimeout = context.getProperty(INGESTION_STATUS_POLLING_TIMEOUT).getValue();
        String ingestionStatusPollingInterval = context.getProperty(INGESTION_STATUS_POLLING_INTERVAL).getValue();
        String ignoreFirstRecord = context.getProperty(IGNORE_FIRST_RECORD).getValue();
        streamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        pollOnIngestionStatus = context.getProperty(POLL_ON_INGESTION_STATUS).evaluateAttributeExpressions().asBoolean();

        boolean isError = false;

        // ingestion into ADX
        try (final InputStream inputStream = session.read(flowFile)) {
            StringBuilder ingestLogString = new StringBuilder().append("Ingesting with: ")
                    .append("dataFormat - ").append(dataFormat).append("|")
                    .append("ingestionMappingName - ").append(mappingName).append("|")
                    .append("databaseName - ").append(databaseName).append("|")
                    .append("tableName - ").append(tableName).append("|")
                    .append("pollOnIngestionStatus - ").append(pollOnIngestionStatus).append("|")
                    .append("ingestionStatusPollingTimeout - ").append(ingestionStatusPollingTimeout).append("|")
                    .append("ingestionStatusPollingInterval - ").append(ingestionStatusPollingInterval).append("|")
                    .append("routePartiallySuccessfulIngestion - ").append(routePartiallySuccessfulIngestion);
            getLogger().debug(ingestLogString.toString());

            KustoIngestionResult result = service.ingestData(
                    new KustoIngestionRequest(streamingEnabled, pollOnIngestionStatus, inputStream, databaseName,
                            tableName, dataFormat, mappingName, ignoreFirstRecord, ingestionStatusPollingTimeout, ingestionStatusPollingInterval)
            );
            getLogger().info("Ingest Status Polling Enabled {} and Ingest Status Polling Enabled {} ", pollOnIngestionStatus, result.toString());

            if (result == KustoIngestionResult.SUCCEEDED) {
                getLogger().info("Ingest Completed - {}", result.getStatus());
            } else if (result == KustoIngestionResult.FAILED) {
                getLogger().error("Ingest Failed - {}", result.getStatus());
                isError = true;
            } else if (result == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                getLogger().warn("Ingest Partially succeeded - {}", result.getStatus());
                flowFile = session.putAttribute(flowFile, "ingestion_status", "partial_success");
                if (StringUtils.equalsIgnoreCase(routePartiallySuccessfulIngestion, "Failure")) {
                    isError = true;
                }
            }

        } catch (IOException | URISyntaxException e) {
            getLogger().error("Azure Data Explorer Ingest processing failed", e);
            isError = true;
        }

        if (isError) {
            getLogger().error("Ingest processing failed {}", flowFile);
            session.transfer(flowFile, FAILURE);
        }else {
            getLogger().info("Ingest processing completed {}", flowFile);
            session.transfer(flowFile, SUCCESS);
        }
    }

    protected boolean checkIfStreamingPolicyIsEnabledInADX(String entityName, String database) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(database, String.format(STREAMING_POLICY_SHOW_COMMAND, PutAzureDataExplorer.DATABASE, entityName));
        if (kustoQueryResponse.isError()) {
            throw new ProcessException(String.format("Error occurred while checking if streaming policy is enabled for the table for entity %s in database %s",entityName,database));
        }
        List<List<Object>> queryResult = kustoQueryResponse.getQueryResult();;
        if ( queryResult.get(0) !=null && queryResult.get(0).get(2)!=null && StringUtils.isNotEmpty(queryResult.get(0).get(2).toString())) {
            return true;
        }
        return false;
    }

    protected boolean checkIfIngestorRoleDoesntExist(String databaseName, String tableName) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, String.format(FETCH_TABLE_COMMAND, tableName));
        return kustoQueryResponse.isError();
    }
}