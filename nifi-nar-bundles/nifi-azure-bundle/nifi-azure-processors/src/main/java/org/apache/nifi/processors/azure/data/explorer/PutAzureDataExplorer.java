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
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.data.explorer.KustoIngestDataFormat;
import org.apache.nifi.services.azure.data.explorer.KustoIngestQueryResponse;
import org.apache.nifi.services.azure.data.explorer.KustoIngestService;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionRequest;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionResult;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"Azure", "Kusto", "ADX", "Explorer", "Data"})
@CapabilityDescription("The PutAzureDataExplorer acts as a ADX sink connector which sends flowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint. The data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutAzureDataExplorer extends AbstractProcessor {

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final String DATABASE = "database";
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
            .Builder().name("Streaming Enabled")
            .displayName("Streaming Enabled")
            .description("This property determines whether we want to stream data to ADX.")
            .required(false)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue(Boolean.FALSE.toString())
            .build();
    public static final PropertyDescriptor POLL_ON_INGESTION_STATUS = new PropertyDescriptor
            .Builder().name("Poll for Ingestion Status")
            .displayName("Poll for Ingestion Status")
            .description("Determines whether to poll on ingestion status after an ingestion to Azure Data Explorer is completed")
            .required(false)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue(Boolean.FALSE.toString())
            .build();
    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name("Kusto Ingest Service")
            .displayName("Kusto Ingest Service")
            .description("Azure Data Explorer Kusto Ingest Service")
            .required(true)
            .identifiesControllerService(KustoIngestService.class)
            .build();
    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .displayName("Data Format")
            .description("The format of the data that is sent to Azure Data Explorer.")
            .required(true)
            .allowableValues(KustoIngestDataFormat.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor IGNORE_FIRST_RECORD = new PropertyDescriptor.Builder()
            .name("Ingestion Ignore First Record")
            .displayName("Ingestion Ignore First Record")
            .description("Defines whether ignore first record while ingestion.")
            .required(false)
            .allowableValues(IGNORE_FIRST_RECORD_YES, IGNORE_FIRST_RECORD_NO)
            .defaultValue(IGNORE_FIRST_RECORD_NO.getValue())
            .build();
    public static final PropertyDescriptor INGESTION_STATUS_POLLING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Ingest Status Polling Timeout")
            .displayName("Ingest Status Polling Timeout")
            .description("Defines the value of timeout for polling on ingestion status")
            .required(false)
            .dependsOn(POLL_ON_INGESTION_STATUS, Boolean.TRUE.toString())
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 m")
            .build();
    public static final PropertyDescriptor INGESTION_STATUS_POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Ingest Status Polling Interval")
            .displayName("Ingest Status Polling Interval")
            .description("Defines the value of timeout for polling on ingestion status in seconds.")
            .required(false)
            .dependsOn(POLL_ON_INGESTION_STATUS, Boolean.TRUE.toString())
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 s")
            .build();
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship For Success")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Relationship For Failure")
            .build();

    public static final PropertyDescriptor ROUTE_PARTIALLY_SUCCESSFUL_INGESTION = new PropertyDescriptor.Builder()
            .name("Route Partially Successful Ingestion Records")
            .displayName("Route Partially Successful Ingestion Records")
            .description("Defines where to route partially successful ingestion records.")
            .required(false)
            .allowableValues(SUCCESS.getName(), FAILURE.getName())
            .defaultValue(FAILURE.getName())
            .build();
    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;
    private transient KustoIngestService service;

    static  {
        descriptors = List.of(
                ADX_SERVICE,
                DATABASE_NAME,
                TABLE_NAME,
                MAPPING_NAME,
                DATA_FORMAT,
                IGNORE_FIRST_RECORD,
                IS_STREAMING_ENABLED,
                POLL_ON_INGESTION_STATUS,
                ROUTE_PARTIALLY_SUCCESSFUL_INGESTION,
                INGESTION_STATUS_POLLING_TIMEOUT,
                INGESTION_STATUS_POLLING_INTERVAL
        );

        relationships = Set.of(SUCCESS, FAILURE);
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
        if (!checkIfIngestorRoleExistInADX(context.getProperty(DATABASE_NAME).getValue(), context.getProperty(TABLE_NAME).getValue())) {
            throw new ProcessException(
                    String.format("User does not have ingestor privileges for database %s and table %s", context.getProperty(DATABASE_NAME).getValue(), context.getProperty(TABLE_NAME).getValue())
            );
        }
        Boolean streamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        if (streamingEnabled && !checkIfStreamingPolicyIsEnabledInADX(context.getProperty(DATABASE_NAME).getValue(), context.getProperty(DATABASE_NAME).getValue())) {
            throw new ProcessException(String.format("Streaming policy is not enabled in database %s",context.getProperty(DATABASE_NAME).getValue()));
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Relationship transferRelationship = FAILURE;

        String databaseName = context.getProperty(DATABASE_NAME).getValue();
        String tableName = context.getProperty(TABLE_NAME).getValue();
        String dataFormat = context.getProperty(DATA_FORMAT).evaluateAttributeExpressions(flowFile).getValue();
        String mappingName = context.getProperty(MAPPING_NAME).getValue();
        String routePartiallySuccessfulIngestion = context.getProperty(ROUTE_PARTIALLY_SUCCESSFUL_INGESTION).getValue();
        Duration ingestionStatusPollingTimeout = Duration.ofSeconds(context.getProperty(INGESTION_STATUS_POLLING_TIMEOUT).asTimePeriod(TimeUnit.SECONDS));
        Duration ingestionStatusPollingInterval = Duration.ofSeconds(context.getProperty(INGESTION_STATUS_POLLING_INTERVAL).asTimePeriod(TimeUnit.SECONDS));
        String ignoreFirstRecord = context.getProperty(IGNORE_FIRST_RECORD).getValue();
        Boolean streamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        Boolean pollOnIngestionStatus = context.getProperty(POLL_ON_INGESTION_STATUS).evaluateAttributeExpressions().asBoolean();

        // ingestion into ADX
        try (final InputStream inputStream = session.read(flowFile)) {
            getLogger().debug("Ingesting with: dataFormat - {} | ingestionMappingName - {} | databaseName - {} | tableName - {} | " +
                            "pollOnIngestionStatus - {} | ingestionStatusPollingTimeout - {} | ingestionStatusPollingInterval - {} | " +
                            "routePartiallySuccessfulIngestion - {}",
                    dataFormat, mappingName, databaseName, tableName, pollOnIngestionStatus,
                    ingestionStatusPollingTimeout, ingestionStatusPollingInterval, routePartiallySuccessfulIngestion);

            KustoIngestionResult result = service.ingestData(
                    new KustoIngestionRequest(streamingEnabled, pollOnIngestionStatus, inputStream, databaseName,
                            tableName, dataFormat, mappingName, ignoreFirstRecord, ingestionStatusPollingTimeout, ingestionStatusPollingInterval)
            );
            getLogger().debug("Ingest Status Polling Enabled {} and Ingest Status Polling Enabled {} ", pollOnIngestionStatus, result);

            if (result == KustoIngestionResult.SUCCEEDED) {
                getLogger().info("Ingest Completed - {}", result.getStatus());
                transferRelationship = SUCCESS;
            } else if (result == KustoIngestionResult.FAILED) {
                getLogger().error("Ingest Failed - {}", result.getStatus());
            } else if (result == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                getLogger().warn("Ingest Partially succeeded - {}", result.getStatus());
                flowFile = session.putAttribute(flowFile, "ingestion_status", "partial_success");
                if (StringUtils.equalsIgnoreCase(routePartiallySuccessfulIngestion, SUCCESS.getName())) {
                    transferRelationship = SUCCESS;
                }
            }
        } catch (IOException e) {
            getLogger().error("Azure Data Explorer Ingest processing failed", e);
        }

        session.transfer(flowFile, transferRelationship);
    }

    protected boolean checkIfStreamingPolicyIsEnabledInADX(String entityName, String database) {
        KustoIngestQueryResponse kustoIngestQueryResponse = service.isStreamingEnabled(database, String.format(STREAMING_POLICY_SHOW_COMMAND, PutAzureDataExplorer.DATABASE, entityName));
        if (kustoIngestQueryResponse.isError()) {
            throw new ProcessException(String.format("Error occurred while checking if streaming policy is enabled for the table for entity %s in database %s", entityName, database));
        }
        return kustoIngestQueryResponse.isStreamingPolicyEnabled();
    }

    protected boolean checkIfIngestorRoleExistInADX(String databaseName, String tableName) {
        KustoIngestQueryResponse kustoIngestQueryResponse = service.isIngestorPrivilegeEnabled(databaseName, String.format(FETCH_TABLE_COMMAND, tableName));
        return kustoIngestQueryResponse.isIngestorRoleEnabled();
    }
}