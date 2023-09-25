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

import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
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
@ReadsAttributes({
        @ReadsAttribute(attribute = "DB_NAME", description = "Specifies the name of the ADX database where the data needs to be stored."),
        @ReadsAttribute(attribute = "TABLE_NAME", description = "Specifies the name of the ADX table where the data needs to be stored."),
        @ReadsAttribute(attribute = "MAPPING_NAME", description = "Specifies the name of the mapping responsible for storing the data in appropriate columns."),
        @ReadsAttribute(attribute = "FLUSH_IMMEDIATE", description = "In case of queued ingestion, this property determines whether the data should be flushed immediately to the ingest endpoint."),
        @ReadsAttribute(attribute = "DATA_FORMAT", description = "Specifies the format of data that is send to Azure Data Explorer."),
        @ReadsAttribute(attribute = "IGNORE_FIRST_RECORD", description = "Specifies whether we want to ignore ingestion of first record. " +
                "This is primarily applicable for csv files. Default is set to NO"),
        @ReadsAttribute(attribute = "POLL_ON_INGESTION_STATUS", description = "Specifies whether we want to poll on ingestion result during ingestion into ADX." +
                "In case of applications that need high throughput it is recommended to keep the default value as false. Default is set to false." +
                "This property should be set to true during Queued Ingestion for near realtime micro-batches of data that require acknowledgement of ingestion status.")
})
public class PutAzureDataExplorer extends AbstractProcessor {

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final String DATABASE = "database";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private transient KustoIngestService service;
    private boolean isStreamingEnabled;
    private boolean pollOnIngestionStatus;

    public static final AllowableValue AVRO = new AllowableValue(
            IngestionProperties.DataFormat.AVRO.getKustoValue(), IngestionProperties.DataFormat.AVRO.getKustoValue(),
            "An Avro format with support for logical types and for the snappy compression codec");

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            IngestionProperties.DataFormat.APACHEAVRO.getKustoValue(), IngestionProperties.DataFormat.APACHEAVRO.getKustoValue(),
            "An Avro format with support for logical types and for the snappy compression codec.");

    public static final AllowableValue CSV = new AllowableValue(
            IngestionProperties.DataFormat.CSV.name(), IngestionProperties.DataFormat.CSV.getKustoValue(),
            "A text file with comma-separated values (,). For more information, see RFC 4180: Common Format " +
                    "and MIME Type for Comma-Separated Values (CSV) Files.");

    public static final AllowableValue JSON = new AllowableValue(
            IngestionProperties.DataFormat.JSON.name(), IngestionProperties.DataFormat.JSON.getKustoValue(),
            "A text file containing JSON objects separated by \\n or \\r\\n. For more information, " +
                    "see JSON Lines (JSONL).");

    public static final AllowableValue MULTIJSON = new AllowableValue(
            IngestionProperties.DataFormat.MULTIJSON.name(), IngestionProperties.DataFormat.MULTIJSON.getKustoValue(),
            "A text file containing a JSON array of property containers (each representing a record) or any " +
                    "number of property containers separated by spaces, \\n or \\r\\n. Each property container may be " +
                    "spread across multiple lines. This format is preferable to JSON unless the data is not property " +
                    "containers.");

    public static final AllowableValue ORC = new AllowableValue(
            IngestionProperties.DataFormat.ORC.name(), IngestionProperties.DataFormat.ORC.getKustoValue(), "An ORC file.");

    public static final AllowableValue PARQUET = new AllowableValue(
            IngestionProperties.DataFormat.PARQUET.name(), IngestionProperties.DataFormat.PARQUET.getKustoValue(), "A parquet file.");

    public static final AllowableValue PSV = new AllowableValue(
            IngestionProperties.DataFormat.PSV.name(), IngestionProperties.DataFormat.PSV.getKustoValue(), "A text file with values separated by vertical bars (|).");

    public static final AllowableValue SCSV = new AllowableValue(
            IngestionProperties.DataFormat.SCSV.name(), IngestionProperties.DataFormat.SCSV.getKustoValue(), "A text file with values separated by semicolons (;).");

    public static final AllowableValue SOHSV = new AllowableValue(
            IngestionProperties.DataFormat.SOHSV.name(), IngestionProperties.DataFormat.SOHSV.getKustoValue(),
            "A text file with SOH-separated values. (SOH is the ASCII code point 1. " +
                    "This format is used by Hive in HDInsight).");

    public static final AllowableValue TSV = new AllowableValue(
            IngestionProperties.DataFormat.TSV.name(), IngestionProperties.DataFormat.TSV.getKustoValue(), "A text file with tab delimited values (\\t).");

    public static final AllowableValue TSVE = new AllowableValue(
            IngestionProperties.DataFormat.TSVE.name(), IngestionProperties.DataFormat.TSVE.getKustoValue(),
            "A text file with tab-delimited values (\\t). A backslash (\\) is used as escape character.");

    public static final AllowableValue TXT = new AllowableValue(
            IngestionProperties.DataFormat.TXT.name(), IngestionProperties.DataFormat.TXT.getKustoValue(),
            "A text file with lines separated by \\n. Empty lines are skipped.");

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
            .Builder().name("Mapping name")
            .displayName("Mapping name")
            .description("The name of the mapping responsible for storing the data in the appropriate columns.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IS_STREAMING_ENABLED = new PropertyDescriptor
            .Builder().name("Is Streaming enabled")
            .displayName("Is Streaming enabled")
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

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for success")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Relationship for failure")
            .build();

    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .displayName("Data Format")
            .description("The format of the data that is sent to ADX.")
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
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
        isStreamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        if (isStreamingEnabled) {
            checkIfStreamingPolicyIsEnabledInADX(context.getProperty(DATABASE_NAME).getValue(), context.getProperty(DATABASE_NAME).getValue());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DATABASE_NAME).getValue(),
                context.getProperty(TABLE_NAME).getValue());

        IngestionMapping.IngestionMappingKind ingestionMappingKind = null;

        isStreamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        pollOnIngestionStatus = context.getProperty(POLL_ON_INGESTION_STATUS).evaluateAttributeExpressions().asBoolean();

        switch (IngestionProperties.DataFormat.valueOf(context.getProperty(DATA_FORMAT).getValue())) {
            case AVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.AVRO.getIngestionMappingKind();
                break;
            case APACHEAVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.APACHEAVRO.getIngestionMappingKind();
                break;
            case CSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionMappingKind = IngestionProperties.DataFormat.CSV.getIngestionMappingKind();
                break;
            case JSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
                ingestionMappingKind = IngestionProperties.DataFormat.JSON.getIngestionMappingKind();
                break;
            case MULTIJSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
                ingestionMappingKind = IngestionProperties.DataFormat.MULTIJSON.getIngestionMappingKind();
                break;
            case ORC:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC);
                ingestionMappingKind = IngestionProperties.DataFormat.ORC.getIngestionMappingKind();
                break;
            case PARQUET:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET);
                ingestionMappingKind = IngestionProperties.DataFormat.PARQUET.getIngestionMappingKind();
                break;
            case PSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV);
                ingestionMappingKind = IngestionProperties.DataFormat.PSV.getIngestionMappingKind();
                break;
            case SCSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SCSV.getIngestionMappingKind();
                break;
            case SOHSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SOHSV.getIngestionMappingKind();
                break;
            case TSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV);
                ingestionMappingKind = IngestionProperties.DataFormat.TSV.getIngestionMappingKind();
                break;
            case TSVE:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE);
                ingestionMappingKind = IngestionProperties.DataFormat.TSVE.getIngestionMappingKind();
                break;
            case TXT:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT);
                ingestionMappingKind = IngestionProperties.DataFormat.TXT.getIngestionMappingKind();
                break;
        }

        if (StringUtils.isNotEmpty(context.getProperty(MAPPING_NAME).getValue()) && ingestionMappingKind != null) {
            ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(), ingestionMappingKind);
        }

        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setFlushImmediately(false);
        ingestionProperties.setIgnoreFirstRecord(StringUtils.equalsIgnoreCase(context.getProperty(IGNORE_FIRST_RECORD).getValue(), IGNORE_FIRST_RECORD_YES.getValue()));

        boolean isError = false;

        // ingestion into ADX
        try (final InputStream inputStream = session.read(flowFile)) {
            StringBuilder ingestLogString = new StringBuilder().append("Ingesting with: ")
                    .append("dataFormat - ").append(ingestionProperties.getDataFormat()).append("|")
                    .append("ingestionMapping - ").append(ingestionProperties.getIngestionMapping().getIngestionMappingReference()).append("|")
                    .append("reportLevel - ").append(ingestionProperties.getReportLevel()).append("|")
                    .append("reportMethod - ").append(ingestionProperties.getReportMethod()).append("|")
                    .append("databaseName - ").append(ingestionProperties.getDatabaseName()).append("|")
                    .append("tableName - ").append(ingestionProperties.getTableName()).append("|")
                    .append("flushImmediately - ").append(ingestionProperties.getFlushImmediately());
            getLogger().info(ingestLogString.toString());

            KustoIngestionResult result = service.ingestData(new KustoIngestionRequest(isStreamingEnabled, pollOnIngestionStatus, inputStream, ingestionProperties));
            getLogger().info("Poll on Ingestion Status {} and Operation status: {} ", pollOnIngestionStatus, result.toString());

            if (result == KustoIngestionResult.SUCCEEDED) {
                getLogger().info("Operation status Succeeded - {}", result.toString());
            } else if (result == KustoIngestionResult.FAILED) {
                getLogger().error("Operation status Error - {}", result.toString());
                isError = true;
            } else if (result == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                getLogger().error("Operation status Partially succeeded - {}", result.toString());
                isError = true;
            }

        } catch (IOException | URISyntaxException e) {
            getLogger().error("Exception occurred while ingesting data into ADX ", e);
            isError = true;
        }

        if (isError) {
            getLogger().error("Process failed - {}");
            session.transfer(flowFile, FAILURE);
        } else {
            getLogger().info("Process succeeded - {}");
            session.transfer(flowFile, SUCCESS);
        }
    }

    protected void checkIfStreamingPolicyIsEnabledInADX(String entityName, String database) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(database, String.format(STREAMING_POLICY_SHOW_COMMAND, PutAzureDataExplorer.DATABASE, entityName));
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while checking if streaming policy is enabled for the table");
        }
        KustoResultSetTable ingestionResultSet = kustoQueryResponse.getIngestionResultSet();
        ingestionResultSet.next();
        ingestionResultSet.getString("Policy");
    }

    protected boolean checkIfIngestorRoleDoesntExist(String databaseName, String tableName) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, String.format(FETCH_TABLE_COMMAND, tableName));
        return kustoQueryResponse.isError();
    }

}
