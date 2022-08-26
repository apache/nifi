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
package org.apache.nifi.processors.adx;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.adx.AdxConnectionService;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.StorageException;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("The Azure ADX Processor sends FlowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class AzureAdxIngestProcessor extends AbstractProcessor {

    public static final AllowableValue AVRO = new AllowableValue(
            "AVRO", ".avro",
            "A legacy implementation for the Avro container file. The following codes are supported: " +
                    "null, deflate. (For snappy, use the apacheavro file format).");

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            "APACHEAVRO", ".apacheavro",
            "An Avro format with support for logical types and for the snappy compression codec.");

    public static final AllowableValue CSV = new AllowableValue(
            "CSV", ".csv",
            "A text file with comma-separated values (,). For more information, see RFC 4180: Common Format " +
                    "and MIME Type for Comma-Separated Values (CSV) Files.");

    public static final AllowableValue JSON = new AllowableValue(
            "JSON", ".json",
            "A text file containing JSON objects separated by \\n or \\r\\n. For more information, " +
                    "see JSON Lines (JSONL).");

    public static final AllowableValue MULTIJSON = new AllowableValue(
            "MULTIJSON", ".multijson",
            "A text file containing a JSON array of property containers (each representing a record) or any " +
                    "number of property containers separated by spaces, \\n or \\r\\n. Each property container may be " +
                    "spread across multiple lines. This format is preferable to JSON unless the data is not property " +
                    "containers.");

    public static final AllowableValue ORC = new AllowableValue(
            "ORC",".orc","An ORC file.");

    public static final AllowableValue PARQUET = new AllowableValue(
            "PARQUET",".parquet","A parquet file.");

    public static final AllowableValue PSV = new AllowableValue(
            "PSV",".psv","A text file with values separated by vertical bars (|).");

    public static final AllowableValue SCSV = new AllowableValue(
            "SCSV",".scsv","A text file with values separated by semicolons (;).");

    public static final AllowableValue SOHSV = new AllowableValue(
            "SOHSV",".sohsv",
            "A text file with SOH-separated values. (SOH is the ASCII code point 1. " +
                    "This format is used by Hive in HDInsight).");

    public static final AllowableValue TSV = new AllowableValue(
            "TSV",".tsv","A text file with tab delimited values (\\t).");

    public static final AllowableValue TSVE = new AllowableValue(
            "TSVE",".tsv",
            "A text file with tab-delimited values (\\t). A backslash (\\) is used as escape character.");

    public static final AllowableValue TXT = new AllowableValue(
            "TXT",".txt",
            "A text file with lines separated by \\n. Empty lines are skipped.");

    public static final AllowableValue IRL_NONE = new AllowableValue(
            "IRL_NONE", "IngestionReportLevel:None",
            "No reports are generated at all.");

    public static final AllowableValue IRL_FAIL = new AllowableValue(
            "IRL_FAIL", "IngestionReportLevel:Failure",
            "Status get's reported on failure only.");

    public static final AllowableValue IRL_FAS = new AllowableValue(
            "IRL_FAS", "IngestionReportLevel:FailureAndSuccess",
            "Status get's reported on failure and success.");

    public static final AllowableValue IRM_QUEUE = new AllowableValue(
            "IRM_QUEUE", "IngestionReportMethod:Queue",
            "Reports are generated for queue-events.");

    public static final AllowableValue IRM_TABLE = new AllowableValue(
            "IRM_TABLE", "IngestionReportMethod:Table",
            "Reports are generated for table-events.");

    public static final AllowableValue IRM_TABLEANDQUEUE = new AllowableValue(
            "IRM_TABLEANDQUEUE", "IngestionReportMethod:TableAndQueue",
            "Reports are generated for table- and queue-events.");

    public static final AllowableValue IM_KIND_UNKNOWN = new AllowableValue(
            "IM_KIND_UNKNOWN", "IngestionsMappingKind:Unknown",
            "Ingestion Mapping Kind (Type) is Unknown.");

    public static final AllowableValue IM_KIND_APACHEAVRO = new AllowableValue(
            "IM_KIND_APACHEAVRO", "IngestionsMappingKind:ApacheAvro",
            "Ingestion Mapping Kind (Type) is ApacheAvro.");

    public static final AllowableValue IM_KIND_AVRO = new AllowableValue(
            "IM_KIND_AVRO", "IngestionsMappingKind:Avro",
            "Ingestion Mapping Kind (Type) is Avro.");

    public static final AllowableValue IM_KIND_CSV = new AllowableValue(
            "IM_KIND_CSV", "IngestionsMappingKind:Csv",
            "Ingestion Mapping Kind (Type) is Csv.");

    public static final AllowableValue IM_KIND_JSON = new AllowableValue(
            "IM_KIND_JSON", "IngestionsMappingKind:Json",
            "Ingestion Mapping Kind (Type) is Json.");

    public static final AllowableValue IM_KIND_PARQUET = new AllowableValue(
            "IM_KIND_PARQUET", "IngestionsMappingKind:Parquet",
            "Ingestion Mapping Kind (Type) is Parquet.");

    public static final AllowableValue IM_KIND_ORC = new AllowableValue(
            "IM_KIND_ORC", "IngestionsMappingKind:Orc",
            "Ingestion Mapping Kind (Type) is Orc.");

    public static final AllowableValue ST_SUCCESS = new AllowableValue(
            "ST_SUCCESS", "IngestionStatus:SUCCESS",
            "Wait until ingestions is reported as succeded.");

    public static final AllowableValue ST_FIREANDFORGET = new AllowableValue(
            "ST_FIREANDFORGET", "ST_FIREANDFORGET",
            "Do not wait on ADX for status.");

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name("DB_NAME")
            .displayName("Database name")
            .description("The name of the database to store the data in.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table Name")
            .description("The name of the table in the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name("MAPPING_NAME")
            .displayName("Mapping name")
            .description("The name of the mapping responsible for storing the data in the appropriate columns.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FLUSH_IMMEDIATE = new PropertyDescriptor.Builder()
            .name("FLUSH_IMMEDIATE")
            .displayName("Flush immediate")
            .description("Flush the content sent immediately to the ingest entpoint.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("DATA_FORMAT")
            .displayName("Data format")
            .description("The format of the data that is sent to ADX.")
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IM_KIND = new PropertyDescriptor.Builder()
            .name("IM_KIND")
            .displayName("IngestionMappingKind")
            .description("The type of the ingestion mapping related to the table in ADX.")
            .required(true)
            .allowableValues(IM_KIND_AVRO, IM_KIND_APACHEAVRO, IM_KIND_CSV, IM_KIND_JSON, IM_KIND_ORC, IM_KIND_PARQUET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_LEVEL = new PropertyDescriptor.Builder()
            .name("IR_LEVEL")
            .displayName("IngestionReportLevel")
            .description("ADX can report events on several levels: None, Failure and Failure&Success.")
            .required(true)
            .allowableValues(IRL_NONE, IRL_FAIL, IRL_FAS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_METHOD = new PropertyDescriptor.Builder()
            .name("IR_METHOD")
            .displayName("IngestionReportMethod")
            .description("ADX can report events on several methods: Table, Queue and Table&Queue.")
            .required(true)
            .allowableValues(IRM_TABLE, IRM_QUEUE, IRM_TABLEANDQUEUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name("ADX_SERVICE")
            .displayName("AzureADXConnectionService")
            .description("Service that provides the ADX-Connections.")
            .required(true)
            .identifiesControllerService(AdxConnectionService.class)
            .build();

    public static final PropertyDescriptor WAIT_FOR_STATUS = new PropertyDescriptor
            .Builder().name("WAIT_FOR_STATUS")
            .displayName("Wait for status")
            .description("Define the status to be waited on.")
            .required(true)
            .allowableValues(ST_SUCCESS, ST_FIREANDFORGET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name("RL_SUCCEEDED")
            .description("Relationship for success")
            .build();

    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name("RL_FAILED")
            .description("Relationship for failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ADX_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(MAPPING_NAME);
        descriptors.add(FLUSH_IMMEDIATE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(IR_LEVEL);
        descriptors.add(IR_METHOD);
        descriptors.add(IM_KIND);
        descriptors.add(WAIT_FOR_STATUS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RL_SUCCEEDED);
        relationships.add(RL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
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

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final ComponentLog logger = getLogger();

        AdxConnectionService service = context.getProperty(ADX_SERVICE).asControllerService(AdxConnectionService.class);

        try (final InputStream in = session.read(flowFile))
        {
            IngestClient client = service.getAdxClient();

            IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                    context.getProperty(TABLE_NAME).getValue());


            switch(context.getProperty(IM_KIND).getValue())
            {
                case "IM_KIND_AVRO": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.AVRO); break;
                case "IM_KIND_APACHEAVRO": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.APACHEAVRO); break;
                case "IM_KIND_CSV": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.CSV); break;
                case "IM_KIND_JSON": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.JSON); break;
                case "IM_KIND_ORC": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.ORC); break;
                case "IM_KIND_PARQUET": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.PARQUET); break;
                /*case "IM_KIND_UNKNOWN": ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind); break;*/
            }

            switch(context.getProperty(DATA_FORMAT).getValue()) {
                case "AVRO": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO); break;
                case "APACHEAVRO": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO); break;
                case "CSV": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV); break;
                case "JSON": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON); break;
                case "MULTIJSON": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON); break;
                case "ORC": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC); break;
                case "PARQUET": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET); break;
                case "PSV": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV); break;
                case "SCSV": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV); break;
                case "SOHSV": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV); break;
                case "TSV": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV); break;
                case "TSVE": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE); break;
                case "TXT": ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT); break;
            }

            switch(context.getProperty(IR_LEVEL).getValue()) {
                case "IRL_NONE": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.NONE); break;
                case "IRL_FAIL": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_ONLY); break;
                case "IRL_FAS": ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES); break;
            }

            switch (context.getProperty(IR_METHOD).getValue()) {
                case "IRM_TABLE": ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE); break;
                case "IRM_QUEUE": ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE); break;
                case "IRM_TABLEANDQUEUE": ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE); break;
            }

            if (StringUtils.equalsIgnoreCase(context.getProperty(FLUSH_IMMEDIATE).getValue(),"true"))
            {
                ingestionProperties.setFlushImmediately(true);
            }
            else {
                ingestionProperties.setFlushImmediately(false);
            }

            logger.info("Ingesting with: "
                    + "dataFormat - "+ ingestionProperties.getDataFormat() +
                    "|" + ingestionProperties.getIngestionMapping().getIngestionMappingReference() +
                    "|" + ingestionProperties.getReportLevel() +
                    "|" + ingestionProperties.getReportMethod() +
                    "|" + ingestionProperties.getDatabaseName() +
                    "|" + ingestionProperties.getTableName() +
                    "|" + ingestionProperties.getFlushImmediately()
            );

            StreamSourceInfo info = new StreamSourceInfo(in);

            IngestionResult result = client.ingestFromStream(info, ingestionProperties);

            List<IngestionStatus> statuses = result.getIngestionStatusCollection();

            if(StringUtils.equalsIgnoreCase(context.getProperty(WAIT_FOR_STATUS).getValue(),"ST_SUCCESS"))
            {
                while (statuses.get(0).status == OperationStatus.Pending) {
                    Thread.sleep(50);
                    statuses = result.getIngestionStatusCollection();
                    if(statuses.get(0).status == OperationStatus.Succeeded) {
                        break;
                    } else if (statuses.get(0).status == OperationStatus.Failed) {
                        break;
                    }
                }
            } else {
                IngestionStatus status = new IngestionStatus();
                status.status = OperationStatus.Succeeded;
                statuses.set(0, status);
            }

            logger.info("Operation status: " + statuses.get(0).details);

            if(statuses.get(0).status == OperationStatus.Succeeded)
            {
                getLogger().info(statuses.get(0).status.toString());
                session.transfer(flowFile, RL_SUCCEEDED);
            }

            if(statuses.get(0).status == OperationStatus.Failed)
            {
                getLogger().error(statuses.get(0).status.toString());
                session.transfer(flowFile, RL_FAILED);
            }

        } catch (IOException | IngestionClientException | IngestionServiceException | StorageException |
                 URISyntaxException e) {
            logger.error("Exception occurred while ingesting data into ADX "+e);
            throw new ProcessException(e);
        } catch (InterruptedException e) {
            getLogger().error("Interrupted exception occurred while ingesting data into ADX "+e);
            throw new ProcessException(e);
        }

    }
}
